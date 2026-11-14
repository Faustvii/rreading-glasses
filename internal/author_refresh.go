package internal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"slices"
	"time"

	"github.com/vektah/gqlparser/v2/gqlerror"
)

// Shallow author data lives only under this key, never under AuthorKey. Its
// presence and its completeness cannot expire independently of the data.
func shallowAuthorKey(id int64) string { return fmt.Sprintf("shallow-author-%d", id) }
func authorRetryKey(id int64) string   { return fmt.Sprintf("author-retry-%d", id) }

type catalogueRefreshKey struct{}

type authorLock struct {
	gate chan struct{}
	refs int
}

// References cover both the owner and waiters, so entries can be removed safely.
func (c *Controller) lockAuthor(ctx context.Context, id int64) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.authorLocksMu.Lock()
	if c.authorLocks == nil {
		c.authorLocks = map[int64]*authorLock{}
	}
	lock := c.authorLocks[id]
	if lock == nil {
		lock = &authorLock{gate: make(chan struct{}, 1)}
		c.authorLocks[id] = lock
	}
	lock.refs++
	c.authorLocksMu.Unlock()
	releaseRef := func() {
		c.authorLocksMu.Lock()
		defer c.authorLocksMu.Unlock()
		lock.refs--
		if lock.refs == 0 {
			delete(c.authorLocks, id)
		}
	}
	select {
	case lock.gate <- struct{}{}:
		return func() { <-lock.gate; releaseRef() }, nil
	case <-ctx.Done():
		releaseRef()
		return nil, ctx.Err()
	}
}

// getIncidentalAuthor resolves relationships without requesting a catalogue.
// The same author lock protects explicit loads and denormalization writes.
func (c *Controller) getIncidentalAuthor(ctx context.Context, id int64) ([]byte, error) {
	if unknownAuthor(id) {
		return nil, errNotFound
	}
	unlock, err := c.lockAuthor(ctx, id)
	if err != nil {
		return nil, err
	}
	defer unlock()
	for _, key := range []string{AuthorKey(id), shallowAuthorKey(id), refreshAuthorKey(id)} {
		b, ttl, ok := c.cache.GetWithTTL(ctx, key)
		if !ok {
			continue
		}
		// The matching catalogue owner already fetched this metadata. Explicit
		// expiry does not require another upstream lookup to finish its work.
		savedForOwner := key == AuthorKey(id) && ctx.Value(catalogueRefreshKey{}) == id && len(b) > 0 && !slices.Equal(b, _missing)
		if ttl <= 0 && !savedForOwner {
			continue
		}
		if slices.Equal(b, _missing) {
			return nil, errNotFound
		}
		return b, nil
	}
	b, err := c.getter.GetAuthor(ctx, id)
	if err != nil {
		return nil, err
	}
	c.cache.Set(ctx, shallowAuthorKey(id), b, fuzz(_authorTTL, 1.5))
	return b, nil
}

// deferAuthor keeps the persisted pre-refresh snapshot until a later successful
// attempt. Retry deadlines survive restarts and never count as completion.
func (c *Controller) deferAuthor(ctx context.Context, r refreshAuthor, err error) {
	if ctx.Err() != nil {
		return
	}
	// Renew the snapshot while work remains deferred so it cannot age out
	// independently of an author catalogue that denormalization keeps fresh.
	if snapshot, ok := c.cache.Get(ctx, refreshAuthorKey(r.id)); ok {
		if persistErr := c.persister.Persist(ctx, r.id, snapshot); persistErr != nil {
			Log(ctx).Warn("problem retaining deferred author snapshot", "authorID", r.id, "err", persistErr)
		}
	}
	state := c.authorRetryState(ctx, r.id)
	if state.Stopped {
		return
	}
	var up *upstreamError
	knownDaily := errors.As(err, &up) && up.Daily && up.ServerRetry
	if !knownDaily {
		state.Attempts++
	}
	if !retryableAuthorError(err) || (!knownDaily && state.Attempts > 5) {
		state.Stopped = true
		state.At = time.Time{}
		c.saveAuthorRetry(ctx, r.id, state)
		Log(ctx).Error("author refresh stopped; explicit refresh required", "authorID", r.id, "attempts", state.Attempts, "err", err)
		return
	}
	state.At = time.Now().Add(fuzz(time.Minute*time.Duration(1<<max(0, state.Attempts-1)), 1.25))
	if errors.As(err, &up) && up.RetryAt.After(state.At) {
		state.At = up.RetryAt.Add(time.Duration(rand.Float64() * float64(time.Second)))
	}
	c.saveAuthorRetry(ctx, r.id, state)
	Log(ctx).Warn("author refresh deferred", "authorID", r.id, "retryAt", state.At, "attempt", state.Attempts, "err", err)
	c.resumeAuthor(ctx, r, state.At)
}

type authorRetryState struct {
	At       time.Time `json:"at"`
	Attempts int       `json:"attempts"`
	Stopped  bool      `json:"stopped"`
}

func retryableAuthorError(err error) bool {
	// The job itself is still live; a shared upstream call can have a canceled
	// caller or its own timeout without making this refresh permanently bad.
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	if errors.Is(err, errBadRequest) || errors.Is(err, errNotFound) {
		return false
	}
	var up *upstreamError
	if errors.As(err, &up) {
		return up.Code == 429 || up.Code == 408 || up.Code >= 500
	}
	// Schema, validation, and conversion errors are not transport outages.
	var gqlErr *gqlerror.Error
	if errors.As(err, &gqlErr) {
		if gqlErr.Err != nil {
			return retryableAuthorError(gqlErr.Err)
		}
		var fieldStatus statusErr
		if errors.As(gqlStatusErr(gqlErr), &fieldStatus) {
			return fieldStatus == 429 || fieldStatus == 408 || fieldStatus >= 500
		}
		return false
	}
	var status statusErr
	if errors.As(err, &status) {
		return status == 429 || status == 408 || status >= 500
	}
	var network net.Error
	return errors.As(err, &network) || errors.Is(err, context.DeadlineExceeded)
}

func (c *Controller) saveAuthorRetry(ctx context.Context, id int64, state authorRetryState) {
	b, _ := json.Marshal(state)
	c.cache.Set(ctx, authorRetryKey(id), b, 365*24*time.Hour+max(time.Duration(0), time.Until(state.At)))
}

func (c *Controller) authorRetryState(ctx context.Context, id int64) authorRetryState {
	var state authorRetryState
	if b, ok := c.cache.Get(ctx, authorRetryKey(id)); ok {
		if json.Unmarshal(b, &state) != nil {
			_ = json.Unmarshal(b, &state.At)
		} // Earlier versions stored only a timestamp.
	}
	return state
}

func (c *Controller) resumeAuthor(ctx context.Context, r refreshAuthor, at time.Time) {
	go func() {
		if waitHardcover(ctx, max(time.Duration(0), time.Until(at))) != nil {
			return
		}
		current := c.authorRetryState(ctx, r.id)
		if current.Stopped || (!at.IsZero() && !current.At.Equal(at)) {
			return
		}
		select {
		case c.refreshC <- r:
		case <-ctx.Done():
		}
	}()
}

func (c *Controller) authorRetryAt(ctx context.Context, id int64) time.Time {
	return c.authorRetryState(ctx, id).At
}
