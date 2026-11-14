package internal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"
)

// Shallow author data lives only under this key, never under AuthorKey. Its
// presence and its completeness cannot expire independently of the data.
func shallowAuthorKey(id int64) string { return fmt.Sprintf("shallow-author-%d", id) }
func authorRetryKey(id int64) string   { return fmt.Sprintf("author-retry-%d", id) }

func (c *Controller) lockAuthor(id int64) func() {
	lock, _ := c.authorLocks.LoadOrStore(id, &sync.Mutex{})
	mu := lock.(*sync.Mutex)
	mu.Lock()
	return mu.Unlock
}

// getIncidentalAuthor resolves relationships without requesting a catalogue.
// The same author lock protects explicit loads and denormalization writes.
func (c *Controller) getIncidentalAuthor(ctx context.Context, id int64) ([]byte, error) {
	if unknownAuthor(id) {
		return nil, errNotFound
	}
	unlock := c.lockAuthor(id)
	defer unlock()
	for _, key := range []string{AuthorKey(id), shallowAuthorKey(id), refreshAuthorKey(id)} {
		if b, ttl, ok := c.cache.GetWithTTL(ctx, key); ok && ttl > 0 {
			if slices.Equal(b, _missing) {
				return nil, errNotFound
			}
			return b, nil
		}
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
	at := time.Now().Add(fuzz(time.Minute, 2))
	var up *upstreamError
	if errors.As(err, &up) && up.Code >= 400 && up.Code < 500 && up.Code != 429 && up.Code != 408 {
		Log(ctx).Error("author refresh requires upstream configuration repair", "authorID", r.id, "status", up.Code)
		return // Retain the snapshot; restart recovery can retry after repair.
	}
	if errors.As(err, &up) && up.RetryAt.After(at) {
		at = up.RetryAt
	}
	b, _ := json.Marshal(at)
	c.cache.Set(ctx, authorRetryKey(r.id), b, time.Until(at)+365*24*time.Hour)
	Log(ctx).Warn("author refresh deferred", "authorID", r.id, "retryAt", at, "err", err)
	c.resumeAuthor(ctx, r, at)
}

func (c *Controller) resumeAuthor(ctx context.Context, r refreshAuthor, at time.Time) {
	go func() {
		if waitHardcover(ctx, max(time.Duration(0), time.Until(at))) != nil {
			return
		}
		select {
		case c.refreshC <- r:
		case <-ctx.Done():
		}
	}()
}

func (c *Controller) authorRetryAt(ctx context.Context, id int64) time.Time {
	var at time.Time
	if b, ok := c.cache.Get(ctx, authorRetryKey(id)); ok {
		_ = json.Unmarshal(b, &at)
	}
	return at
}
