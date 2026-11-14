package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"iter"
	"testing"
	"time"

	"github.com/Khan/genqlient/graphql"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestIncidentalAuthorPromotesToFullCatalogue(t *testing.T) {
	ctx := t.Context()
	store := newMemoryCache()
	get := NewMockgetter(gomock.NewController(t))
	p := &Persister{cache: store}
	c, err := NewController(store, get, p, nil)
	require.NoError(t, err)
	author := []byte(`{"foreignId":42,"name":"A","works":[]}`)
	get.EXPECT().GetAuthor(gomock.Any(), int64(42)).Return(author, nil)
	for range 2 {
		b, err := c.getIncidentalAuthor(ctx, 42)
		require.NoError(t, err)
		require.Equal(t, author, b)
	}
	_, ok := store.Get(ctx, AuthorKey(42))
	require.False(t, ok, "shallow data must never look like a full catalogue")
	_, ok = store.Get(ctx, refreshAuthorKey(42))
	require.False(t, ok, "incidental fetch must not persist a refresh")
	queued := make(chan refreshAuthor, 1)
	go func() { queued <- <-c.refreshC }()
	b, _, err := c.GetAuthor(ctx, 42)
	require.NoError(t, err)
	require.Equal(t, author, b)
	r := <-queued
	require.EqualValues(t, 42, r.id)
	_, ok = store.Get(ctx, refreshAuthorKey(42))
	require.True(t, ok)
	// Repeated explicit reads do not schedule a second refresh.
	_, _, err = c.GetAuthor(ctx, 42)
	require.NoError(t, err)
	get.EXPECT().GetAuthorBooks(gomock.Any(), int64(42)).Return(iter.Seq2[int64, error](func(func(int64, error) bool) {}))
	require.NoError(t, c.refreshAuthor(ctx, r.id, r.state))
	_, ok = store.Get(ctx, refreshAuthorKey(42))
	require.False(t, ok)
}

func TestFailedCatalogueRetainsProductionSnapshot(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	store := newMemoryCache()
	get := NewMockgetter(gomock.NewController(t))
	p := &Persister{cache: store}
	c, err := NewController(store, get, p, nil)
	require.NoError(t, err)
	before := []byte(`{"foreignId":42,"name":"old","works":[{"foreignId":7}]}`)
	store.Set(ctx, AuthorKey(42), before, time.Hour)
	require.NoError(t, p.Persist(ctx, 42, before))
	failed := &upstreamError{Code: 429, Daily: true, RetryAt: time.Now().Add(2 * time.Hour)}
	get.EXPECT().GetAuthorBooks(gomock.Any(), int64(42)).Return(iter.Seq2[int64, error](func(yield func(int64, error) bool) { yield(0, failed) }))
	err = c.refreshAuthor(ctx, 42, before)
	require.ErrorIs(t, err, statusErr(429))
	c.deferAuthor(ctx, refreshAuthor{id: 42, state: before}, err)
	b, ok := store.Get(ctx, refreshAuthorKey(42))
	require.True(t, ok)
	require.Equal(t, before, b)
	b, _, err = c.GetAuthor(ctx, 42)
	require.NoError(t, err)
	require.Equal(t, before, b)
	require.WithinDuration(t, failed.RetryAt, c.authorRetryAt(ctx, 42), time.Second)
	// A new controller reads the persisted retry deadline.
	restarted, err := NewController(store, get, p, nil)
	require.NoError(t, err)
	require.Equal(t, c.authorRetryAt(ctx, 42), restarted.authorRetryAt(ctx, 42))
}

func TestIncidentalExplicitAuthorRace(t *testing.T) {
	ctx := t.Context()
	store := newMemoryCache()
	get := NewMockgetter(gomock.NewController(t))
	p := &Persister{cache: store}
	c, err := NewController(store, get, p, nil)
	require.NoError(t, err)
	started := make(chan struct{})
	release := make(chan struct{})
	author, _ := json.Marshal(AuthorResource{ForeignID: 42, Name: "A"})
	get.EXPECT().GetAuthor(gomock.Any(), int64(42)).DoAndReturn(func(context.Context, int64) ([]byte, error) { close(started); <-release; return author, nil })
	incidental := make(chan error, 1)
	explicit := make(chan error, 1)
	queued := make(chan refreshAuthor, 1)
	go func() { _, e := c.getIncidentalAuthor(ctx, 42); incidental <- e }()
	<-started
	go func() { _, _, e := c.GetAuthor(ctx, 42); explicit <- e }()
	go func() { queued <- <-c.refreshC }()
	close(release)
	require.NoError(t, <-incidental)
	require.NoError(t, <-explicit)
	<-queued
	_, ok := store.Get(ctx, shallowAuthorKey(42))
	require.False(t, ok)
	b, ok := store.Get(ctx, AuthorKey(42))
	require.True(t, ok)
	require.Equal(t, author, b)
}

func TestStaleBookServedOnlyToInteractiveCaller(t *testing.T) {
	store, err := newPostgresCache(t.Context(), "postgres://postgres@localhost:5432/test", nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Delete(context.Background(), BookKey(8))); store.db.Close() })
	get := NewMockgetter(gomock.NewController(t))
	c, err := NewController(store, get, nil, nil)
	require.NoError(t, err)
	before := []byte(`{"foreignId":7,"books":[{"foreignId":8}]}`)
	store.Set(t.Context(), BookKey(8), before, time.Hour)
	require.NoError(t, store.Expire(t.Context(), BookKey(8)))
	throttled := &upstreamError{Code: 429, Daily: true, RetryAt: time.Now().Add(time.Hour)}
	get.EXPECT().GetBook(gomock.Any(), int64(8), gomock.Any()).Return(nil, int64(0), int64(0), throttled).Times(2)
	b, _, err := c.GetBook(t.Context(), 8)
	require.NoError(t, err)
	require.Equal(t, before, b)
	background := context.WithValue(t.Context(), backgroundRequestKey{}, true)
	_, _, err = c.GetBook(background, 8)
	require.ErrorIs(t, err, statusErr(429), "a stale fallback must not mark a background refresh successful")
}

func TestPermanentUpstreamFailureDoesNotScheduleRefreshRetry(t *testing.T) {
	store := newMemoryCache()
	c, err := NewController(store, nil, &Persister{cache: store}, nil)
	require.NoError(t, err)
	c.deferAuthor(t.Context(), refreshAuthor{id: 42}, &upstreamError{Code: 401})
	require.True(t, c.authorRetryAt(t.Context(), 42).IsZero())
}

func TestRefreshRecoversPromotionBeforeCatalogueWrite(t *testing.T) {
	store := newMemoryCache()
	get := NewMockgetter(gomock.NewController(t))
	p := &Persister{cache: store}
	c, err := NewController(store, get, p, nil)
	require.NoError(t, err)
	snapshot := []byte(`{"foreignId":42,"name":"saved","works":[]}`)
	require.NoError(t, p.Persist(t.Context(), 42, snapshot))
	get.EXPECT().GetAuthorBooks(gomock.Any(), int64(42)).Return(iter.Seq2[int64, error](func(func(int64, error) bool) {}))
	require.NoError(t, c.refreshAuthor(t.Context(), 42, nil))
	b, ok := store.Get(t.Context(), AuthorKey(42))
	require.True(t, ok)
	require.Equal(t, snapshot, b)
	_, ok = store.Get(t.Context(), refreshAuthorKey(42))
	require.False(t, ok)
}

func TestPermanentLocalErrorDoesNotScheduleRefreshRetry(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	store := newMemoryCache()
	c, err := NewController(store, nil, &Persister{cache: store}, nil)
	require.NoError(t, err)
	c.deferAuthor(ctx, refreshAuthor{id: 42}, errBadRequest)
	require.True(t, c.authorRetryAt(ctx, 42).IsZero())
}

func TestRefreshRetriesHavePersistentBudget(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	store := newMemoryCache()
	c, err := NewController(store, nil, &Persister{cache: store}, nil)
	require.NoError(t, err)
	for range 8 {
		c.deferAuthor(ctx, refreshAuthor{id: 42}, &upstreamError{Code: 503})
	}
	require.True(t, c.authorRetryAt(ctx, 42).IsZero(), "retry budget must stop repeated catalogue scans")
}

func TestFailedRefreshPublishesCollectedWorks(t *testing.T) {
	store := newMemoryCache()
	get := NewMockgetter(gomock.NewController(t))
	p := &Persister{cache: store}
	c, err := NewController(store, get, p, nil)
	require.NoError(t, err)
	initial, _ := json.Marshal(AuthorResource{ForeignID: 42, Name: "A"})
	work, _ := json.Marshal(workResource{ForeignID: 7, Books: []bookResource{{ForeignID: 8}}})
	store.Set(t.Context(), AuthorKey(42), initial, time.Hour)
	store.Set(t.Context(), WorkKey(7), work, time.Hour)
	store.Set(t.Context(), BookKey(8), work, time.Hour)
	require.NoError(t, p.Persist(t.Context(), 42, initial))
	failed := &upstreamError{Code: 429, Daily: true, RetryAt: time.Now().Add(time.Hour)}
	get.EXPECT().GetAuthorBooks(gomock.Any(), int64(42)).Return(iter.Seq2[int64, error](func(yield func(int64, error) bool) {
		if yield(8, nil) {
			yield(0, failed)
		}
	}))
	get.EXPECT().GetWork(gomock.Any(), int64(7), nil).Return(work, int64(0), nil)
	require.ErrorIs(t, c.refreshAuthor(t.Context(), 42, initial), statusErr(429))
	b, ok := store.Get(t.Context(), AuthorKey(42))
	require.True(t, ok)
	var author AuthorResource
	require.NoError(t, json.Unmarshal(b, &author))
	require.Len(t, author.Works, 1)
	_, ok = store.Get(t.Context(), refreshAuthorKey(42))
	require.True(t, ok, "partial progress must not mark completion")
}

func TestAuthorWaitHonorsCancellationAndReclaimsLocks(t *testing.T) {
	store := newMemoryCache()
	c, err := NewController(store, nil, nil, nil)
	require.NoError(t, err)
	store.Set(t.Context(), AuthorKey(42), []byte(`{"foreignId":42}`), time.Hour)
	unlock, err := c.lockAuthor(t.Context(), 42)
	require.NoError(t, err)
	defer func() {
		if unlock != nil {
			unlock()
		}
	}()
	first := make(chan error, 1)
	go func() { _, _, err := c.GetAuthor(t.Context(), 42); first <- err }()
	require.Eventually(t, func() bool {
		c.authorLocksMu.Lock()
		defer c.authorLocksMu.Unlock()
		return c.authorLocks[42].refs == 2
	}, time.Second, time.Millisecond)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	second := make(chan error, 1)
	go func() { _, _, err := c.GetAuthor(ctx, 42); second <- err }()
	cancel()
	select {
	case err := <-second:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("coalesced author caller ignored cancellation")
	}
	incidental := make(chan error, 1)
	waitCtx, cancelWait := context.WithCancel(t.Context())
	go func() { _, err := c.getIncidentalAuthor(waitCtx, 42); incidental <- err }()
	require.Eventually(t, func() bool {
		c.authorLocksMu.Lock()
		defer c.authorLocksMu.Unlock()
		return c.authorLocks[42].refs == 3
	}, time.Second, time.Millisecond)
	cancelWait()
	select {
	case err := <-incidental:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("author lock ignored cancellation")
	}
	unlock()
	unlock = nil
	require.NoError(t, <-first)
	require.Eventually(t, func() bool { c.authorLocksMu.Lock(); defer c.authorLocksMu.Unlock(); return len(c.authorLocks) == 0 }, time.Second, time.Millisecond)
}

func TestRefreshSkipsInvalidItemAndKeepsValidWorks(t *testing.T) {
	store := newMemoryCache()
	get := NewMockgetter(gomock.NewController(t))
	p := &Persister{cache: store}
	c, err := NewController(store, get, p, nil)
	require.NoError(t, err)
	initial, _ := json.Marshal(AuthorResource{ForeignID: 42, Name: "A"})
	work, _ := json.Marshal(workResource{ForeignID: 7, Books: []bookResource{{ForeignID: 8}}})
	store.Set(t.Context(), AuthorKey(42), initial, time.Hour)
	store.Set(t.Context(), WorkKey(7), work, time.Hour)
	store.Set(t.Context(), BookKey(8), work, time.Hour)
	require.NoError(t, p.Persist(t.Context(), 42, initial))
	get.EXPECT().GetAuthorBooks(gomock.Any(), int64(42)).Return(iter.Seq2[int64, error](func(yield func(int64, error) bool) {
		if yield(9, nil) {
			yield(8, nil)
		}
	}))
	get.EXPECT().GetBook(gomock.Any(), int64(9), gomock.Any()).Return(nil, int64(0), int64(0), errBadRequest)
	get.EXPECT().GetWork(gomock.Any(), int64(7), nil).Return(work, int64(0), nil)
	require.NoError(t, c.refreshAuthor(t.Context(), 42, initial))
	b, ok := store.Get(t.Context(), AuthorKey(42))
	require.True(t, ok)
	var author AuthorResource
	require.NoError(t, json.Unmarshal(b, &author))
	require.Len(t, author.Works, 1)
	_, ok = store.Get(t.Context(), refreshAuthorKey(42))
	require.False(t, ok)
}

func TestSharedCallerCancellationAndWrappedTimeoutRemainRetryable(t *testing.T) {
	for name, cause := range map[string]error{"shared caller canceled": context.Canceled, "wrapped timeout": hardcoverPartErrors(&graphql.Request{Query: `query { me{id} }`}, context.DeadlineExceeded)} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			store := newMemoryCache()
			c, err := NewController(store, nil, nil, nil)
			require.NoError(t, err)
			c.deferAuthor(ctx, refreshAuthor{id: 42}, cause)
			require.False(t, c.authorRetryState(ctx, 42).Stopped)
			require.False(t, c.authorRetryAt(ctx, 42).IsZero())
		})
	}
}

func TestSharedFetchSurvivesFirstCallerCancellation(t *testing.T) {
	for _, kind := range []string{"book", "work", "author"} {
		t.Run(kind, func(t *testing.T) {
			store := newMemoryCache()
			get := NewMockgetter(gomock.NewController(t))
			c, err := NewController(store, get, nil, nil)
			require.NoError(t, err)
			started := make(chan struct{})
			release := make(chan struct{})
			data := []byte(`{"foreignId":42}`)
			wait := func(ctx context.Context) ([]byte, error) {
				close(started)
				select {
				case <-release:
					return data, nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			var fetch func(context.Context) ([]byte, time.Duration, error)
			switch kind {
			case "book":
				get.EXPECT().GetBook(gomock.Any(), int64(42), gomock.Any()).DoAndReturn(func(ctx context.Context, _ int64, _ editionsCallback) ([]byte, int64, int64, error) {
					b, e := wait(ctx)
					return b, 0, 0, e
				})
				fetch = func(ctx context.Context) ([]byte, time.Duration, error) { return c.GetBook(ctx, 42) }
			case "work":
				get.EXPECT().GetWork(gomock.Any(), int64(42), gomock.Any()).DoAndReturn(func(ctx context.Context, _ int64, _ editionsCallback) ([]byte, int64, error) {
					b, e := wait(ctx)
					return b, 0, e
				})
				fetch = func(ctx context.Context) ([]byte, time.Duration, error) { return c.GetWork(ctx, 42) }
				go c.Run(t.Context())
			case "author":
				get.EXPECT().GetAuthor(gomock.Any(), int64(42)).DoAndReturn(func(ctx context.Context, _ int64) ([]byte, error) { return wait(ctx) })
				fetch = func(ctx context.Context) ([]byte, time.Duration, error) { return c.GetAuthor(ctx, 42) }
				go func() { <-c.refreshC }()
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			first := make(chan error, 1)
			go func() { _, _, err := fetch(ctx); first <- err }()
			<-started
			second := make(chan error, 1)
			background := context.WithValue(t.Context(), backgroundRequestKey{}, true)
			go func() {
				b, _, err := fetch(background)
				if err == nil && string(b) != string(data) {
					err = fmt.Errorf("unexpected shared data")
				}
				second <- err
			}()
			cancel()
			require.ErrorIs(t, <-first, context.Canceled)
			close(release)
			require.NoError(t, <-second)
		})
	}
}

func TestKnownDailyResetDoesNotExhaustFailureBudget(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	store := newMemoryCache()
	c, err := NewController(store, nil, nil, nil)
	require.NoError(t, err)
	daily := &upstreamError{Code: 429, Daily: true, ServerRetry: true, RetryAt: time.Now().Add(24 * time.Hour)}
	for range 8 {
		c.deferAuthor(ctx, refreshAuthor{id: 42}, daily)
	}
	state := c.authorRetryState(ctx, 42)
	require.False(t, state.Stopped)
	require.Zero(t, state.Attempts)
	require.False(t, state.At.Before(daily.RetryAt))
}

func TestConcurrentCatalogueRefreshesAreCoalesced(t *testing.T) {
	store := newMemoryCache()
	get := NewMockgetter(gomock.NewController(t))
	p := &Persister{cache: store}
	c, err := NewController(store, get, p, nil)
	require.NoError(t, err)
	author := []byte(`{"foreignId":42}`)
	require.NoError(t, p.Persist(t.Context(), 42, author))
	started := make(chan struct{})
	release := make(chan struct{})
	get.EXPECT().GetAuthorBooks(gomock.Any(), int64(42)).Return(iter.Seq2[int64, error](func(func(int64, error) bool) { close(started); <-release }))
	first := make(chan error, 1)
	go func() { first <- c.refreshAuthor(t.Context(), 42, author) }()
	<-started
	require.NoError(t, c.refreshAuthor(t.Context(), 42, author))
	_, ok := store.Get(t.Context(), refreshAuthorKey(42))
	require.True(t, ok, "a duplicate job must not mark the active refresh complete")
	close(release)
	require.NoError(t, <-first)
	_, ok = c.refreshing.Load(int64(42))
	require.False(t, ok)
}
