package internal

import (
	"context"
	"encoding/json"
	"iter"
	"testing"
	"time"

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
