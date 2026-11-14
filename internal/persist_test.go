package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"iter"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"go.uber.org/mock/gomock"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPersister(t *testing.T) {
	ctx := t.Context()

	dsn := "postgres://postgres@localhost:5432/test"
	cache, err := NewCache(t.Context(), dsn, nil, nil)
	require.NoError(t, err)

	p, err := NewPersister(ctx, cache, dsn)
	require.NoError(t, err)

	authorIDs, err := p.Persisted(ctx)
	require.NoError(t, err)

	assert.Empty(t, authorIDs)

	assert.NoError(t, p.Persist(ctx, 2, _missing))
	assert.NoError(t, p.Persist(ctx, 1, _missing))
	assert.NoError(t, p.Persist(ctx, 1, _missing))
	assert.NoError(t, p.Persist(ctx, 3, _missing))

	authorIDs, err = p.Persisted(ctx)
	require.NoError(t, err)

	assert.Equal(t, []int64{2, 1, 3}, authorIDs)

	assert.NoError(t, p.Delete(ctx, 1))
	assert.NoError(t, p.Delete(ctx, 2))
	assert.NoError(t, p.Delete(ctx, 3))
	assert.NoError(t, p.Delete(ctx, 10))
}

func TestHardcoverCooldownAndRefreshSurvivePostgresRestart(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	store, err := newPostgresCache(ctx, "postgres://postgres@localhost:5432/test", nil)
	require.NoError(t, err)
	p, err := NewPersister(ctx, store, "postgres://postgres@localhost:5432/test")
	require.NoError(t, err)
	get := NewMockgetter(gomock.NewController(t))
	c, err := NewController(store, get, p, nil)
	require.NoError(t, err)
	const id = int64(987654321)
	snapshot := []byte(`{"foreignId":987654321,"name":"saved","works":[]}`)
	require.NoError(t, p.Persist(ctx, id, snapshot))
	store.Set(ctx, AuthorKey(id), snapshot, time.Hour)
	key := "postgres-cooldown-regression"
	client := newHardcoverClient(&http.Client{}, store, key, nil)
	failed := &upstreamError{Code: 429, Daily: true, RetryAt: time.Now().Add(time.Hour)}
	client.deferUntil(ctx, failed)
	c.deferAuthor(ctx, refreshAuthor{id: id, state: snapshot}, failed)
	t.Cleanup(func() {
		cleanup := context.Background()
		require.NoError(t, store.Delete(cleanup, client.cacheKey))
		require.NoError(t, store.Delete(cleanup, AuthorKey(id)))
		require.NoError(t, store.Delete(cleanup, refreshAuthorKey(id)))
		require.NoError(t, store.Delete(cleanup, authorRetryKey(id)))
		store.db.Close()
		p.db.Close()
	})
	// Reconnect without either process's memory cache.
	restartedStore, err := newPostgresCache(ctx, "postgres://postgres@localhost:5432/test", nil)
	require.NoError(t, err)
	defer restartedStore.db.Close()
	restartedClient := newHardcoverClient(&http.Client{}, restartedStore, key, nil)
	require.NotNil(t, restartedClient.cooldown)
	require.Equal(t, failed.RetryAt.Unix(), restartedClient.cooldown.RetryAt.Unix())
	restarted, err := NewController(restartedStore, get, p, nil)
	require.NoError(t, err)
	require.Equal(t, c.authorRetryAt(ctx, id).UnixNano(), restarted.authorRetryAt(ctx, id).UnixNano())
	ids, err := p.Persisted(ctx)
	require.NoError(t, err)
	require.Contains(t, ids, id)
	b, _, err := restarted.GetAuthor(ctx, id)
	require.NoError(t, err)
	require.Equal(t, snapshot, b)
	get.EXPECT().GetAuthorBooks(gomock.Any(), id).Return(iter.Seq2[int64, error](func(func(int64, error) bool) {}))
	require.NoError(t, restarted.refreshAuthor(ctx, id, snapshot))
	_, ok := restartedStore.Get(ctx, refreshAuthorKey(id))
	require.False(t, ok)
	_, ok = restartedStore.Get(ctx, authorRetryKey(id))
	require.False(t, ok)
}

func TestIncidentalUpdateDoesNotRenewExpiredCatalogue(t *testing.T) {
	ctx := t.Context()
	store, err := newPostgresCache(ctx, "postgres://postgres@localhost:5432/test", nil)
	require.NoError(t, err)
	const id = int64(987654322)
	t.Cleanup(func() {
		cleanup := context.Background()
		for _, key := range []string{AuthorKey(id), shallowAuthorKey(id), refreshAuthorKey(id), authorRetryKey(id)} {
			require.NoError(t, store.Delete(cleanup, key))
		}
		store.db.Close()
	})
	get := NewMockgetter(gomock.NewController(t))
	c, err := NewController(store, get, &Persister{cache: store}, nil)
	require.NoError(t, err)
	old, _ := json.Marshal(AuthorResource{ForeignID: id, Name: "old catalogue"})
	fresh, _ := json.Marshal(AuthorResource{ForeignID: id, Name: "fresh metadata"})
	work, _ := json.Marshal(workResource{ForeignID: 7, Books: []bookResource{{ForeignID: 8}}})
	store.Set(ctx, AuthorKey(id), old, time.Hour)
	require.NoError(t, store.Expire(ctx, AuthorKey(id)))
	get.EXPECT().GetAuthor(gomock.Any(), id).Return(fresh, nil)
	get.EXPECT().GetWork(gomock.Any(), int64(7), nil).Return(work, int64(0), nil)
	require.NoError(t, c.denormalizeWorks(ctx, id, 7))
	_, ttl, ok := store.GetWithTTL(ctx, AuthorKey(id))
	require.True(t, ok)
	require.LessOrEqual(t, ttl, time.Duration(0), "incidental activity renewed an expired catalogue")
	queued := make(chan refreshAuthor, 1)
	go func() { queued <- <-c.refreshC }()
	b, _, err := c.GetAuthor(ctx, id)
	require.NoError(t, err)
	require.Equal(t, old, b)
	select {
	case r := <-queued:
		require.Equal(t, id, r.id)
	case <-time.After(time.Second):
		t.Fatal("explicit request failed to refresh the expired catalogue")
	}
}

func TestExplicitRefreshResetsStoppedRetryBudget(t *testing.T) {
	ctx := t.Context()
	store, err := newPostgresCache(ctx, "postgres://postgres@localhost:5432/test", nil)
	require.NoError(t, err)
	const id = int64(987654323)
	t.Cleanup(func() {
		cleanup := context.Background()
		for _, key := range []string{AuthorKey(id), shallowAuthorKey(id), refreshAuthorKey(id), authorRetryKey(id)} {
			require.NoError(t, store.Delete(cleanup, key))
		}
		store.db.Close()
	})
	get := NewMockgetter(gomock.NewController(t))
	c, err := NewController(store, get, &Persister{cache: store}, nil)
	require.NoError(t, err)
	author, _ := json.Marshal(AuthorResource{ForeignID: id, Name: "A"})
	store.Set(ctx, AuthorKey(id), author, time.Hour)
	require.NoError(t, c.persister.Persist(ctx, id, author))
	c.saveAuthorRetry(ctx, id, authorRetryState{Attempts: 6, Stopped: true})
	restarted, err := NewController(store, get, c.persister, nil)
	require.NoError(t, err)
	require.True(t, restarted.authorRetryState(ctx, id).Stopped, "restart must not reset an exhausted retry budget")
	get.EXPECT().GetAuthor(gomock.Any(), id).Return(author, nil)
	queued := make(chan refreshAuthor, 1)
	go func() { queued <- <-restarted.refreshC }()
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodDelete, fmt.Sprintf("/author/%d", id), nil)
	NewHandler(restarted).getAuthorID(recorder, request)
	require.Equal(t, http.StatusOK, recorder.Code)
	select {
	case r := <-queued:
		require.Equal(t, id, r.id)
	case <-time.After(time.Second):
		t.Fatal("explicit refresh did not start after retry budget exhaustion")
	}
	_, _, err = restarted.GetAuthor(ctx, id)
	require.NoError(t, err)
	state := restarted.authorRetryState(ctx, id)
	require.False(t, state.Stopped)
	require.Zero(t, state.Attempts)
}

func TestExplicitRefreshPreservesFullSnapshot(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			ctx := t.Context()
			store, err := newPostgresCache(ctx, "postgres://postgres@localhost:5432/test", nil)
			require.NoError(t, err)
			const id = int64(987654324)
			t.Cleanup(func() {
				for _, key := range []string{AuthorKey(id), refreshAuthorKey(id), shallowAuthorKey(id), authorRetryKey(id)} {
					require.NoError(t, store.Delete(context.Background(), key))
				}
				store.db.Close()
			})
			get := NewMockgetter(gomock.NewController(t))
			p := &Persister{cache: store}
			c, err := NewController(store, get, p, nil)
			require.NoError(t, err)
			full, _ := json.Marshal(AuthorResource{ForeignID: id, Name: "full", Works: []workResource{{ForeignID: 1}, {ForeignID: 2}}})
			partial, _ := json.Marshal(AuthorResource{ForeignID: id, Name: "partial"})
			store.Set(ctx, AuthorKey(id), partial, time.Hour)
			require.NoError(t, p.Persist(ctx, id, full))
			require.NoError(t, store.Expire(ctx, AuthorKey(id)))
			require.NoError(t, store.Expire(ctx, refreshAuthorKey(id)))
			if fail {
				get.EXPECT().GetAuthor(gomock.Any(), id).Return(nil, &upstreamError{Code: 429, Daily: true, RetryAt: time.Now().Add(time.Hour)})
			} else {
				get.EXPECT().GetAuthor(gomock.Any(), id).Return(partial, nil)
				go func() { <-c.refreshC }()
			}
			b, _, err := c.GetAuthor(ctx, id)
			require.NoError(t, err)
			require.Equal(t, full, b)
			saved, ok := store.Get(ctx, refreshAuthorKey(id))
			require.True(t, ok)
			require.Equal(t, full, saved, "explicit refresh overwrote the full snapshot with a partial catalogue")
		})
	}
}

func TestForcedRefreshDuringActiveJobDoesNotPersistDuplicate(t *testing.T) {
	ctx := t.Context()
	store, err := newPostgresCache(ctx, "postgres://postgres@localhost:5432/test", nil)
	require.NoError(t, err)
	const id = int64(987654325)
	t.Cleanup(func() {
		for _, key := range []string{AuthorKey(id), refreshAuthorKey(id), authorRetryKey(id)} {
			require.NoError(t, store.Delete(context.Background(), key))
		}
		store.db.Close()
	})
	c, err := NewController(store, nil, &Persister{cache: store}, nil)
	require.NoError(t, err)
	full := []byte(`{"foreignId":987654325,"name":"saved full"}`)
	partial := []byte(`{"foreignId":987654325,"name":"partial"}`)
	store.Set(ctx, AuthorKey(id), partial, time.Hour)
	require.NoError(t, c.persister.Persist(ctx, id, full))
	require.NoError(t, store.Expire(ctx, AuthorKey(id)))
	require.NoError(t, store.Expire(ctx, refreshAuthorKey(id)))
	c.refreshing.Store(id, struct{}{})
	b, _, err := c.GetAuthor(ctx, id)
	require.NoError(t, err)
	require.Equal(t, full, b)
	_, ttl, ok := store.GetWithTTL(ctx, refreshAuthorKey(id))
	require.True(t, ok)
	require.LessOrEqual(t, ttl, time.Duration(0), "forced refresh persisted a duplicate that can be discarded after the active job completes")
	require.NoError(t, c.persister.Delete(ctx, id))
	c.refreshing.Delete(id)
	_, ok = store.Get(ctx, refreshAuthorKey(id))
	require.False(t, ok)
}
