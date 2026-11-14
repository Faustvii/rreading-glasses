package internal

import (
	"context"
	"iter"
	"net/http"
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
	require.Equal(t, failed.RetryAt.Unix(), restarted.authorRetryAt(ctx, id).Unix())
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
