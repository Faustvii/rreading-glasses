package internal

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func newCatalogTestController(t *testing.T) (*Controller, cache[[]byte], *Mockgetter) {
	t.Helper()
	store := newMemoryCache()
	getter := NewMockgetter(gomock.NewController(t))
	ctrl, err := NewController(store, getter, &Persister{cache: store}, nil)
	require.NoError(t, err)
	return ctrl, store, getter
}

func getAuthorResponse(t *testing.T, ctrl *Controller, target string) *httptest.ResponseRecorder {
	t.Helper()
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, target, nil)
	NewHandler(ctrl).getAuthorID(recorder, request)
	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	return recorder
}

func TestCatalogStatusHeader(t *testing.T) {
	const authorID = int64(1001)
	shallow, _ := json.Marshal(AuthorResource{ForeignID: authorID, Name: "A", Works: []workResource{{ForeignID: 1}}})
	full, _ := json.Marshal(AuthorResource{ForeignID: authorID, Name: "A", Works: []workResource{{ForeignID: 1}, {ForeignID: 2}}})
	partialCache := fmt.Sprintf("public, s-maxage=%d", int(_partialAuthorTTL.Seconds()))

	t.Run("first load is loading", func(t *testing.T) {
		ctrl, _, getter := newCatalogTestController(t)
		getter.EXPECT().GetAuthor(gomock.Any(), authorID).Return(shallow, nil)

		queued := make(chan refreshAuthor, 1)
		go func() { queued <- <-ctrl.refreshC }()

		got := getAuthorResponse(t, ctrl, fmt.Sprintf("/author/%d", authorID))
		assert.Equal(t, "loading", got.Header().Get(_catalogStatusHeader))
		assert.Equal(t, partialCache, got.Header().Get("Cache-Control"))
		assert.JSONEq(t, string(shallow), got.Body.String())

		select {
		case r := <-queued:
			assert.Equal(t, authorID, r.id)
		case <-time.After(time.Second):
			t.Fatal("first load did not queue a catalogue refresh")
		}
	})

	t.Run("snapshot during refresh is loading", func(t *testing.T) {
		ctrl, store, _ := newCatalogTestController(t)
		store.Set(t.Context(), AuthorKey(authorID), full, time.Hour)
		require.NoError(t, ctrl.persister.Persist(t.Context(), authorID, shallow))

		got := getAuthorResponse(t, ctrl, fmt.Sprintf("/author/%d", authorID))
		assert.Equal(t, "loading", got.Header().Get(_catalogStatusHeader))
		assert.Equal(t, partialCache, got.Header().Get("Cache-Control"))
		assert.JSONEq(t, string(shallow), got.Body.String())
	})

	t.Run("running refresh is loading", func(t *testing.T) {
		ctrl, store, _ := newCatalogTestController(t)
		// A catalogue job is running, for example after an explicit refresh
		// removed the pending snapshot.
		store.Set(t.Context(), AuthorKey(authorID), shallow, time.Hour)
		ctrl.refreshing.Store(authorID, struct{}{})

		got := getAuthorResponse(t, ctrl, fmt.Sprintf("/author/%d", authorID))
		assert.Equal(t, "loading", got.Header().Get(_catalogStatusHeader))
		assert.Equal(t, partialCache, got.Header().Get("Cache-Control"))
		assert.JSONEq(t, string(shallow), got.Body.String())
	})

	t.Run("edition lookup during refresh is loading", func(t *testing.T) {
		ctrl, store, _ := newCatalogTestController(t)
		const bookID = int64(8)
		work, _ := json.Marshal(workResource{ForeignID: 1, Books: []bookResource{{ForeignID: bookID}}})
		store.Set(t.Context(), AuthorKey(authorID), full, time.Hour)
		store.Set(t.Context(), BookKey(bookID), work, 24*time.Hour)
		require.NoError(t, ctrl.persister.Persist(t.Context(), authorID, shallow))

		got := getAuthorResponse(t, ctrl, fmt.Sprintf("/author/%d?edition=%d", authorID, bookID))
		assert.Equal(t, "loading", got.Header().Get(_catalogStatusHeader))
		assert.Equal(t, partialCache, got.Header().Get("Cache-Control"))
	})

	t.Run("full catalogue is complete", func(t *testing.T) {
		ctrl, store, _ := newCatalogTestController(t)
		store.Set(t.Context(), AuthorKey(authorID), full, 24*time.Hour)

		got := getAuthorResponse(t, ctrl, fmt.Sprintf("/author/%d", authorID))
		assert.Equal(t, "complete", got.Header().Get(_catalogStatusHeader))
		assert.NotEqual(t, partialCache, got.Header().Get("Cache-Control"))
		assert.JSONEq(t, string(full), got.Body.String())
	})

	t.Run("truncated catalogue keeps the normal cache time", func(t *testing.T) {
		ctrl, store, _ := newCatalogTestController(t)
		store.Set(t.Context(), AuthorKey(authorID), full, 24*time.Hour)
		store.Set(t.Context(), truncatedAuthorKey(authorID), []byte("upstream page limit"), time.Hour)

		got := getAuthorResponse(t, ctrl, fmt.Sprintf("/author/%d", authorID))
		assert.Equal(t, "truncated", got.Header().Get(_catalogStatusHeader))
		assert.NotEqual(t, partialCache, got.Header().Get("Cache-Control"))
		assert.JSONEq(t, string(full), got.Body.String())
	})

	t.Run("refresh of a truncated catalogue is loading", func(t *testing.T) {
		ctrl, store, _ := newCatalogTestController(t)
		store.Set(t.Context(), AuthorKey(authorID), full, time.Hour)
		store.Set(t.Context(), truncatedAuthorKey(authorID), []byte("upstream page limit"), time.Hour)
		require.NoError(t, ctrl.persister.Persist(t.Context(), authorID, full))

		got := getAuthorResponse(t, ctrl, fmt.Sprintf("/author/%d", authorID))
		assert.Equal(t, "loading", got.Header().Get(_catalogStatusHeader))
		assert.Equal(t, partialCache, got.Header().Get("Cache-Control"))
	})
}
