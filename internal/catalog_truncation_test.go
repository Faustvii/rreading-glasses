package internal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"testing"
	"time"

	"github.com/Khan/genqlient/graphql"
	"github.com/blampe/rreading-glasses/gr"
	"github.com/blampe/rreading-glasses/hardcover"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vektah/gqlparser/v2/gqlerror"
	"go.uber.org/mock/gomock"
)

// newTruncationTestController seeds author 42 with one fetchable work (7,
// best edition 8), an empty catalogue, and a pending refresh snapshot.
func newTruncationTestController(t *testing.T) (*Controller, cache[[]byte], *Mockgetter) {
	t.Helper()
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
	get.EXPECT().GetWork(gomock.Any(), int64(7), nil).Return(work, int64(0), nil).AnyTimes()
	return c, store, get
}

func authorWorkCount(t *testing.T, store cache[[]byte], authorID int64) int {
	t.Helper()
	b, ok := store.Get(t.Context(), AuthorKey(authorID))
	require.True(t, ok)
	var author AuthorResource
	require.NoError(t, json.Unmarshal(b, &author))
	return len(author.Works)
}

func TestUpstreamPageLimitFinishesTruncated(t *testing.T) {
	c, store, get := newTruncationTestController(t)
	// An earlier version stopped this author on the same error.
	c.saveAuthorRetry(t.Context(), 42, authorRetryState{Attempts: 1, Stopped: true})
	limit := fmt.Errorf("%w: %w", errCatalogTruncated, errors.Join(&gqlerror.Error{Message: _grCursorLimit}, statusErr(502)))
	get.EXPECT().GetAuthorBooks(gomock.Any(), int64(42)).Return(iter.Seq2[int64, error](func(yield func(int64, error) bool) {
		if yield(8, nil) {
			yield(0, limit)
		}
	}))

	require.NoError(t, c.refreshAuthor(t.Context(), 42, nil))

	assert.Equal(t, 1, authorWorkCount(t, store, 42), "works fetched before the limit are published")
	_, ok := store.Get(t.Context(), refreshAuthorKey(42))
	assert.False(t, ok, "the pre-refresh snapshot must not hide the published works")
	_, ok = store.Get(t.Context(), authorRetryKey(42))
	assert.False(t, ok, "a page limit is final, not a retry")
	assert.Equal(t, catalogTruncated, c.AuthorCatalogStatus(t.Context(), 42))
}

func TestCompleteRefreshClearsTruncation(t *testing.T) {
	c, store, get := newTruncationTestController(t)
	store.Set(t.Context(), truncatedAuthorKey(42), []byte("upstream page limit"), time.Hour)
	get.EXPECT().GetAuthorBooks(gomock.Any(), int64(42)).Return(iter.Seq2[int64, error](func(yield func(int64, error) bool) {
		yield(8, nil)
	}))

	require.NoError(t, c.refreshAuthor(t.Context(), 42, nil))

	assert.Equal(t, catalogComplete, c.AuthorCatalogStatus(t.Context(), 42))
}

func TestLocalEditionLimitFinishesTruncated(t *testing.T) {
	c, store, get := newTruncationTestController(t)
	get.EXPECT().GetBook(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, int64(0), int64(0), errNotFound).AnyTimes()
	get.EXPECT().GetAuthorBooks(gomock.Any(), int64(42)).Return(iter.Seq2[int64, error](func(yield func(int64, error) bool) {
		if !yield(8, nil) {
			return
		}
		for id := int64(1000); id < 2001; id++ {
			if !yield(id, nil) {
				return
			}
		}
	}))

	require.NoError(t, c.refreshAuthor(t.Context(), 42, nil))

	assert.Equal(t, 1, authorWorkCount(t, store, 42))
	assert.Equal(t, catalogTruncated, c.AuthorCatalogStatus(t.Context(), 42))
	b, _ := store.Get(t.Context(), truncatedAuthorKey(42))
	assert.Equal(t, "local edition limit", string(b))
}

func TestOtherRefreshErrorsStillDefer(t *testing.T) {
	c, store, get := newTruncationTestController(t)
	get.EXPECT().GetAuthorBooks(gomock.Any(), int64(42)).Return(iter.Seq2[int64, error](func(yield func(int64, error) bool) {
		if yield(8, nil) {
			yield(0, statusErr(503))
		}
	}))

	require.ErrorIs(t, c.refreshAuthor(t.Context(), 42, nil), statusErr(503))

	_, ok := store.Get(t.Context(), refreshAuthorKey(42))
	assert.True(t, ok, "a transient failure keeps the refresh pending")
	assert.Equal(t, catalogLoading, c.AuthorCatalogStatus(t.Context(), 42))
}

// grWorksPage builds a GetAuthorWorks page with one work by author 42.
func grWorksPage(bookID int64, next string) gr.GetAuthorWorksGetWorksByContributorContributorWorksConnection {
	return gr.GetAuthorWorksGetWorksByContributorContributorWorksConnection{
		Edges: []gr.GetAuthorWorksGetWorksByContributorContributorWorksConnectionEdgesContributorWorksEdge{{
			Node: gr.GetAuthorWorksGetWorksByContributorContributorWorksConnectionEdgesContributorWorksEdgeNodeWork{
				BestBook: gr.GetAuthorWorksGetWorksByContributorContributorWorksConnectionEdgesContributorWorksEdgeNodeWorkBestBook{
					LegacyId: bookID,
					PrimaryContributorEdge: gr.GetAuthorWorksGetWorksByContributorContributorWorksConnectionEdgesContributorWorksEdgeNodeWorkBestBookPrimaryContributorEdgeBookContributorEdge{
						Role: "Author",
						Node: gr.GetAuthorWorksGetWorksByContributorContributorWorksConnectionEdgesContributorWorksEdgeNodeWorkBestBookPrimaryContributorEdgeBookContributorEdgeNodeContributor{LegacyId: 42},
					},
				},
			},
		}},
		PageInfo: gr.GetAuthorWorksGetWorksByContributorContributorWorksConnectionPageInfo{HasNextPage: next != "", NextPageToken: next},
	}
}

func TestGRAuthorBookIDsReportsPageLimit(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		truncated bool
	}{
		{
			// The batching client joins field errors with a synthetic 502.
			name:      "cursor limit",
			err:       errors.Join(&gqlerror.Error{Message: _grCursorLimit}, statusErr(502)),
			truncated: true,
		},
		{
			name: "other GraphQL error",
			err:  errors.Join(&gqlerror.Error{Message: "Internal Server Error"}, statusErr(502)),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gql := hardcover.NewMockgql(gomock.NewController(t))
			gql.EXPECT().MakeRequest(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
				func(_ context.Context, req *graphql.Request, res *graphql.Response) error {
					vars := req.Variables.(interface{ GetPagination() gr.PaginationInput })
					if vars.GetPagination().After == "" {
						res.Data.(*gr.GetAuthorWorksResponse).GetWorksByContributor = grWorksPage(8, "eyJuZXh0X3BhZ2UiOjI2fQ")
						return nil
					}
					return tt.err
				}).Times(2)
			g, err := NewGRGetter(newMemoryCache(), gql, nil)
			require.NoError(t, err)

			var ids []int64
			var last error
			for id, err := range g.authorBookIDs(t.Context(), 42, "kca://author/test") {
				if err != nil {
					last = err
					break
				}
				ids = append(ids, id)
			}

			assert.Equal(t, []int64{8}, ids)
			require.Error(t, last)
			assert.Equal(t, tt.truncated, errors.Is(last, errCatalogTruncated))
		})
	}
}
