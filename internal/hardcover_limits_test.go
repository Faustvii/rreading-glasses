package internal

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Khan/genqlient/graphql"
	"github.com/stretchr/testify/require"
)

func TestHardcoverDailyCooldown(t *testing.T) {
	cache := newMemoryCache()
	var calls atomic.Int32
	transport := roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		calls.Add(1)
		return &http.Response{StatusCode: 429, Header: http.Header{"Ratelimit": {`"daily";r=0;t=18977`}, "Retry-After": {"18977"}}, Body: io.NopCloser(strings.NewReader(`{"error":"Too Many Requests"}`))}, nil
	})
	client := newHardcoverClient(&http.Client{Transport: transport}, cache, "test", nil)
	req := &graphql.Request{Query: `query { me { id } }`}
	for range 2 {
		var data any
		err := client.MakeRequest(t.Context(), req, &graphql.Response{Data: &data})
		var upstream *upstreamError
		require.ErrorAs(t, err, &upstream)
		require.True(t, upstream.Daily)
		require.WithinDuration(t, time.Now().Add(18977*time.Second), upstream.RetryAt, 2*time.Second)
	}
	require.EqualValues(t, 1, calls.Load())
	// Restarting the client must retain the daily cooldown.
	restarted := newHardcoverClient(&http.Client{Transport: transport}, cache, "test", nil)
	require.Error(t, restarted.MakeRequest(t.Context(), req, &graphql.Response{}))
	require.EqualValues(t, 1, calls.Load())
	// A different key must not inherit the cooldown.
	other := newHardcoverClient(&http.Client{Transport: transport}, cache, "other", nil)
	require.Error(t, other.MakeRequest(t.Context(), req, &graphql.Response{}))
	require.EqualValues(t, 2, calls.Load())
}

func TestHardcoverHeaderParsing(t *testing.T) {
	now := time.Unix(1794672000, 0)
	for _, tc := range []struct {
		name    string
		headers http.Header
		daily   bool
		wait    time.Duration
	}{
		{"modern daily", http.Header{"Ratelimit": {`"daily";r=0;t=123`}}, true, 123 * time.Second},
		{"modern burst", http.Header{"Ratelimit": {`"Free";r=0;t=3, "daily";r=5;t=900`}}, false, 3 * time.Second},
		{"legacy daily", http.Header{"X-Ratelimit-Daily-Remaining": {"0"}, "X-Ratelimit-Daily-Reset": {"1794672123"}, "X-Ratelimit-Reset": {"1794672000"}}, true, 123 * time.Second},
		{"retry preferred", http.Header{"Ratelimit": {`"daily";r=0;t=123`}, "Retry-After": {"150"}}, true, 150 * time.Second},
		{"http date", http.Header{"Retry-After": {now.Add(time.Minute).UTC().Format(http.TimeFormat)}}, false, time.Minute},
		{"malformed", http.Header{"Retry-After": {"oops"}}, false, time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := hardcoverRateError(tc.headers, now)
			require.Equal(t, tc.daily, e.Daily)
			require.Equal(t, now.Add(tc.wait), e.RetryAt)
		})
	}
}

func TestHardcoverCapacitySplit(t *testing.T) {
	var costs []int
	transport := roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		var req graphql.Request
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		cost, err := queryCost(req.Query)
		require.NoError(t, err)
		costs = append(costs, cost)
		if cost > 1 {
			return &http.Response{StatusCode: 403, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(`{"errors":["request_exceeds_capacity"]}`))}, nil
		}
		field := "a"
		if strings.Contains(req.Query, "b:") {
			field = "b"
		}
		return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(`{"data":{"` + field + `":{"id":1}}}`))}, nil
	})
	client := newHardcoverClient(&http.Client{Transport: transport}, newMemoryCache(), "test", nil)
	var data map[string]any
	require.NoError(t, client.MakeRequest(t.Context(), &graphql.Request{Query: `query { a: me { id } b: me { id } }`}, &graphql.Response{Data: &data}))
	require.Equal(t, []int{2, 1, 1}, costs)
	require.Len(t, data, 2)
}

func TestHardcoverQueryCost(t *testing.T) {
	cost, err := queryCost(`query { ...Top ... on Query { b: me { id } } } fragment Top on Query { a: me { id } }`)
	require.NoError(t, err)
	require.Equal(t, 2, cost)
}

func TestHardcoverCancelledWait(t *testing.T) {
	client := newHardcoverClient(&http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) { t.Fatal("unexpected network request"); return nil, nil })}, newMemoryCache(), "test", nil)
	client.tokens = 0
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	err := client.MakeRequest(ctx, &graphql.Request{Query: `{me{id}}`}, &graphql.Response{})
	require.True(t, errors.Is(err, context.Canceled))
}

func TestHardcoverWeightedPacingAndRetries(t *testing.T) {
	{
		start := time.Now()
		now := start
		var times []time.Duration
		calls := 0
		transport := roundTripperFunc(func(r *http.Request) (*http.Response, error) {
			times = append(times, now.Sub(start))
			calls++
			if calls == 2 {
				return &http.Response{StatusCode: 429, Header: http.Header{"Retry-After": {"3"}}, Body: io.NopCloser(strings.NewReader(`{}`))}, nil
			}
			return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(`{"data":{}}`))}, nil
		})
		client := newHardcoverClient(&http.Client{Transport: transport}, newMemoryCache(), "test", nil)
		client.now = func() time.Time { return now }
		client.wait = func(ctx context.Context, d time.Duration) error { now = now.Add(d); return ctx.Err() }
		client.updated = now
		req := &graphql.Request{Query: `query {a:me{id} b:me{id} c:me{id} d:me{id} e:me{id} f:me{id} g:me{id} h:me{id}}`}
		for range 2 {
			var data map[string]any
			require.NoError(t, client.MakeRequest(t.Context(), req, &graphql.Response{Data: &data}))
		}
		require.Equal(t, []time.Duration{0, 6 * time.Second, 14 * time.Second}, times, "retries must acquire eight tokens again, even when Retry-After is shorter")
	}
}

func TestHardcoverSplitPrunesVariablesAndFragments(t *testing.T) {
	parts, err := splitHardcoverQuery(&graphql.Request{Query: `query Test($a:Int!,$b:Int!){ a:books_by_pk(id:$a){...A} b:books_by_pk(id:$b){...B} } fragment A on books {id} fragment B on books {title}`}, 1)
	require.NoError(t, err)
	require.Len(t, parts, 2)
	require.Contains(t, parts[0].Query, "$a")
	require.NotContains(t, parts[0].Query, "$b")
	require.Contains(t, parts[0].Query, "fragment A")
	require.NotContains(t, parts[0].Query, "fragment B")
	require.Contains(t, parts[1].Query, "$b")
	require.NotContains(t, parts[1].Query, "$a")
	require.NotContains(t, parts[1].Query, "fragment A")
	require.Contains(t, parts[1].Query, "fragment B")
}

func TestHardcoverRetryResponse(t *testing.T) {
	response := httptest.NewRecorder()
	NewHandler(nil).error(response, &upstreamError{Code: 429, Daily: true, RetryAt: time.Now().Add(time.Hour)})
	require.Equal(t, 429, response.Code)
	seconds, err := strconv.Atoi(response.Header().Get("Retry-After"))
	require.NoError(t, err)
	require.InDelta(t, 3600, seconds, 1)
	for _, code := range []int{400, 401, 403} {
		response = httptest.NewRecorder()
		NewHandler(nil).error(response, &upstreamError{Code: code})
		require.Equal(t, 502, response.Code, "upstream credentials and query errors are not caller authentication failures")
	}
}

func TestHardcoverIteratorReturnsPaginationFailure(t *testing.T) {
	client := newHardcoverClient(&http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 429, Header: http.Header{"Ratelimit": {`"daily";r=0;t=123`}}, Body: io.NopCloser(strings.NewReader(`{}`))}, nil
	})}, newMemoryCache(), "test", nil)
	getter, err := NewHardcoverGetter(newMemoryCache(), client)
	require.NoError(t, err)
	yielded := 0
	for _, err := range getter.GetAuthorBooks(t.Context(), 42) {
		yielded++
		require.ErrorIs(t, err, statusErr(429))
	}
	require.Equal(t, 1, yielded)
}

func TestHardcoverMissingRetryHeadersUsesBoundedBackoff(t *testing.T) {
	calls := 0
	now := time.Now()
	start := now
	client := newHardcoverClient(&http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
		calls++
		return &http.Response{StatusCode: 429, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(`{}`))}, nil
	})}, newMemoryCache(), "test", nil)
	client.updated = now
	client.now = func() time.Time { return now }
	client.wait = func(ctx context.Context, d time.Duration) error { now = now.Add(d); return ctx.Err() }
	var data any
	require.Error(t, client.MakeRequest(t.Context(), &graphql.Request{Query: `query { me { id } }`}, &graphql.Response{Data: &data}))
	require.Equal(t, 3, calls)
	require.GreaterOrEqual(t, now.Sub(start), 3*time.Second)
	require.Less(t, now.Sub(start), 4*time.Second, "the last failure must not add a final sleep")
}
