package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Khan/genqlient/graphql"
	"github.com/graphql-go/graphql/language/ast"
	"github.com/graphql-go/graphql/language/parser"
	"github.com/stretchr/testify/require"
)

func rootAliases(t *testing.T, query string) []string {
	t.Helper()
	doc, err := parser.Parse(parser.ParseParams{Source: query})
	require.NoError(t, err)
	fields, _, err := hardcoverFields(doc)
	require.NoError(t, err)
	aliases := make([]string, 0, len(fields))
	for _, sel := range fields {
		f := sel.(*ast.Field)
		name := f.Name.Value
		if f.Alias != nil {
			name = f.Alias.Value
		}
		aliases = append(aliases, name)
	}
	return aliases
}

func readQuery(t *testing.T, r *http.Request) *graphql.Request {
	t.Helper()
	var q graphql.Request
	require.NoError(t, json.NewDecoder(r.Body).Decode(&q))
	return &q
}

func testResponse(code int, h http.Header, body string) *http.Response {
	return &http.Response{StatusCode: code, Header: h, Body: io.NopCloser(strings.NewReader(body))}
}

func receiveBatch(t *testing.T, sub *subscription) error {
	t.Helper()
	select {
	case err := <-sub.respC:
		return err
	case <-time.After(3 * time.Second):
		t.Fatal("batch did not finish")
		return context.DeadlineExceeded
	}
}

func TestGoodreadsRetriesTransportStatus(t *testing.T) {
	for _, code := range []int{429, 503} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			calls := 0
			transport := errorProxyTransport{RoundTripper: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
				calls++
				q := readQuery(t, r)
				if calls == 1 {
					return testResponse(code, nil, `{}`), nil
				}
				alias := rootAliases(t, q.Query)[0]
				return testResponse(200, nil, `{"data":{"`+alias+`":{"id":1}}}`), nil
			})}
			client, err := NewBatchedGraphQLClient("https://example.invalid", &http.Client{Transport: transport}, time.Hour, 8, nil)
			require.NoError(t, err)
			batch := client.(*batchedgqlclient)
			var data json.RawMessage
			sub := batch.enqueue(t.Context(), &graphql.Request{Query: `query Test { me { id } }`}, &graphql.Response{Data: &data})
			batch.flush(t.Context())
			require.NoError(t, receiveBatch(t, sub))
			require.Equal(t, 2, calls)
		})
	}
}

func TestNestedBatchErrorsReleaseSlot(t *testing.T) {
	client, err := NewBatchedGraphQLClient("https://example.invalid", &http.Client{Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		alias := rootAliases(t, readQuery(t, r).Query)[0]
		body := fmt.Sprintf(`{"data":{},"errors":[{"message":"bad","path":[%q,"node",0]},{"message":"bad again","path":[%q,"node",1]}]}`, alias, alias)
		return testResponse(200, nil, body), nil
	})}, time.Hour, 1, nil)
	require.NoError(t, err)
	batch := client.(*batchedgqlclient)
	batch.inflight = make(chan struct{}, 1)
	var data json.RawMessage
	sub := batch.enqueue(t.Context(), &graphql.Request{Query: `query Test { me { id } }`}, &graphql.Response{Data: &data})
	batch.flush(t.Context())
	require.Error(t, receiveBatch(t, sub))
	require.Eventually(t, func() bool { return len(batch.inflight) == 0 }, time.Second, time.Millisecond, "nested errors held the batch slot")
}

func TestSplitVariablesMatchDefinitions(t *testing.T) {
	parts, err := splitHardcoverQuery(&graphql.Request{Query: `query Test($a:Int!,$b:Int!){a:books_by_pk(id:$a){id} b:books_by_pk(id:$b){id}}`, Variables: map[string]any{"a": 1, "b": 2}}, 1)
	require.NoError(t, err)
	require.Len(t, parts, 2)
	require.Equal(t, map[string]any{"a": float64(1)}, parts[0].Variables)
	require.Equal(t, map[string]any{"b": float64(2)}, parts[1].Variables)
}

func TestSplitErrorsStayWithTheirPart(t *testing.T) {
	client := newHardcoverClient(&http.Client{Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		alias := rootAliases(t, readQuery(t, r).Query)[0]
		if alias == "bad" {
			return testResponse(200, nil, `{"errors":[{"message":"invalid field"}]}`), nil
		}
		return testResponse(200, nil, `{"data":{"good":{"id":1}}}`), nil
	})}, newMemoryCache(), "review", nil)
	client.burst = 1
	client.refill = 1000
	var data map[string]any
	resp := &graphql.Response{Data: &data}
	require.Error(t, client.MakeRequest(t.Context(), &graphql.Request{Query: `query Test {bad:me{id} good:me{id}}`}, resp))
	require.Contains(t, data, "good")
	require.Len(t, resp.Errors, 1)
	require.NotEmpty(t, resp.Errors[0].Path)
	require.Equal(t, "bad", fmt.Sprint(resp.Errors[0].Path[0]))
}

func TestSplitTransportFailureDoesNotHideBehindGraphQLError(t *testing.T) {
	client := newHardcoverClient(&http.Client{Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		alias := rootAliases(t, readQuery(t, r).Query)[0]
		if alias == "bad" {
			return testResponse(200, nil, `{"errors":[{"message":"bad","path":["bad"]}]}`), nil
		}
		return testResponse(429, http.Header{"Ratelimit": {`"daily";r=0;t=3600`}}, `{}`), nil
	})}, newMemoryCache(), "review", nil)
	client.burst = 1
	client.refill = 1000
	var data map[string]any
	resp := &graphql.Response{Data: &data}
	require.Error(t, client.MakeRequest(t.Context(), &graphql.Request{Query: `query Test {bad:me{id} limited:me{id} waiting:me{id}}`}, resp))
	affected := map[string]bool{}
	for _, err := range resp.Errors {
		if len(err.Path) > 0 {
			affected[fmt.Sprint(err.Path[0])] = true
		}
	}
	require.Equal(t, map[string]bool{"bad": true, "limited": true, "waiting": true}, affected)
}

func TestBatchMissingAliasIsNotSuccessfulNull(t *testing.T) {
	client, err := NewBatchedGraphQLClient("https://example.invalid", &http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) { return testResponse(200, nil, `{"data":{}}`), nil })}, time.Hour, 1, nil)
	require.NoError(t, err)
	var data json.RawMessage
	batch := client.(*batchedgqlclient)
	sub := batch.enqueue(t.Context(), &graphql.Request{Query: `query { me{id} }`}, &graphql.Response{Data: &data})
	batch.flush(t.Context())
	require.ErrorIs(t, receiveBatch(t, sub), statusErr(503))
}

func TestLearnedCapacitySurvivesPolicyHeaders(t *testing.T) {
	var costs []int
	policy := http.Header{"Ratelimit-Policy": {`"Free";q=600000;w=60;burst=10`}}
	client := newHardcoverClient(&http.Client{Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		q := readQuery(t, r)
		aliases := rootAliases(t, q.Query)
		costs = append(costs, len(aliases))
		if len(aliases) > 1 {
			return testResponse(403, policy, `{"errors":["request_exceeds_capacity"]}`), nil
		}
		return testResponse(200, policy, fmt.Sprintf(`{"data":{%q:{"id":1}}}`, aliases[0])), nil
	})}, newMemoryCache(), "review", nil)
	for range 2 {
		var data map[string]any
		require.NoError(t, client.MakeRequest(t.Context(), &graphql.Request{Query: `query { a:me{id} b:me{id} }`}, &graphql.Response{Data: &data}))
	}
	require.Equal(t, []int{2, 1, 1, 1, 1}, costs)
}
