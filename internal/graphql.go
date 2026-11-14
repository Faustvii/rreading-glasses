package internal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math/rand/v2"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/Khan/genqlient/graphql"
	"github.com/bytedance/sonic"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/graphql-go/graphql/language/ast"
	"github.com/graphql-go/graphql/language/parser"
	"github.com/graphql-go/graphql/language/printer"
	"github.com/graphql-go/graphql/language/source"
	"github.com/graphql-go/graphql/language/visitor"
	"github.com/prometheus/client_golang/prometheus"
)

// batchedgqlclient accumulates queries and executes them in batch in order to
// make better use of RPS limits.
type batchedgqlclient struct {
	mu sync.Mutex

	batchSize int            // batchSize is the max number of queries per batch.
	queue     []batchedQuery // queue contains spillover in cases where we've accumulated more queries than our batch size allows.
	every     time.Duration  // every controls how often requests are flushed.
	metrics   *gqlMetrics    // metrics tracks batches and queries sent.

	inflight chan struct{}
	wrapped  graphql.Client
}

// NewBatchedGraphQLClient creates a batching GraphQL client. Queries are
// accumulated and executed regularly accurding to the given rate.
func NewBatchedGraphQLClient(url string, client *http.Client, every time.Duration, batchSize int, reg *prometheus.Registry) (graphql.Client, error) {
	return newBatchedGraphQLClient(graphql.NewClient(url, client), every, batchSize, reg)
}

func newBatchedGraphQLClient(wrapped graphql.Client, every time.Duration, batchSize int, reg *prometheus.Registry) (graphql.Client, error) {
	if batchSize < 1 || every <= 0 {
		return nil, fmt.Errorf("batch size and interval must be positive")
	}

	c := &batchedgqlclient{
		batchSize: batchSize,
		inflight:  make(chan struct{}, 4),
		wrapped:   wrapped,
		queue:     []batchedQuery{},
		metrics:   newGQLMetrics(reg),
		every:     every,
	}

	if hc, ok := wrapped.(*hardcoverClient); ok {
		hc.metrics = c.metrics
		c.inflight = make(chan struct{}, 1)
	}
	go func() {
		ctx := context.WithValue(context.Background(), middleware.RequestIDKey, fmt.Sprintf("batch-flush-%d", time.Now().Unix()))
		for {
			time.Sleep(c.every)
			c.flush(ctx)
		}
	}()

	// Log gql stats every minute.
	go func() {
		ctx := context.Background()
		for {
			time.Sleep(1 * time.Minute)
			batchesWaiting := c.metrics.batchesWaitingGet()
			batchesSent := c.metrics.batchesSentGet()
			queriesSent := c.metrics.queriesSentGet()

			Log(ctx).Debug("query stats",
				"batchesWaiting", batchesWaiting,
				"batchesSent", batchesSent,
				"queriesSent", queriesSent,
				"averageBatchSize", (float32(queriesSent) / float32(batchesSent)),
			)
		}
	}()

	return c, nil
}

// flush pops the oldest batchedQuery off the queue and executes it.
// Individualized errors are returned to listeners if possible, so one query
// can fail without the entire batch failing. The whole batch can still fail in
// other cases, e.g. 4XX response codes.
func (c *batchedgqlclient) flush(ctx context.Context) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.metrics.batchesWaitingSet(len(c.queue))

	if len(c.queue) == 0 {
		return // Nothing to do yet.
	}

	select {
	case c.inflight <- struct{}{}:
	default:
		return
	}
	// Prefer batches containing an interactive caller.
	for i, candidate := range c.queue {
		interactive := false
		for _, sub := range candidate.subscribers {
			if sub.ctx.Value(backgroundRequestKey{}) == nil {
				interactive = true
				break
			}
		}
		if interactive {
			c.queue[0], c.queue[i] = c.queue[i], c.queue[0]
			break
		}
	}
	// Take our oldest batch off the queue.
	batch := c.queue[0]
	c.queue = c.queue[1:]
	// Rebuild from active callers so canceled queries consume no upstream tokens.
	active := batchedQuery{qb: newQueryBuilder(), subscribers: map[string]*subscription{}}
	for _, sub := range batch.subscribers {
		if sub.ctx.Err() != nil {
			continue
		}
		var vars map[string]any
		b, _ := json.Marshal(sub.req.Variables)
		_ = json.Unmarshal(b, &vars)
		id, field, err := active.qb.add(sub.req.Query, vars)
		if err != nil {
			sub.respC <- err
			continue
		}
		sub.field = field
		active.subscribers[id] = sub
	}
	batch = active
	if len(batch.subscribers) == 0 {
		<-c.inflight
		return
	}

	c.metrics.batchesSentInc()
	c.metrics.queriesSentAdd(int64(len(batch.subscribers)))

	query, vars, err := batch.qb.build()
	if err != nil {
		Log(ctx).Error("unable to build query", "err", err)
		for _, sub := range batch.subscribers {
			sub.respC <- err
		}
		<-c.inflight
		return
	}

	opName := ""
	if batch.qb.op.Name != nil {
		opName = batch.qb.op.Name.Value
	}
	data := map[string]any{}
	req := &graphql.Request{
		Query:     query,
		Variables: vars,
		OpName:    opName,
	}
	resp := &graphql.Response{
		Data: &data,
	}

	// Issue the request in a separate goroutine so we can continue to
	// accumulate queries without needing to wait for the network call.
	go func(batch batchedQuery) {
		defer func() { <-c.inflight }()
		ctx, cancel := context.WithTimeout(ctx, 120*time.Second)
		defer cancel()
		var subscribersMu sync.Mutex
		remaining := len(batch.subscribers)
		var stops []func() bool
		for _, sub := range batch.subscribers {
			stops = append(stops, context.AfterFunc(sub.ctx, func() {
				subscribersMu.Lock()
				defer subscribersMu.Unlock()
				remaining--
				if remaining == 0 {
					cancel()
				}
			}))
		}
		defer func() {
			for _, stop := range stops {
				stop()
			}
		}()

		// Retry the whole batch on rate-limit (429) / transient (5xx)
		// responses with exponential backoff, so a burst against a
		// rate-limited upstream (e.g. Hardcover's free tier) doesn't just
		// silently drop every subscriber's result.
		var err error
		backoff := 2 * time.Second
		for attempt := range 5 {
			data = map[string]any{}
			resp.Data = &data
			resp.Errors = nil
			err = c.wrapped.MakeRequest(ctx, req, resp)
			if err == nil {
				break
			}
			if !retryableBatchError(err) || attempt == 4 {
				break
			}
			// Hardcover handles retries through its shared scheduler. Other
			// upstreams retain bounded retries using genqlient's typed status.
			if _, ok := c.wrapped.(*hardcoverClient); ok {
				break
			}
			if waitErr := waitHardcover(ctx, backoff); waitErr != nil {
				err = waitErr
				break
			}

			backoff *= 2
		}

		// Extract any field-level errors, and return them to their
		// subscribers. We can ignore the top-level err in this case, because
		// it's just the wrapped version of our response errors.
		if resp != nil && len(resp.Errors) > 0 {
			for _, e := range resp.Errors {
				if len(e.Path) == 0 {
					for id, sub := range batch.subscribers {
						sub.respC <- errors.Join(gqlStatusErr(e), statusErr(502))
						delete(batch.subscribers, id)
					}
					break
				}
				id := fmt.Sprint(e.Path[0])
				sub, ok := batch.subscribers[id]
				if !ok {
					continue
				}
				sub.respC <- errors.Join(gqlStatusErr(e), statusErr(502))
				// Remove our subscriber because we already responded.
				delete(batch.subscribers, id)
			}
		} else if err != nil {
			// For everything else return the status code to all our subscribers.
			Log(ctx).Warn("batched query error", "count", len(batch.subscribers), "err", err, "resp.Errors", resp.Errors)
			for _, sub := range batch.subscribers {
				sub.respC <- gqlStatusErr(err)
			}
			return
		}

		for id, sub := range batch.subscribers {
			if _, present := data[id]; !present {
				sub.respC <- &upstreamError{Code: 502}
				continue
			}
			byt, err := json.Marshal(map[string]any{
				sub.field: data[id],
			})
			if err != nil {
				sub.respC <- err
				continue
			}

			sub.respC <- sonic.ConfigStd.Unmarshal(byt, &sub.resp.Data)
		}
	}(batch)
}

// MakeRequest implements graphql.Client.
func (c *batchedgqlclient) MakeRequest(
	ctx context.Context,
	req *graphql.Request,
	resp *graphql.Response,
) error {
	var raw json.RawMessage
	result := &graphql.Response{Data: &raw}
	sub := c.enqueue(ctx, req, result)
	select {
	case err := <-sub.respC:
		if err != nil {
			return err
		}
		return json.Unmarshal(raw, resp.Data)
	case <-ctx.Done():
		return ctx.Err()
	}
}

// enqueue adds a query to the batch and returns a subscription whose result
// channel resolves when the batch is executed.
func (c *batchedgqlclient) enqueue(
	ctx context.Context,
	req *graphql.Request,
	resp *graphql.Response,
) *subscription {
	c.mu.Lock()
	defer c.mu.Unlock()

	respC := make(chan error, 1)
	sub := &subscription{ctx: ctx, resp: resp, respC: respC, req: req}
	if len(c.queue) >= 256 {
		respC <- &upstreamError{Code: 503, RetryAt: time.Now().Add(time.Minute)}
		return sub
	}
	// Take the youngest batch if it isn't full yet, otherwise start a new batch.
	if len(c.queue) == 0 || len(c.queue[len(c.queue)-1].subscribers) >= c.batchSize {
		c.queue = append(c.queue, batchedQuery{
			qb:          newQueryBuilder(),
			subscribers: map[string]*subscription{},
		})
	}
	batch := c.queue[len(c.queue)-1]

	var vars map[string]any
	out, _ := json.Marshal(req.Variables)
	_ = sonic.ConfigStd.Unmarshal(out, &vars)

	id, field, err := batch.qb.add(req.Query, vars)
	if err != nil {
		respC <- err
		return sub
	}

	batch.subscribers[id] = &subscription{
		ctx:   ctx,
		resp:  resp,
		respC: respC,
		field: field,
		req:   req,
	}

	return sub
}

// subscription holds information about a caller who is waiting for a query to
// be resolved as part of a batch.
type backgroundRequestKey struct{}

type subscription struct {
	req   *graphql.Request
	ctx   context.Context
	resp  *graphql.Response
	respC chan error
	field string
}

// gqlStatusErr translates errors into meaningful status codes. The client
// normally returns error responses with a 200 OK status code and a populated
// "Errors" field containing stringed errors. We want to instead surface e.g.
// 404 errors directly.
//
// The error is returned unchanged if it doesn't include a status code.
func gqlStatusErr(err error) error {
	var httpErr *graphql.HTTPError
	if errors.As(err, &httpErr) {
		return errors.Join(err, statusErr(httpErr.StatusCode))
	}
	errStr := err.Error()
	idx := strings.Index(errStr, "Request failed with status code")
	if idx == -1 {
		return err
	}
	code, _ := pathToID(errStr[idx:])
	return errors.Join(err, statusErr(code))
}

// queryBuilder accumulates queries into one query with multiple fields so they
// can all be executed as part of one request.
type queryBuilder struct {
	op        *ast.OperationDefinition
	fragments map[string]string
	vars      map[string]any
}

type batchedQuery struct {
	qb          *queryBuilder
	subscribers map[string]*subscription
}

// newQueryBuilder initializes a new QueryBuilder with an empty Document.
func newQueryBuilder() *queryBuilder {
	return &queryBuilder{
		vars:      make(map[string]any),
		fragments: map[string]string{},
	}
}

var runes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

// randRunes returns a short random string of length n.
func randRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = runes[rand.IntN(len(runes))]
	}
	return string(b)
}

// add extends the current query with a new field. The field's alias and name
// are returned so they can be recovered later.
func (qb *queryBuilder) add(query string, vars map[string]any) (id string, field string, err error) {
	src := source.NewSource(&source.Source{
		Body: []byte(query),
	})

	parsedDoc, err := parser.Parse(parser.ParseParams{Source: src})
	if err != nil {
		return "", "", fmt.Errorf("failed to parse query: %w", err)
	}

	id = randRunes(8)

	varRename := make(map[string]string)

	// TODO: Only handle one def
	for _, def := range parsedDoc.Definitions {
		// Include fragments, if there are any, and cache their strings because
		// they don't change.
		if fragDef, ok := def.(*ast.FragmentDefinition); ok {
			name := fragDef.Name.Value
			if _, seen := qb.fragments[name]; !seen {
				qb.fragments[name] = printer.Print(fragDef).(string)
			}
		}

		opDef, ok := def.(*ast.OperationDefinition)
		if !ok {
			continue
		}

		if qb.op == nil {
			qb.op = opDef
		}

		// Visit the AST to rename vars and alias fields
		opts := visitor.VisitInParallel(&visitor.VisitorOptions{
			Enter: func(p visitor.VisitFuncParams) (string, any) {
				switch node := p.Node.(type) {
				case *ast.VariableDefinition:
					oldName := node.Variable.Name.Value
					newName := id + "_" + oldName
					varRename[oldName] = newName
					node.Variable.Name.Value = newName
					qb.vars[newName] = vars[oldName]
				case *ast.Variable:
					if newName, ok := varRename[node.Name.Value]; ok {
						node.Name.Value = newName
					}
				case *ast.Field:
					if len(p.Ancestors) == 3 {
						field = node.Name.Value
						node.Alias = &ast.Name{Value: id, Kind: "Name"}
					}
				}
				return visitor.ActionNoChange, nil
			},
		})
		visitor.Visit(opDef, opts, nil)

		if qb.op == opDef {
			continue
		}

		qb.op.SelectionSet.Selections = append(qb.op.SelectionSet.Selections, opDef.SelectionSet.Selections...)
		qb.op.VariableDefinitions = append(qb.op.VariableDefinitions, opDef.VariableDefinitions...)
	}

	return id, field, nil
}

// Build returns the merged query string and variables map.
func (qb *queryBuilder) build() (string, map[string]any, error) {
	builder := strings.Builder{}

	builder.WriteString(printer.Print(qb.op).(string))

	for _, fragName := range slices.Sorted(maps.Keys(qb.fragments)) {
		builder.WriteString("\n")
		builder.WriteString(qb.fragments[fragName])
	}

	return builder.String(), qb.vars, nil
}

// The Goodreads transport returns statusErr before genqlient can construct an
// HTTPError. Both preserve the status through wrapping by http.Client.
func retryableBatchError(err error) bool {
	var h *graphql.HTTPError
	var s statusErr
	code := 0
	if errors.As(err, &h) {
		code = h.StatusCode
	} else if errors.As(err, &s) {
		code = s.Status()
	}
	return code == 429 || (code >= 500 && code <= 599)
}
