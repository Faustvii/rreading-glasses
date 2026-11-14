package internal

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Khan/genqlient/graphql"
	"github.com/graphql-go/graphql/language/ast"
	"github.com/graphql-go/graphql/language/parser"
	"github.com/graphql-go/graphql/language/printer"
	"github.com/graphql-go/graphql/language/visitor"
	"github.com/prometheus/client_golang/prometheus"
	gqlast "github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

// upstreamError preserves the HTTP outcome independently of the response body.
// RetryAt is absolute so downstream retries and persisted work share one deadline.
type upstreamError struct {
	Code        int       `json:"code"`
	RetryAt     time.Time `json:"retryAt"`
	Daily       bool      `json:"daily"`
	Capacity    bool      `json:"capacity"`
	ServerRetry bool      `json:"serverRetry,omitempty"`
}

func (e *upstreamError) Error() string { return fmt.Sprintf("upstream HTTP %d", e.Code) }
func (e *upstreamError) Unwrap() error {
	switch {
	case e.Code == 429:
		return statusErr(429)
	case e.Code == 408 || e.Code == 504:
		return statusErr(504)
	case e.Code >= 500:
		return statusErr(503)
	default:
		return statusErr(502)
	}
}

// hardcoverClient serializes attempts for one key, including retries. This lets
// response headers affect the next attempt before any further request is sent.
type hardcoverClient struct {
	now       func() time.Time
	wait      func(context.Context, time.Duration) error
	endpoint  string
	http      *http.Client
	cache     cache[[]byte]
	cacheKey  string
	gate      chan struct{}
	tokens    float64
	burst     int
	maxFields int
	refill    float64
	updated   time.Time
	cooldown  *upstreamError
	metrics   *gqlMetrics
}

// NewHardcoverGraphQLClient batches reads and applies Hardcover's field-based
// token bucket. key is used only to namespace persisted cooldowns by its hash.
func NewHardcoverGraphQLClient(url string, client *http.Client, every time.Duration, batchSize int, reg *prometheus.Registry, store cache[[]byte], key string) (graphql.Client, error) {
	hc := newHardcoverClient(client, store, key, nil)
	hc.endpoint = url
	return newBatchedGraphQLClient(hc, every, batchSize, reg)
}

func newHardcoverClient(client *http.Client, store cache[[]byte], key string, metrics *gqlMetrics) *hardcoverClient {
	c := &hardcoverClient{now: time.Now, wait: waitHardcover, http: client, cache: store, cacheKey: fmt.Sprintf("hc-cooldown-%x", sha256.Sum256([]byte(key))), gate: make(chan struct{}, 1), tokens: 10, burst: 10, refill: 1, updated: time.Now(), metrics: metrics, endpoint: "https://api.hardcover.app/v1/graphql"}
	if store != nil {
		if b, ok := store.Get(context.Background(), c.cacheKey); ok {
			var e upstreamError
			if json.Unmarshal(b, &e) == nil && time.Now().Before(e.RetryAt) {
				c.cooldown = &e
			}
		}
	}
	return c
}

func (c *hardcoverClient) MakeRequest(ctx context.Context, req *graphql.Request, resp *graphql.Response) error {
	select {
	case c.gate <- struct{}{}:
		defer func() { <-c.gate }()
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	parts, err := splitHardcoverQuery(req, c.requestCapacity())
	if err != nil {
		return err
	}
	data := map[string]any{}
	for len(parts) > 0 {
		part := parts[0]
		parts = parts[1:]
		cost, err := queryCost(part.Query)
		if err != nil {
			return err
		}
		if cost > c.requestCapacity() {
			smaller, err := splitHardcoverQuery(part, c.requestCapacity())
			if err != nil {
				return err
			}
			parts = append(smaller, parts...)
			continue
		}
		var out map[string]any
		result := &graphql.Response{Data: &out}
		err = c.execute(ctx, part, result, cost)
		var up *upstreamError
		if errors.As(err, &up) && up.Capacity && cost > 1 {
			c.maxFields = min(c.requestCapacity(), max(1, cost/2))
			smaller, splitErr := splitHardcoverQuery(part, c.requestCapacity())
			if splitErr != nil {
				return splitErr
			}
			parts = append(smaller, parts...)
			continue
		}
		if err != nil && len(result.Errors) == 0 {
			// This part and all remaining parts are unanswered. Keep earlier
			// results, but never turn these failures into successful null data.
			for _, unanswered := range append([]*graphql.Request{part}, parts...) {
				resp.Errors = append(resp.Errors, hardcoverPartErrors(unanswered, err)...)
			}
			break
		}
		for _, fieldErr := range result.Errors {
			if len(fieldErr.Path) == 0 {
				resp.Errors = append(resp.Errors, hardcoverPartErrors(part, fieldErr)...)
			} else {
				resp.Errors = append(resp.Errors, fieldErr)
			}
		}
		for k, v := range out {
			data[k] = v
		}
	}
	b, err := json.Marshal(data)
	if err != nil {
		return err
	}
	if resp.Data != nil {
		if err = json.Unmarshal(b, resp.Data); err != nil {
			return err
		}
	}
	if len(resp.Errors) > 0 {
		return resp.Errors
	}
	return nil
}

func (c *hardcoverClient) execute(ctx context.Context, req *graphql.Request, resp *graphql.Response, cost int) error {
	body, err := json.Marshal(req)
	if err != nil {
		return err
	}
	for attempt := range 3 {
		if err = c.acquire(ctx, cost); err != nil {
			return err
		}
		httpReq, buildErr := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(body))
		if buildErr != nil {
			return buildErr
		}
		httpReq.Header.Set("Content-Type", "application/json")
		response, requestErr := c.http.Do(httpReq)
		if c.metrics != nil {
			c.metrics.totals.WithLabelValues("http_attempts").Inc()
			c.metrics.totals.WithLabelValues("attempted_fields").Add(float64(cost))
		}
		if requestErr != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			err = &upstreamError{Code: 503}
		} else {
			if c.metrics != nil {
				c.metrics.totals.WithLabelValues("http_status_" + strconv.Itoa(response.StatusCode)).Inc()
			}
			c.observe(response.Header, c.now())
			b, readErr := io.ReadAll(io.LimitReader(response.Body, 64<<20))
			_ = response.Body.Close()
			switch {
			case readErr != nil:
				err = &upstreamError{Code: 502}
			case response.StatusCode == 200:
				if err = json.Unmarshal(b, resp); err != nil {
					return &upstreamError{Code: 502}
				}
				if len(resp.Errors) > 0 {
					return resp.Errors
				}
				return nil
			default:
				up := &upstreamError{Code: response.StatusCode}
				if up.Code == 429 {
					up = hardcoverRateError(response.Header, c.now())
					if !up.ServerRetry {
						delay := fuzz(time.Second*time.Duration(1<<attempt), 1.25)
						if up.Daily {
							delay = max(time.Minute, delay)
						}
						up.RetryAt = c.now().Add(delay)
					}
					c.deferUntil(ctx, up)
				}
				if up.Code == 403 {
					var envelope struct {
						Error  string   `json:"error"`
						Errors []string `json:"errors"`
					}
					_ = json.Unmarshal(b, &envelope)
					up.Capacity = envelope.Error == "top_level_limit_exceeded"
					for _, code := range envelope.Errors {
						up.Capacity = up.Capacity || code == "request_exceeds_capacity"
					}
				}
				err = up
			}
		}
		var up *upstreamError
		if !errors.As(err, &up) {
			return err
		}
		if up.Daily || up.Capacity || attempt == 2 {
			return err
		}
		if up.Code != 429 && up.Code != 408 && up.Code < 500 {
			return err
		}
		if up.RetryAt.IsZero() {
			up.RetryAt = c.now().Add(fuzz(time.Second*time.Duration(1<<attempt), 1.25))
			c.deferUntil(ctx, up)
		}
		// Long waits belong to deferred jobs, not an interactive HTTP request.
		if up.RetryAt.Sub(c.now()) > 10*time.Second {
			return err
		}
		if c.metrics != nil {
			c.metrics.totals.WithLabelValues("http_retries").Inc()
		}
	}
	return err
}

func (c *hardcoverClient) acquire(ctx context.Context, cost int) error {
	for {
		now := c.now()
		if c.cooldown != nil && now.Before(c.cooldown.RetryAt) {
			if c.cooldown.Daily || c.cooldown.RetryAt.Sub(now) > 10*time.Second {
				return c.cooldown
			}
			if err := c.wait(ctx, c.cooldown.RetryAt.Sub(now)); err != nil {
				return err
			}
			continue
		}
		c.tokens = math.Min(float64(c.burst), c.tokens+now.Sub(c.updated).Seconds()*c.refill)
		c.updated = now
		if c.tokens >= float64(cost) {
			c.tokens -= float64(cost)
			return nil
		}
		if err := c.wait(ctx, time.Duration((float64(cost)-c.tokens)/c.refill*float64(time.Second))); err != nil {
			return err
		}
	}
}

func waitHardcover(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *hardcoverClient) deferUntil(ctx context.Context, e *upstreamError) {
	if c.cooldown != nil && c.cooldown.RetryAt.After(e.RetryAt) {
		return
	}
	c.cooldown = e
	if c.metrics != nil {
		c.metrics.gauge.WithLabelValues("upstream_retry_at").Set(float64(e.RetryAt.Unix()))
	}
	if c.cache != nil {
		b, _ := json.Marshal(e)
		c.cache.Set(ctx, c.cacheKey, b, e.RetryAt.Sub(c.now())+time.Minute)
	}
	Log(ctx).Warn("Hardcover upstream paused", "status", e.Code, "daily", e.Daily, "retryAt", e.RetryAt)
}

// rateEntries reads the numeric structured-field parameters used by Hardcover.
func rateEntries(header string) map[string]map[string]float64 {
	entries := map[string]map[string]float64{}
	for _, entry := range strings.Split(header, ",") {
		parts := strings.Split(entry, ";")
		name := strings.Trim(strings.TrimSpace(parts[0]), `"`)
		params := map[string]float64{}
		for _, p := range parts[1:] {
			kv := strings.SplitN(strings.TrimSpace(p), "=", 2)
			if len(kv) != 2 {
				continue
			}
			if n, err := strconv.ParseFloat(kv[1], 64); err == nil && !math.IsNaN(n) && !math.IsInf(n, 0) {
				params[kv[0]] = n
			}
		}
		entries[name] = params
	}
	return entries
}

func hardcoverRateError(h http.Header, now time.Time) *upstreamError {
	e := &upstreamError{Code: 429}
	entries := rateEntries(h.Get("RateLimit"))
	daily := entries["daily"]
	remaining, known := daily["r"]
	e.Daily = known && remaining == 0
	if h.Get("RateLimit") == "" {
		e.Daily = h.Get("X-RateLimit-Daily-Remaining") == "0"
	}
	wait := time.Second
	if e.Daily {
		if seconds, ok := daily["t"]; ok && seconds > 0 {
			wait = time.Duration(seconds * float64(time.Second))
			e.ServerRetry = true
		} else {
			wait = legacyReset(h.Get("X-RateLimit-Daily-Reset"), now)
			if n, err := strconv.ParseInt(h.Get("X-RateLimit-Daily-Reset"), 10, 64); err == nil && n > now.Unix() {
				e.ServerRetry = true
			}
		}
	} else {
		for name, params := range entries {
			if name != "daily" {
				if seconds, ok := params["t"]; ok && seconds > 0 {
					wait = time.Duration(seconds * float64(time.Second))
					e.ServerRetry = true
				}
			}
		}
		if h.Get("RateLimit") == "" {
			wait = legacyReset(h.Get("X-RateLimit-Reset"), now)
			if n, err := strconv.ParseInt(h.Get("X-RateLimit-Reset"), 10, 64); err == nil && n > now.Unix() {
				e.ServerRetry = true
			}
		}
	}
	if n, err := strconv.ParseInt(h.Get("Retry-After"), 10, 32); err == nil && n >= 0 {
		wait = time.Duration(n) * time.Second
		e.ServerRetry = true
	} else if when, err := http.ParseTime(h.Get("Retry-After")); err == nil && when.After(now) {
		wait = when.Sub(now)
		e.ServerRetry = true
	}
	e.RetryAt = now.Add(max(time.Second, wait))
	return e
}

func legacyReset(value string, now time.Time) time.Duration {
	n, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return time.Second
	}
	return max(time.Second, time.Unix(n, 0).Sub(now))
}

func (c *hardcoverClient) observe(h http.Header, now time.Time) {
	c.tokens = math.Min(float64(c.burst), c.tokens+now.Sub(c.updated).Seconds()*c.refill)
	c.updated = now
	for name, params := range rateEntries(h.Get("RateLimit-Policy")) {
		if name == "daily" {
			continue
		}
		if b := params["burst"]; b >= 1 && b <= 1000 {
			c.burst = int(b)
		}
		if q, w := params["q"], params["w"]; q > 0 && w > 0 {
			c.refill = q / w
		}
	}
	if h.Get("RateLimit-Policy") == "" {
		if b, err := strconv.Atoi(h.Get("X-RateLimit-Limit")); err == nil && b > 0 && b <= 1000 {
			c.burst = b
		}
	}
	c.tokens = math.Min(c.tokens, float64(c.burst))
	if c.metrics != nil {
		if r, ok := rateEntries(h.Get("RateLimit"))["daily"]["r"]; ok {
			c.metrics.gauge.WithLabelValues("daily_remaining").Set(r)
		} else if r, err := strconv.Atoi(h.Get("X-RateLimit-Daily-Remaining")); err == nil {
			c.metrics.gauge.WithLabelValues("daily_remaining").Set(float64(r))
		}
	}
	for name, params := range rateEntries(h.Get("RateLimit")) {
		if name != "daily" {
			if r, ok := params["r"]; ok && r >= 0 {
				c.tokens = math.Min(c.tokens, r)
			}
		}
	}
	if h.Get("RateLimit") == "" {
		if r, err := strconv.Atoi(h.Get("X-RateLimit-Remaining")); err == nil && r >= 0 {
			c.tokens = math.Min(c.tokens, float64(r))
		}
	}
	// Some gateways announce exhaustion on the last successful response.
	if params := rateEntries(h.Get("RateLimit"))["daily"]; params != nil {
		if r, ok := params["r"]; ok && r == 0 {
			c.deferUntil(context.Background(), hardcoverRateError(h, now))
		}
	}
}

func queryCost(query string) (int, error) {
	doc, err := parser.Parse(parser.ParseParams{Source: query})
	if err != nil {
		return 0, err
	}
	fields, _, err := hardcoverFields(doc)
	return len(fields), err
}

func hardcoverFields(doc *ast.Document) ([]ast.Selection, *ast.OperationDefinition, error) {
	fragments := map[string]*ast.FragmentDefinition{}
	var op *ast.OperationDefinition
	for _, def := range doc.Definitions {
		switch d := def.(type) {
		case *ast.FragmentDefinition:
			fragments[d.Name.Value] = d
		case *ast.OperationDefinition:
			if op != nil {
				return nil, nil, fmt.Errorf("multiple operations are not supported")
			}
			op = d
		}
	}
	if op == nil || op.Operation != "query" {
		return nil, nil, fmt.Errorf("hardcover batching only supports read queries")
	}
	var fields []ast.Selection
	var expand func(*ast.SelectionSet, map[string]bool) error
	expand = func(set *ast.SelectionSet, seen map[string]bool) error {
		for _, sel := range set.Selections {
			switch node := sel.(type) {
			case *ast.Field:
				fields = append(fields, node)
			case *ast.InlineFragment:
				if err := expand(node.SelectionSet, seen); err != nil {
					return err
				}
			case *ast.FragmentSpread:
				name := node.Name.Value
				if seen[name] || fragments[name] == nil {
					return fmt.Errorf("invalid root fragment %q", name)
				}
				seen[name] = true
				if err := expand(fragments[name].SelectionSet, seen); err != nil {
					return err
				}
				delete(seen, name)
			}
		}
		return nil
	}
	if err := expand(op.SelectionSet, map[string]bool{}); err != nil {
		return nil, nil, err
	}
	return fields, op, nil
}

func splitHardcoverQuery(req *graphql.Request, capacity int) ([]*graphql.Request, error) {
	doc, err := parser.Parse(parser.ParseParams{Source: req.Query})
	if err != nil {
		return nil, err
	}
	fields, op, err := hardcoverFields(doc)
	if err != nil {
		return nil, err
	}
	if len(fields) == 0 {
		return nil, fmt.Errorf("query contains no fields")
	}
	if len(fields) <= capacity {
		return []*graphql.Request{req}, nil
	}
	var parts []*graphql.Request
	for start := 0; start < len(fields); start += capacity {
		op.SelectionSet = &ast.SelectionSet{Kind: "SelectionSet", Selections: fields[start:min(start+capacity, len(fields))]}
		copyReq := *req
		// Split operations need only their referenced fragments and variables.
		pruned := *doc
		prunedOp := *op
		usedVars := map[string]bool{}
		usedFragments := map[string]bool{}
		var visit func(ast.Node)
		visit = func(node ast.Node) {
			visitor.Visit(node, visitor.VisitInParallel(&visitor.VisitorOptions{Enter: func(p visitor.VisitFuncParams) (string, any) {
				switch n := p.Node.(type) {
				case *ast.Variable:
					usedVars[n.Name.Value] = true
				case *ast.FragmentSpread:
					name := n.Name.Value
					if !usedFragments[name] {
						usedFragments[name] = true
						for _, def := range doc.Definitions {
							if f, ok := def.(*ast.FragmentDefinition); ok && f.Name.Value == name {
								visit(f)
							}
						}
					}
				}
				return visitor.ActionNoChange, nil
			}}), nil)
		}
		visit(op.SelectionSet)
		prunedOp.VariableDefinitions = nil
		for _, v := range op.VariableDefinitions {
			if usedVars[v.Variable.Name.Value] {
				prunedOp.VariableDefinitions = append(prunedOp.VariableDefinitions, v)
			}
		}
		pruned.Definitions = []ast.Node{&prunedOp}
		for _, def := range doc.Definitions {
			if f, ok := def.(*ast.FragmentDefinition); ok && usedFragments[f.Name.Value] {
				pruned.Definitions = append(pruned.Definitions, f)
			}
		}
		copyReq.Query = printer.Print(&pruned).(string)
		allVars := map[string]any{}
		if req.Variables != nil {
			b, err := json.Marshal(req.Variables)
			if err != nil {
				return nil, err
			}
			if err = json.Unmarshal(b, &allVars); err != nil {
				return nil, err
			}
		}
		partVars := map[string]any{}
		for name := range usedVars {
			if value, ok := allVars[name]; ok {
				partVars[name] = value
			}
		}
		copyReq.Variables = partVars
		parts = append(parts, &copyReq)
	}
	return parts, nil
}

// A transport or pathless GraphQL error affects only aliases in its own part.
// Err retains typed causes (including a daily reset) for each subscriber.
func hardcoverPartErrors(req *graphql.Request, cause error) gqlerror.List {
	doc, err := parser.Parse(parser.ParseParams{Source: req.Query})
	if err != nil {
		return gqlerror.List{&gqlerror.Error{Message: cause.Error(), Err: cause}}
	}
	fields, _, err := hardcoverFields(doc)
	if err != nil {
		return gqlerror.List{&gqlerror.Error{Message: cause.Error(), Err: cause}}
	}
	result := make(gqlerror.List, 0, len(fields))
	for _, sel := range fields {
		field := sel.(*ast.Field)
		name := field.Name.Value
		if field.Alias != nil {
			name = field.Alias.Value
		}
		result = append(result, &gqlerror.Error{Message: cause.Error(), Err: cause, Path: gqlast.Path{gqlast.PathName(name)}})
	}
	return result
}

// A rejected request teaches an independent per-request ceiling. Token-bucket
// policy headers can update burst capacity without forgetting that ceiling.
func (c *hardcoverClient) requestCapacity() int {
	if c.maxFields > 0 {
		return min(c.burst, c.maxFields)
	}
	return c.burst
}
