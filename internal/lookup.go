package internal

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"
)

type requireFreshKey struct{}

// Coalesced work has a bounded lifetime independent of its first caller. Each
// caller can stop waiting, and only interactive callers can use stale fallback.
func (c *Controller) lookupResource(ctx context.Context, key string, load func(context.Context) (ttlpair, error)) ([]byte, time.Duration, error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	result := c.group.DoChan(key, func() (out any, err error) {
		defer func() {
			if r := recover(); r != nil {
				Log(ctx).Error("panic loading resource", "key", key, "details", r)
				out = ttlpair{}
				err = fmt.Errorf("loading resource failed")
			}
		}()
		shared, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Minute)
		defer cancel()
		return load(context.WithValue(shared, requireFreshKey{}, true))
	})
	select {
	case p := <-result:
		pair := p.Val.(ttlpair)
		if p.Err != nil && ctx.Value(backgroundRequestKey{}) == nil && !errors.Is(p.Err, errNotFound) && !errors.Is(p.Err, errBadRequest) {
			cached := pair.bytes
			if len(cached) == 0 {
				cached, _ = c.cache.Get(ctx, key)
			}
			if len(cached) > 0 && !slices.Equal(cached, _missing) {
				return cached, time.Minute, nil
			}
		}
		return pair.bytes, pair.ttl, p.Err
	case <-ctx.Done():
		return nil, 0, ctx.Err()
	}
}
