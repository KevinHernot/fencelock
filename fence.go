package fencelock

import (
	"context"
	"errors"
	"sync"
)

// ErrStaleToken indicates a protected resource rejected an old fencing token.
var ErrStaleToken = errors.New("stale fencing token")

// FencingTokenGuard validates tokens before a protected resource accepts work.
type FencingTokenGuard interface {
	CheckAndAdvance(ctx context.Context, resource string, token int64) error
}

// MemoryFencingTokenGuard tracks the highest token seen per resource.
type MemoryFencingTokenGuard struct {
	mu     sync.Mutex
	tokens map[string]int64
}

// NewMemoryFencingTokenGuard creates an in-memory token guard.
func NewMemoryFencingTokenGuard() *MemoryFencingTokenGuard {
	return &MemoryFencingTokenGuard{
		tokens: make(map[string]int64),
	}
}

// CheckAndAdvance rejects tokens that are not greater than the stored token.
func (g *MemoryFencingTokenGuard) CheckAndAdvance(ctx context.Context, resource string, token int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if resource == "" {
		return errors.New("resource is required")
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	if token <= g.tokens[resource] {
		return ErrStaleToken
	}
	g.tokens[resource] = token
	return nil
}

var _ FencingTokenGuard = (*MemoryFencingTokenGuard)(nil)
