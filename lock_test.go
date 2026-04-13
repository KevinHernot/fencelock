package fencelock

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestAcquireReturnsIncreasingFencingTokens(t *testing.T) {
	ctx := context.Background()
	manager := NewManager(NewMemoryStore())

	first, err := manager.Acquire(ctx, "orders:order-1", WithMaxRetries(0))
	if err != nil {
		t.Fatalf("Acquire first failed: %v", err)
	}
	if first.FencingToken() != 1 {
		t.Fatalf("expected first token to be 1, got %d", first.FencingToken())
	}
	if err := first.Release(ctx); err != nil {
		t.Fatalf("Release first failed: %v", err)
	}

	second, err := manager.Acquire(ctx, "orders:order-1", WithMaxRetries(0))
	if err != nil {
		t.Fatalf("Acquire second failed: %v", err)
	}
	defer second.Release(ctx)

	if second.FencingToken() <= first.FencingToken() {
		t.Fatalf("expected increasing token, got first=%d second=%d", first.FencingToken(), second.FencingToken())
	}
}

func TestSecondAcquireFailsWhileLockHeld(t *testing.T) {
	ctx := context.Background()
	manager := NewManager(NewMemoryStore())

	lock, err := manager.Acquire(ctx, "orders:order-1", WithMaxRetries(0))
	if err != nil {
		t.Fatalf("Acquire failed: %v", err)
	}
	defer lock.Release(ctx)

	_, err = manager.Acquire(ctx, "orders:order-1", WithMaxRetries(0))
	if !errors.Is(err, ErrLockHeld) {
		t.Fatalf("expected ErrLockHeld, got %v", err)
	}
}

func TestStaleReleaseDoesNotDeleteNewOwner(t *testing.T) {
	ctx := context.Background()
	manager := NewManager(NewMemoryStore())

	first, err := manager.Acquire(ctx, "orders:order-1", WithTTL(20*time.Millisecond), WithMaxRetries(0))
	if err != nil {
		t.Fatalf("Acquire first failed: %v", err)
	}

	time.Sleep(30 * time.Millisecond)

	second, err := manager.Acquire(ctx, "orders:order-1", WithTTL(time.Second), WithMaxRetries(0))
	if err != nil {
		t.Fatalf("Acquire second failed: %v", err)
	}
	defer second.Release(ctx)

	if err := first.Release(ctx); !errors.Is(err, ErrStaleLock) {
		t.Fatalf("expected ErrStaleLock, got %v", err)
	}

	_, err = manager.Acquire(ctx, "orders:order-1", WithMaxRetries(0))
	if !errors.Is(err, ErrLockHeld) {
		t.Fatalf("expected new owner to keep the lock, got %v", err)
	}
}

func TestRefreshKeepsOwnership(t *testing.T) {
	ctx := context.Background()
	manager := NewManager(NewMemoryStore())

	lock, err := manager.Acquire(ctx, "orders:order-1", WithTTL(20*time.Millisecond), WithMaxRetries(0))
	if err != nil {
		t.Fatalf("Acquire failed: %v", err)
	}
	defer lock.Release(ctx)

	if err := lock.Refresh(ctx, 100*time.Millisecond); err != nil {
		t.Fatalf("Refresh failed: %v", err)
	}

	time.Sleep(40 * time.Millisecond)

	_, err = manager.Acquire(ctx, "orders:order-1", WithMaxRetries(0))
	if !errors.Is(err, ErrLockHeld) {
		t.Fatalf("expected refreshed lock to still be held, got %v", err)
	}
}

func TestWithLockAddsFencingTokenToContext(t *testing.T) {
	ctx := context.Background()
	manager := NewManager(NewMemoryStore())

	err := manager.WithLock(ctx, "orders:order-1", func(ctx context.Context, token int64) error {
		fromContext, ok := FencingTokenFromContext(ctx)
		if !ok {
			t.Fatal("expected fencing token in context")
		}
		if fromContext != token {
			t.Fatalf("context token %d != callback token %d", fromContext, token)
		}
		return nil
	}, WithMaxRetries(0))
	if err != nil {
		t.Fatalf("WithLock failed: %v", err)
	}
}

func TestMemoryFencingTokenGuardRejectsStaleTokens(t *testing.T) {
	ctx := context.Background()
	guard := NewMemoryFencingTokenGuard()

	if err := guard.CheckAndAdvance(ctx, "orders:order-1", 10); err != nil {
		t.Fatalf("CheckAndAdvance fresh token failed: %v", err)
	}
	if err := guard.CheckAndAdvance(ctx, "orders:order-1", 9); !errors.Is(err, ErrStaleToken) {
		t.Fatalf("expected ErrStaleToken for lower token, got %v", err)
	}
	if err := guard.CheckAndAdvance(ctx, "orders:order-1", 10); !errors.Is(err, ErrStaleToken) {
		t.Fatalf("expected ErrStaleToken for repeated token, got %v", err)
	}
	if err := guard.CheckAndAdvance(ctx, "orders:order-1", 11); err != nil {
		t.Fatalf("CheckAndAdvance newer token failed: %v", err)
	}
}
