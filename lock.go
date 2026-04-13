// Package fencelock provides distributed lock primitives with fencing tokens.
package fencelock

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync/atomic"
	"time"
)

const (
	defaultTTL             = 10 * time.Second
	defaultRetryDelay      = 100 * time.Millisecond
	defaultMaxRetries      = 3
	defaultAcquireTimeout  = 5 * time.Second
	defaultTokenCounterKey = "fencelock:tokens"
)

var (
	// ErrLockHeld indicates another owner currently holds the lock.
	ErrLockHeld = errors.New("lock already held")
	// ErrLockNotHeld indicates the lock was already released.
	ErrLockNotHeld = errors.New("lock not held")
	// ErrStaleLock indicates the lock expired or another owner replaced it.
	ErrStaleLock = errors.New("stale lock")
)

// Store is the minimal backend required for fenced locks.
type Store interface {
	NextToken(ctx context.Context, counterKey string) (int64, error)
	TrySet(ctx context.Context, key string, value string, ttl time.Duration) (bool, error)
	CompareAndDelete(ctx context.Context, key string, value string) (bool, error)
	CompareAndRefresh(ctx context.Context, key string, value string, ttl time.Duration) (bool, error)
}

// Options configures lock acquisition.
type Options struct {
	TTL             time.Duration
	RetryDelay      time.Duration
	MaxRetries      int
	AcquireTimeout  time.Duration
	TokenCounterKey string
}

// Option customizes lock acquisition.
type Option func(*Options)

// DefaultOptions returns conservative defaults for short critical sections.
func DefaultOptions() Options {
	return Options{
		TTL:             defaultTTL,
		RetryDelay:      defaultRetryDelay,
		MaxRetries:      defaultMaxRetries,
		AcquireTimeout:  defaultAcquireTimeout,
		TokenCounterKey: defaultTokenCounterKey,
	}
}

// WithTTL sets the lock expiration.
func WithTTL(ttl time.Duration) Option {
	return func(options *Options) {
		options.TTL = ttl
	}
}

// WithRetryDelay sets the delay between acquisition attempts.
func WithRetryDelay(delay time.Duration) Option {
	return func(options *Options) {
		options.RetryDelay = delay
	}
}

// WithMaxRetries sets the number of retry attempts after the first attempt.
func WithMaxRetries(maxRetries int) Option {
	return func(options *Options) {
		options.MaxRetries = maxRetries
	}
}

// WithAcquireTimeout sets the maximum total time spent acquiring the lock.
func WithAcquireTimeout(timeout time.Duration) Option {
	return func(options *Options) {
		options.AcquireTimeout = timeout
	}
}

// WithTokenCounterKey sets the backend key used for fencing token generation.
func WithTokenCounterKey(key string) Option {
	return func(options *Options) {
		options.TokenCounterKey = key
	}
}

// Manager acquires locks using a Store.
type Manager struct {
	store Store
}

// NewManager creates a lock manager.
func NewManager(store Store) *Manager {
	return &Manager{store: store}
}

// Acquire attempts to acquire a lock and returns a fencing token on success.
func (m *Manager) Acquire(ctx context.Context, key string, options ...Option) (*Lock, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if m == nil || m.store == nil {
		return nil, errors.New("nil lock store")
	}
	if key == "" {
		return nil, errors.New("lock key is required")
	}

	opts := DefaultOptions()
	for _, option := range options {
		if option != nil {
			option(&opts)
		}
	}
	if opts.TTL <= 0 {
		return nil, errors.New("lock ttl must be positive")
	}
	if opts.RetryDelay < 0 {
		opts.RetryDelay = 0
	}
	if opts.MaxRetries < 0 {
		opts.MaxRetries = 0
	}
	if opts.TokenCounterKey == "" {
		opts.TokenCounterKey = defaultTokenCounterKey
	}

	acquireCtx := ctx
	cancel := func() {}
	if opts.AcquireTimeout > 0 {
		acquireCtx, cancel = context.WithTimeout(ctx, opts.AcquireTimeout)
	}
	defer cancel()

	ownerID, err := randomOwnerID()
	if err != nil {
		return nil, fmt.Errorf("generate lock owner id: %w", err)
	}

	for attempt := 0; attempt <= opts.MaxRetries; attempt++ {
		if attempt > 0 && opts.RetryDelay > 0 {
			timer := time.NewTimer(opts.RetryDelay)
			select {
			case <-acquireCtx.Done():
				timer.Stop()
				return nil, acquireCtx.Err()
			case <-timer.C:
			}
		}

		token, err := m.store.NextToken(acquireCtx, opts.TokenCounterKey)
		if err != nil {
			return nil, fmt.Errorf("next fencing token: %w", err)
		}

		value := formatLockValue(ownerID, token)
		acquired, err := m.store.TrySet(acquireCtx, key, value, opts.TTL)
		if err != nil {
			return nil, fmt.Errorf("try acquire lock: %w", err)
		}
		if acquired {
			return &Lock{
				store:        m.store,
				key:          key,
				value:        value,
				ownerID:      ownerID,
				ttl:          opts.TTL,
				fencingToken: token,
				acquiredAt:   time.Now().UTC(),
			}, nil
		}
	}

	return nil, ErrLockHeld
}

// WithLock acquires a lock, passes the fencing token to fn, and releases it.
func (m *Manager) WithLock(
	ctx context.Context,
	key string,
	fn func(context.Context, int64) error,
	options ...Option,
) error {
	if fn == nil {
		return errors.New("lock callback is required")
	}

	lock, err := m.Acquire(ctx, key, options...)
	if err != nil {
		return err
	}
	defer func() {
		_ = lock.Release(context.Background())
	}()

	tokenCtx := ContextWithFencingToken(ctx, lock.FencingToken())
	return fn(tokenCtx, lock.FencingToken())
}

// Lock represents a held lock.
type Lock struct {
	store        Store
	key          string
	value        string
	ownerID      string
	ttl          time.Duration
	fencingToken int64
	acquiredAt   time.Time
	released     atomic.Bool
}

// Key returns the lock key.
func (l *Lock) Key() string {
	return l.key
}

// Value returns the backend value used for compare-and-delete operations.
func (l *Lock) Value() string {
	return l.value
}

// OwnerID returns the random owner identifier embedded in the lock value.
func (l *Lock) OwnerID() string {
	return l.ownerID
}

// FencingToken returns the monotonically increasing token for this lock.
func (l *Lock) FencingToken() int64 {
	return l.fencingToken
}

// AcquiredAt returns when the lock was acquired.
func (l *Lock) AcquiredAt() time.Time {
	return l.acquiredAt
}

// Release releases the lock only if this owner still holds it.
func (l *Lock) Release(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if l == nil || l.store == nil {
		return ErrLockNotHeld
	}
	if !l.released.CompareAndSwap(false, true) {
		return ErrLockNotHeld
	}

	released, err := l.store.CompareAndDelete(ctx, l.key, l.value)
	if err != nil {
		l.released.Store(false)
		return err
	}
	if !released {
		return ErrStaleLock
	}
	return nil
}

// Refresh extends the lock TTL only if this owner still holds it.
func (l *Lock) Refresh(ctx context.Context, ttl ...time.Duration) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if l == nil || l.store == nil || l.released.Load() {
		return ErrLockNotHeld
	}

	nextTTL := l.ttl
	if len(ttl) > 0 && ttl[0] > 0 {
		nextTTL = ttl[0]
	}
	if nextTTL <= 0 {
		return errors.New("lock ttl must be positive")
	}

	refreshed, err := l.store.CompareAndRefresh(ctx, l.key, l.value, nextTTL)
	if err != nil {
		return err
	}
	if !refreshed {
		return ErrStaleLock
	}
	l.ttl = nextTTL
	return nil
}

func randomOwnerID() (string, error) {
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}

func formatLockValue(ownerID string, token int64) string {
	return fmt.Sprintf("%s:%d", ownerID, token)
}

type fencingTokenContextKey struct{}

// ContextWithFencingToken returns a context carrying the current fencing token.
func ContextWithFencingToken(ctx context.Context, token int64) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, fencingTokenContextKey{}, token)
}

// FencingTokenFromContext returns a fencing token previously added to ctx.
func FencingTokenFromContext(ctx context.Context) (int64, bool) {
	if ctx == nil {
		return 0, false
	}
	token, ok := ctx.Value(fencingTokenContextKey{}).(int64)
	return token, ok
}
