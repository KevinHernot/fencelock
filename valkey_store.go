package fencelock

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/valkey-io/valkey-go"
)

// ValkeyStore implements Store with Valkey or Redis-compatible servers.
type ValkeyStore struct {
	client valkey.Client
}

// NewValkeyStore creates a Valkey-backed lock store.
func NewValkeyStore(client valkey.Client) *ValkeyStore {
	return &ValkeyStore{client: client}
}

// NextToken increments and returns the named fencing token counter.
func (s *ValkeyStore) NextToken(ctx context.Context, counterKey string) (int64, error) {
	if s == nil || s.client == nil {
		return 0, errors.New("nil Valkey client")
	}
	if counterKey == "" {
		counterKey = defaultTokenCounterKey
	}
	return s.client.Do(ctx, s.client.B().Incr().Key(counterKey).Build()).AsInt64()
}

// TrySet stores a value with NX and TTL semantics.
func (s *ValkeyStore) TrySet(ctx context.Context, key string, value string, ttl time.Duration) (bool, error) {
	if s == nil || s.client == nil {
		return false, errors.New("nil Valkey client")
	}
	if ttl <= 0 {
		return false, errors.New("lock ttl must be positive")
	}

	result, err := s.client.Do(ctx, s.client.B().Set().Key(key).Value(value).Nx().Ex(ttl).Build()).ToString()
	if valkey.IsValkeyNil(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return result == "OK", nil
}

// CompareAndDelete deletes a lock only when the stored value matches.
func (s *ValkeyStore) CompareAndDelete(ctx context.Context, key string, value string) (bool, error) {
	if s == nil || s.client == nil {
		return false, errors.New("nil Valkey client")
	}

	const script = `
		if redis.call("GET", KEYS[1]) == ARGV[1] then
			return redis.call("DEL", KEYS[1])
		end
		return 0
	`

	result, err := s.client.Do(ctx, s.client.B().Eval().Script(script).Numkeys(1).Key(key).Arg(value).Build()).AsInt64()
	if err != nil {
		return false, err
	}
	return result == 1, nil
}

// CompareAndRefresh extends a lock only when the stored value matches.
func (s *ValkeyStore) CompareAndRefresh(ctx context.Context, key string, value string, ttl time.Duration) (bool, error) {
	if s == nil || s.client == nil {
		return false, errors.New("nil Valkey client")
	}
	if ttl <= 0 {
		return false, errors.New("lock ttl must be positive")
	}

	const script = `
		if redis.call("GET", KEYS[1]) == ARGV[1] then
			return redis.call("PEXPIRE", KEYS[1], ARGV[2])
		end
		return 0
	`

	ttlMilliseconds := ttl.Milliseconds()
	if ttlMilliseconds <= 0 {
		ttlMilliseconds = 1
	}
	ttlMillis := strconv.FormatInt(ttlMilliseconds, 10)
	result, err := s.client.Do(ctx, s.client.B().Eval().Script(script).Numkeys(1).Key(key).Arg(value).Arg(ttlMillis).Build()).AsInt64()
	if err != nil {
		return false, err
	}
	return result == 1, nil
}

var _ Store = (*ValkeyStore)(nil)
