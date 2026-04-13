package fencelock

import (
	"context"
	"errors"
	"sync"
	"time"
)

type memoryEntry struct {
	value     string
	expiresAt time.Time
}

// MemoryStore is a process-local Store implementation for tests and examples.
type MemoryStore struct {
	mu       sync.Mutex
	entries  map[string]memoryEntry
	counters map[string]int64
	now      func() time.Time
}

// NewMemoryStore creates an in-memory lock store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		entries:  make(map[string]memoryEntry),
		counters: make(map[string]int64),
		now:      time.Now,
	}
}

// NextToken increments and returns the named fencing token counter.
func (s *MemoryStore) NextToken(ctx context.Context, counterKey string) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if counterKey == "" {
		counterKey = defaultTokenCounterKey
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.counters[counterKey]++
	return s.counters[counterKey], nil
}

// TrySet stores a value when the key is absent or expired.
func (s *MemoryStore) TrySet(ctx context.Context, key string, value string, ttl time.Duration) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if key == "" {
		return false, errors.New("lock key is required")
	}
	if ttl <= 0 {
		return false, errors.New("lock ttl must be positive")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.pruneExpiredLocked(key)
	if _, exists := s.entries[key]; exists {
		return false, nil
	}

	s.entries[key] = memoryEntry{
		value:     value,
		expiresAt: s.now().UTC().Add(ttl),
	}
	return true, nil
}

// CompareAndDelete deletes a lock only when the stored value matches.
func (s *MemoryStore) CompareAndDelete(ctx context.Context, key string, value string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.pruneExpiredLocked(key)
	entry, exists := s.entries[key]
	if !exists || entry.value != value {
		return false, nil
	}

	delete(s.entries, key)
	return true, nil
}

// CompareAndRefresh extends a lock only when the stored value matches.
func (s *MemoryStore) CompareAndRefresh(ctx context.Context, key string, value string, ttl time.Duration) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if ttl <= 0 {
		return false, errors.New("lock ttl must be positive")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.pruneExpiredLocked(key)
	entry, exists := s.entries[key]
	if !exists || entry.value != value {
		return false, nil
	}

	entry.expiresAt = s.now().UTC().Add(ttl)
	s.entries[key] = entry
	return true, nil
}

func (s *MemoryStore) pruneExpiredLocked(key string) {
	entry, exists := s.entries[key]
	if !exists {
		return
	}
	if !entry.expiresAt.After(s.now().UTC()) {
		delete(s.entries, key)
	}
}

var _ Store = (*MemoryStore)(nil)
