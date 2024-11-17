package storage

import (
	"sync"
	"time"
)

// Item represents a value in the store with optional expiration
type Item struct {
	Value     string
	ExpiresAt time.Time
	HasExpiry bool
}

// Store represents a thread-safe key-value store
type Store struct {
	mu      sync.RWMutex
	data    map[string]Item
	janitor *time.Ticker
}

// NewStore creates a new key-value store with optional cleanup interval
func NewStore(cleanupInterval time.Duration) *Store {
	store := &Store{
		data:    make(map[string]Item), // Initialize the map
		janitor: time.NewTicker(cleanupInterval),
	}

	// Start cleanup goroutine
	go store.cleanup()

	return store
}

// Set stores a key-value pair with optional expiration
func (s *Store) Set(key string, value string, expiration time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	item := Item{
		Value:     value,
		HasExpiry: expiration > 0,
	}

	if item.HasExpiry {
		item.ExpiresAt = time.Now().Add(expiration)
	}

	s.data[key] = item
}

// Get retrieves a value by key
func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	item, exists := s.data[key]
	if !exists {
		return "", false
	}

	// Check if item has expired
	if item.HasExpiry && time.Now().After(item.ExpiresAt) {
		return "", false
	}

	return item.Value, true
}

// Delete removes a key from the store
func (s *Store) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key)
}

// cleanup periodically removes expired items
func (s *Store) cleanup() {
	for range s.janitor.C {
		s.mu.Lock()
		now := time.Now()

		for key, item := range s.data {
			if item.HasExpiry && now.After(item.ExpiresAt) {
				delete(s.data, key)
			}
		}

		s.mu.Unlock()
	}
}

// Close stops the cleanup goroutine
func (s *Store) Close() {
	s.janitor.Stop()
}
