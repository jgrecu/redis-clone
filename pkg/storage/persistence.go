package storage

import (
	"path/filepath"
	"sync"
	"time"
)

type ValueType uint8

const (
	String ValueType = iota
	List
	Set
	ZSet
	Hash
)

// Item represents a value in the store with optional expiration
type Item struct {
	Value  string
	Expire *time.Time
	Type   ValueType
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
		Value: value,
	}

	if expiration > 0 {
		expireTime := time.Now().Add(expiration)
		item.Expire = &expireTime
	}

	s.data[key] = item
}

// Add stores a key-value pair to the map, key str, value Item
func (s *Store) add(key string, value *Item) {
	s.data[key] = *value
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
	if item.Expire != nil && time.Now().After(*item.Expire) {
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
			if item.Expire != nil && now.After(*item.Expire) {
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

// Dump returns a copy of the current store data
func (s *Store) Dump() map[string]Item {
	s.mu.Lock()
	defer s.mu.Unlock()

	dump := make(map[string]Item, len(s.data))
	for k, v := range s.data {
		dump[k] = v
	}
	return dump
}

// Restore replaces the current store data with the provided data
func (s *Store) Restore(data map[string]Item) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for k, v := range data {
		s.add(k, &v)
	}
}

func (s *Store) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	clear(s.data)
}

// Keys return the list of current keys in store data matching the pattern
func (s *Store) Keys(pattern string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	if pattern == "*" {
		keys := make([]string, 0, len(s.data))
		for k := range s.data {
			keys = append(keys, k)
		}
		return keys
	}

	var keys []string
	for k := range s.data {
		if matched, _ := filepath.Match(pattern, k); matched {
			keys = append(keys, k)
		}
	}

	return keys
}
