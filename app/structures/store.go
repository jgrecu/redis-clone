package structures

import (
	"fmt"
	"strconv"
	"sync"
	"time"
)

// Store encapsulates the Redis key-value store with thread-safe access.
type Store struct {
	data RedisDB
	mu   sync.RWMutex
}

// NewStore creates a new empty Store.
func NewStore() *Store {
	return &Store{
		data: make(RedisDB),
	}
}

// Get retrieves a string value by key, handling lazy expiry.
func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	value, ok := s.data[key]
	s.mu.RUnlock()

	if !ok {
		return "", false
	}

	if !value.Expiry.IsZero() && value.Expiry.Before(time.Now()) {
		s.mu.Lock()
		delete(s.data, key)
		s.mu.Unlock()
		return "", false
	}

	return value.String, true
}

// Set stores a string value with an optional expiry time.
func (s *Store) Set(key, value string, expiry time.Time) {
	s.mu.Lock()
	s.data[key] = MapValue{
		Typ:    "string",
		String: value,
		Expiry: expiry,
	}
	s.mu.Unlock()
}

// Delete removes a key from the store.
func (s *Store) Delete(key string) {
	s.mu.Lock()
	delete(s.data, key)
	s.mu.Unlock()
}

// Keys returns all key names in the store.
func (s *Store) Keys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	return keys
}

// Type returns the type name for a key ("string", "stream", or "none").
func (s *Store) Type(key string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	value, ok := s.data[key]
	if !ok {
		return "none"
	}
	return value.Typ
}

// Incr atomically increments a numeric string value by 1.
// If the key doesn't exist, it is created with value "1".
func (s *Store) Incr(key string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	item, ok := s.data[key]
	if !ok {
		s.data[key] = MapValue{
			Typ:    "string",
			String: "1",
		}
		return 1, nil
	}

	intValue, err := strconv.Atoi(item.String)
	if err != nil {
		return 0, err
	}

	intValue++
	item.String = strconv.Itoa(intValue)
	s.data[key] = item

	return intValue, nil
}

// LoadKeys replaces the entire store contents (used for RDB loading).
func (s *Store) LoadKeys(db RedisDB) {
	s.mu.Lock()
	s.data = db
	s.mu.Unlock()
}

// XAdd adds an entry to a stream, creating the stream if needed.
func (s *Store) XAdd(streamKey, entryKey string, pairs map[string]string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	val, ok := s.data[streamKey]
	if !ok {
		val = MapValue{
			Typ:    "stream",
			Stream: NewStream(),
		}
	}

	key, err := val.Stream.Add(entryKey, pairs)
	if err != nil {
		return "", err
	}

	s.data[streamKey] = val
	return key, nil
}

// XRange returns entries from a stream between start and end IDs.
func (s *Store) XRange(streamKey, start, end string) ([]Entry, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	val, ok := s.data[streamKey]
	if !ok || val.Typ != "stream" {
		return nil, false
	}

	return val.Stream.Range(start, end), true
}

// XRead returns entries from multiple streams after the given IDs.
func (s *Store) XRead(streamKeys, ids []string) map[string][]Entry {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make(map[string][]Entry)
	for i, key := range streamKeys {
		val, ok := s.data[key]
		if !ok || val.Typ != "stream" {
			continue
		}
		entries := val.Stream.Read(ids[i])
		if len(entries) > 0 {
			result[key] = entries
		}
	}
	return result
}

// StreamSize returns the total number of entries across the given streams.
func (s *Store) StreamSize(streamKeys []string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	size := 0
	for _, key := range streamKeys {
		val, ok := s.data[key]
		if !ok || val.Typ != "stream" {
			continue
		}
		size += val.Stream.Len()
	}
	return size
}

// LastStreamID returns the last entry ID for a stream, or "0-0" if not found.
func (s *Store) LastStreamID(key string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	val, ok := s.data[key]
	if !ok || val.Typ != "stream" {
		return "0-0"
	}

	lastTs := val.Stream.LastTimestamp()
	lastSeq := val.Stream.LastSeq(lastTs)
	return fmt.Sprintf("%d-%d", lastTs, lastSeq)
}
