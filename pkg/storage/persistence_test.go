package storage

import (
	"testing"
	"time"
)

func TestNewStore(t *testing.T) {
	store := NewStore(time.Second)
	if store == nil {
		t.Error("NewStore returned nil")
	}
	if store.data == nil {
		t.Error("Store data map not initialized")
	}
	if store.janitor == nil {
		t.Error("Store janitor not initialized")
	}
	store.Close()
}

func TestStore_SetGet(t *testing.T) {
	store := NewStore(time.Second)
	defer store.Close()

	tests := []struct {
		name       string
		key        string
		value      string
		expiration time.Duration
		wait       time.Duration
		wantValue  string
		wantExists bool
	}{
		{
			name:       "Set and get without expiration",
			key:        "key1",
			value:      "value1",
			expiration: 0,
			wait:       0,
			wantValue:  "value1",
			wantExists: true,
		},
		{
			name:       "Set and get with expiration not reached",
			key:        "key2",
			value:      "value2",
			expiration: time.Second * 2,
			wait:       time.Second,
			wantValue:  "value2",
			wantExists: true,
		},
		{
			name:       "Set and get with expiration reached",
			key:        "key3",
			value:      "value3",
			expiration: time.Millisecond * 100,
			wait:       time.Millisecond * 200,
			wantValue:  "",
			wantExists: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store.Set(tt.key, tt.value, tt.expiration)
			if tt.wait > 0 {
				time.Sleep(tt.wait)
			}
			got, exists := store.Get(tt.key)
			if exists != tt.wantExists {
				t.Errorf("Store.Get() exists = %v, want %v", exists, tt.wantExists)
			}
			if got != tt.wantValue {
				t.Errorf("Store.Get() = %v, want %v", got, tt.wantValue)
			}
		})
	}
}

func TestStore_Delete(t *testing.T) {
	store := NewStore(time.Second)
	defer store.Close()

	// Set a value
	store.Set("key1", "value1", 0)

	// Delete it
	store.Delete("key1")

	// Try to get it
	_, exists := store.Get("key1")
	if exists {
		t.Error("Key still exists after deletion")
	}

	// Delete non-existent key (should not panic)
	store.Delete("nonexistent")
}

func TestStore_Cleanup(t *testing.T) {
	store := NewStore(time.Millisecond * 100)
	defer store.Close()

	// Set some values with different expirations
	store.Set("key1", "value1", time.Millisecond*50)  // Should expire quickly
	store.Set("key2", "value2", time.Millisecond*500) // Should not expire yet
	store.Set("key3", "value3", 0)                    // Should never expire

	// Wait for cleanup to run
	time.Sleep(time.Millisecond * 200)

	tests := []struct {
		key        string
		wantExists bool
	}{
		{"key1", false}, // Should be cleaned up
		{"key2", true},  // Should still exist
		{"key3", true},  // Should still exist
	}

	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			_, exists := store.Get(tt.key)
			if exists != tt.wantExists {
				t.Errorf("Store.Get(%s) exists = %v, want %v", tt.key, exists, tt.wantExists)
			}
		})
	}
}

func TestStore_Keys(t *testing.T) {
	store := NewStore(time.Second)
	defer store.Close()

	// Set some test data
	testData := map[string]string{
		"user:1":     "value1",
		"user:2":     "value2",
		"product:1":  "product1",
		"product:2":  "product2",
		"settings:1": "setting1",
	}

	for k, v := range testData {
		store.Set(k, v, 0)
	}

	tests := []struct {
		name    string
		pattern string
		want    int // number of expected matches
	}{
		{"Match all", "*", 5},
		{"Match users", "user:*", 2},
		{"Match products", "product:*", 2},
		{"Match settings", "settings:*", 1},
		{"Match none", "nonexistent:*", 0},
		{"Match specific", "user:1", 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := store.Keys(tt.pattern)
			if len(got) != tt.want {
				t.Errorf("Store.Keys() returned %d keys, want %d", len(got), tt.want)
			}
		})
	}
}

func TestStore_DumpRestore(t *testing.T) {
	store := NewStore(time.Second)
	defer store.Close()

	// Set some test data
	store.Set("key1", "value1", 0)
	store.Set("key2", "value2", time.Hour) // With expiration

	// Dump the data
	dump := store.Dump()

	// Create a new store and restore the data
	store2 := NewStore(time.Second)
	defer store2.Close()
	store2.Restore(dump)

	// Verify the restored data
	tests := []struct {
		key   string
		want  string
		exist bool
	}{
		{"key1", "value1", true},
		{"key2", "value2", true},
		{"key3", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			got, exists := store2.Get(tt.key)
			if exists != tt.exist {
				t.Errorf("Store.Get() exists = %v, want %v", exists, tt.exist)
			}
			if exists && got != tt.want {
				t.Errorf("Store.Get() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStore_Clear(t *testing.T) {
	store := NewStore(time.Second)
	defer store.Close()

	// Set some test data
	store.Set("key1", "value1", 0)
	store.Set("key2", "value2", 0)

	// Clear the store
	store.Clear()

	// Verify all data is gone
	if len(store.Dump()) != 0 {
		t.Error("Store not empty after Clear()")
	}

	// Verify individual keys
	for _, key := range []string{"key1", "key2"} {
		if _, exists := store.Get(key); exists {
			t.Errorf("Key %s still exists after Clear()", key)
		}
	}
}
