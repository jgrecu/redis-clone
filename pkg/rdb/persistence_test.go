package rdb

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/jgrecu/redis-clone/pkg/config"
	"github.com/jgrecu/redis-clone/pkg/storage"
)

func TestNewRDB(t *testing.T) {
	cfg := config.NewConfig()
	store := storage.NewStore(time.Second)
	rdb := NewRDB(cfg, store)

	if rdb == nil {
		t.Fatal("NewRDB returned nil")
	}

	if rdb.config != cfg {
		t.Error("NewRDB did not set config correctly")
	}
	if rdb.store != store {
		t.Error("NewRDB did not set store correctly")
	}
}

func TestRDB_SaveAndLoad(t *testing.T) {
	// Create a temporary directory for test files
	tempDir, err := os.MkdirTemp("", "rdb-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create config with temp directory
	cfg := config.NewConfig()
	cfg.Dir = tempDir
	cfg.DbFilename = "test.rdb"
	cfg.SaveInterval = time.Millisecond * 100 // Short interval for testing

	// Create store with test data
	store := storage.NewStore(time.Hour)
	store.Set("key1", "value1", 0)
	store.Set("key2", "value2", time.Hour)

	// Create RDB instance
	rdb := NewRDB(cfg, store)

	// Test Save
	err = rdb.Save()
	if err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	// Verify file exists
	rdbPath := filepath.Join(tempDir, "test.rdb")
	if _, err := os.Stat(rdbPath); os.IsNotExist(err) {
		t.Error("Save() did not create RDB file")
	}

	// Create new store and RDB instance for loading
	newStore := storage.NewStore(time.Hour)
	newRDB := NewRDB(cfg, newStore)

	// Test Load
	err = newRDB.Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	// Verify loaded data
	tests := []struct {
		key      string
		want     string
		wantBool bool
	}{
		{"key1", "value1", true},
		{"key2", "value2", true},
		{"nonexistent", "", false},
	}

	for _, tt := range tests {
		got, exists := newStore.Get(tt.key)
		if exists != tt.wantBool {
			t.Errorf("Store.Get(%s) exists = %v, want %v", tt.key, exists, tt.wantBool)
		}
		if exists && got != tt.want {
			t.Errorf("Store.Get(%s) = %v, want %v", tt.key, got, tt.want)
		}
	}
}

// func TestRDB_BackgroundSave(t *testing.T) {
// 	// Create a temporary directory for test files
// 	tempDir, err := os.MkdirTemp("", "rdb-test")
// 	if err != nil {
// 		t.Fatalf("Failed to create temp dir: %v", err)
// 	}
// 	defer os.RemoveAll(tempDir)

// 	// Create config with very short save interval
// 	cfg := config.NewConfig()
// 	cfg.Dir = tempDir
// 	cfg.DbFilename = "test.rdb"
// 	cfg.SaveInterval = 100 * time.Millisecond

// 	// Create store with test data
// 	store := storage.NewStore(time.Hour)
// 	store.Set("key1", "value1", 0)

// 	// Create RDB instance (this starts background save)
// 	_ = NewRDB(cfg, store) // Background save starts automatically

// 	// Wait for at least one background save
// 	time.Sleep(150 * time.Millisecond)

// 	// Verify file exists
// 	rdbPath := filepath.Join(tempDir, "test.rdb")
// 	if _, err := os.Stat(rdbPath); os.IsNotExist(err) {
// 		t.Error("Background save did not create RDB file")
// 	}

// 	// Add more data and wait for another save
// 	store.Set("key2", "value2", 0)
// 	time.Sleep(150 * time.Millisecond)

// 	// Load data into new store to verify background save worked
// 	newStore := storage.NewStore(time.Hour)
// 	newRDB := NewRDB(cfg, newStore)
// 	err = newRDB.Load()
// 	if err != nil {
// 		t.Fatalf("Load() error = %v", err)
// 	}

// 	// Verify both keys were saved
// 	expectedData := map[string]string{
// 		"key1": "value1",
// 		"key2": "value2",
// 	}
// 	for key, expectedValue := range expectedData {
// 		if val, exists := newStore.Get(key); !exists || val != expectedValue {
// 			t.Errorf("Background save did not persist key %s correctly, got %s, want %s", key, val, expectedValue)
// 		}
// 	}
// }

func TestRDB_ConcurrentAccess(t *testing.T) {
	// Create a temporary directory for test files
	tempDir, err := os.MkdirTemp("", "rdb-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create config
	cfg := config.NewConfig()
	cfg.Dir = tempDir
	cfg.DbFilename = "test.rdb"

	// Create store
	store := storage.NewStore(time.Hour)
	rdb := NewRDB(cfg, store)

	// Number of concurrent operations
	const numOps = 100
	var wg sync.WaitGroup
	wg.Add(3) // For save, load, and set goroutines

	// Concurrent saves
	go func() {
		defer wg.Done()
		for i := 0; i < numOps; i++ {
			err := rdb.Save()
			if err != nil {
				t.Errorf("Concurrent Save() error = %v", err)
			}
		}
	}()

	// Concurrent loads
	go func() {
		defer wg.Done()
		for i := 0; i < numOps; i++ {
			err := rdb.Load()
			if err != nil {
				t.Errorf("Concurrent Load() error = %v", err)
			}
		}
	}()

	// Concurrent data modifications
	go func() {
		defer wg.Done()
		for i := 0; i < numOps; i++ {
			key := fmt.Sprintf("key%d", i)
			store.Set(key, fmt.Sprintf("value%d", i), 0)
			time.Sleep(time.Millisecond) // Small delay to ensure some overlap
		}
	}()

	wg.Wait()
}

func TestRDB_LoadNonExistent(t *testing.T) {
	// Create a temporary directory for test files
	tempDir, err := os.MkdirTemp("", "rdb-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create parent directory if it doesn't exist
	err = os.MkdirAll(tempDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create parent directory: %v", err)
	}

	// Create config with non-existent RDB file
	cfg := config.NewConfig()
	cfg.Dir = tempDir
	cfg.DbFilename = "nonexistent.rdb"

	store := storage.NewStore(time.Hour)
	rdb := NewRDB(cfg, store)

	// Test Load with non-existent file
	err = rdb.Load()
	if err != nil {
		t.Errorf("Load() error = %v, want nil for non-existent file", err)
	}
}

func TestRDB_LoadCorrupt(t *testing.T) {
	// Create a temporary directory for test files
	tempDir, err := os.MkdirTemp("", "rdb-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create parent directory if it doesn't exist
	err = os.MkdirAll(tempDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create parent directory: %v", err)
	}

	// Create config
	cfg := config.NewConfig()
	cfg.Dir = tempDir
	cfg.DbFilename = "corrupt.rdb"

	// Create corrupt RDB file
	rdbPath := filepath.Join(tempDir, "corrupt.rdb")
	err = os.WriteFile(rdbPath, []byte("corrupt data"), 0644)
	if err != nil {
		t.Fatalf("Failed to create corrupt file: %v", err)
	}

	store := storage.NewStore(time.Hour)
	rdb := NewRDB(cfg, store)

	// Test Load with corrupt file
	err = rdb.Load()
	if err == nil {
		t.Error("Load() error = nil, want error for corrupt file")
	}
}

func TestRDB_SaveError(t *testing.T) {
	// Create config with invalid directory
	cfg := config.NewConfig()
	cfg.Dir = "/nonexistent/directory"
	cfg.DbFilename = "test.rdb"

	store := storage.NewStore(time.Hour)
	rdb := NewRDB(cfg, store)

	// Test Save with invalid directory
	err := rdb.Save()
	if err == nil {
		t.Error("Save() error = nil, want error for invalid directory")
	}
}
