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

func TestRDB_ConcurrentOperations(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "rdb-concurrent-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	cfg := config.NewConfig()
	cfg.Dir = tempDir
	cfg.DbFilename = "concurrent.rdb"
	cfg.SaveInterval = time.Millisecond * 100

	store := storage.NewStore(time.Hour)
	rdb := NewRDB(cfg, store)

	// Start multiple goroutines performing operations
	var wg sync.WaitGroup
	numWorkers := 5
	wg.Add(numWorkers * 2) // Writers and readers

	// Channel to collect errors from goroutines
	errChan := make(chan error, numWorkers*2*10) // Buffer for all possible errors

	// Writers
	for i := 0; i < numWorkers; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				key := fmt.Sprintf("key%d-%d", id, j)
				store.Set(key, fmt.Sprintf("value%d-%d", id, j), time.Hour)
				if err := rdb.Save(); err != nil {
					errChan <- fmt.Errorf("Save error in writer %d: %v", id, err)
					return
				}
				time.Sleep(time.Millisecond * 10)
			}
		}(i)
	}

	// Readers
	for i := 0; i < numWorkers; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				if err := rdb.Load(); err != nil {
					errChan <- fmt.Errorf("Load error in reader %d: %v", id, err)
					return
				}
				time.Sleep(time.Millisecond * 10)
			}
		}(i)
	}

	// Wait for all goroutines to complete
	wg.Wait()
	close(errChan)

	// Check for any errors
	for err := range errChan {
		t.Error(err)
	}
}

func TestRDB_LargeDataset(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "rdb-large-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	cfg := config.NewConfig()
	cfg.Dir = tempDir
	cfg.DbFilename = "large.rdb"

	store := storage.NewStore(time.Hour)
	rdb := NewRDB(cfg, store)

	// Create and store test data
	numEntries := 10000
	for i := 0; i < numEntries; i++ {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("value%d", i)
		store.Set(key, value, time.Hour)
	}

	// Save and load data
	if err := rdb.Save(); err != nil {
		t.Fatalf("Failed to save: %v", err)
	}

	newStore := storage.NewStore(time.Hour)
	newRDB := NewRDB(cfg, newStore)

	if err := newRDB.Load(); err != nil {
		t.Fatalf("Failed to load: %v", err)
	}

	// Verify data
	for i := 0; i < numEntries; i++ {
		key := fmt.Sprintf("key%d", i)
		expectedValue := fmt.Sprintf("value%d", i)
		if value, exists := newStore.Get(key); !exists || value != expectedValue {
			t.Errorf("Data mismatch for key %s", key)
		}
	}
}

func TestRDB_ErrorConditions(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "rdb-error-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	tests := []struct {
		name      string
		setup     func(*config.Config, *storage.Store) *RDB
		operation string // "save" or "load"
		wantErr   bool
	}{
		{
			name: "invalid directory",
			setup: func(cfg *config.Config, store *storage.Store) *RDB {
				cfg.Dir = "/nonexistent/directory"
				return NewRDB(cfg, store)
			},
			operation: "save",
			wantErr:   true,
		},
		{
			name: "corrupted file",
			setup: func(cfg *config.Config, store *storage.Store) *RDB {
				rdb := NewRDB(cfg, store)
				// Create corrupted RDB file with invalid header version
				corruptedData := append([]byte("REDIS9999"), []byte("corrupted data")...)
				err := os.WriteFile(filepath.Join(cfg.Dir, cfg.DbFilename), corruptedData, 0644)
				if err != nil {
					t.Fatalf("Failed to create corrupted file: %v", err)
				}
				return rdb
			},
			operation: "load",
			wantErr:   true,
		},
		{
			name: "permission denied",
			setup: func(cfg *config.Config, store *storage.Store) *RDB {
				// Create read-only directory
				readOnlyDir := filepath.Join(tempDir, "readonly")
				if err := os.Mkdir(readOnlyDir, 0500); err != nil {
					t.Fatalf("Failed to create read-only dir: %v", err)
				}
				cfg.Dir = readOnlyDir
				return NewRDB(cfg, store)
			},
			operation: "save",
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := config.NewConfig()
			cfg.Dir = tempDir
			cfg.DbFilename = "error.rdb"
			store := storage.NewStore(time.Hour)

			rdb := tt.setup(cfg, store)
			var err error
			if tt.operation == "save" {
				err = rdb.Save()
			} else {
				err = rdb.Load()
			}
			if (err != nil) != tt.wantErr {
				t.Errorf("%s() error = %v, wantErr %v", tt.operation, err, tt.wantErr)
			}
		})
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
