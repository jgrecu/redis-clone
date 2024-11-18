package rdb

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jgrecu/redis-clone/pkg/config"
	"github.com/jgrecu/redis-clone/pkg/storage"
)

func TestRDB_SaveAndLoadWithCustomDir(t *testing.T) {
	// Create temporary directory
	tempDir, err := os.MkdirTemp("", "redis-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create config and store
	cfg := config.NewConfig()
	cfg.Dir = tempDir
	cfg.DbFilename = "test.rdb"
	store := storage.NewStore(time.Minute)

	// Create test data
	store.Set("key1", "value1", 0)
	store.Set("key2", "value2", time.Hour)

	// Create RDB handler
	rdb := NewRDB(cfg, store)

	// Test Save
	if err := rdb.Save(); err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	// Verify file exists
	rdbPath := filepath.Join(tempDir, "test.rdb")
	if _, err := os.Stat(rdbPath); os.IsNotExist(err) {
		t.Errorf("RDB file was not created at %s", rdbPath)
	}

	// Clear store
	//storage.

	// Test Load
	if err := rdb.Load(); err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	// Verify data
	if val, ok := store.Get("key1"); !ok || val != "value1" {
		t.Errorf("Load() failed to restore key1")
	}
}
