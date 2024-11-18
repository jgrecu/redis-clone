package rdb

import (
	"encoding/gob"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/jgrecu/redis-clone/pkg/config"
	"github.com/jgrecu/redis-clone/pkg/storage"
)

// RDB handles Redis database persistence
type RDB struct {
	config *config.Config
	store  *storage.Store
	mu     sync.Mutex
}

// NewRDB creates a new RDB handler
func NewRDB(config *config.Config, store *storage.Store) *RDB {
	rdb := &RDB{
		config: config,
		store:  store,
	}

	// Start background save routine
	go rdb.backgroundSave()

	return rdb
}

// backgroundSave periodically saves the database
func (r *RDB) backgroundSave() {
	for {
		time.Sleep(r.config.SaveInterval)
		if err := r.Save(); err != nil {
			fmt.Printf("Error saving RDB: %v\n", err)
		}
	}
}

// Save saves the current database state to disk
func (r *RDB) Save() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	rdbPath := r.config.GetRDBPath()
	tempPath := rdbPath + ".temp"

	// Ensure directory exists
	if err := os.MkdirAll(filepath.Dir(rdbPath), 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// Create temporary file
	file, err := os.Create(tempPath)
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}

	// Encode store data
	encoder := gob.NewEncoder(file)
	if err := encoder.Encode(r.store.Dump()); err != nil {
		file.Close()
		os.Remove(tempPath)
		return fmt.Errorf("failed to encode data: %w", err)
	}

	if err := file.Close(); err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("failed to close temp file: %w", err)
	}

	// Atomically rename temp file to target file
	if err := os.Rename(tempPath, rdbPath); err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("failed to rename temp file: %w", err)
	}

	return nil
}

// Load loads the database state from disk
func (r *RDB) Load() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	rdbPath := r.config.GetRDBPath()
	file, err := os.Open(rdbPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // No existing RDB file is not an error
		}
		return fmt.Errorf("failed to open RDB file: %w", err)
	}
	defer file.Close()

	decoder := gob.NewDecoder(file)
	data := make(map[string]storage.Item)
	if err := decoder.Decode(&data); err != nil {
		return fmt.Errorf("failed to decode RDB data: %w", err)
	}

	r.store.Restore(data)
	return nil
}
