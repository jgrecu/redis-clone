package rdb

import (
	"fmt"
	"os"
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

	database := []Database{{Index: 0, Keys: r.store.Dump()}}
	parser := NewRDBParser(rdbPath)
	err := parser.SaveRDB(database)

	if err != nil {
		return fmt.Errorf("failed to save file: %w", err)
	}

	return nil
}

// Load loads the database state from disk
func (r *RDB) Load() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	rdbPath := r.config.GetRDBPath()
	parser := NewRDBParser(rdbPath)

	db, err := parser.ParseRDB()
	if err != nil {
		if os.IsNotExist(err) {
			return nil // No existing RDB file is not an error
		}
		return fmt.Errorf("failed to open RDB file: %w", err)
	}

	if len(db) > 0 {
		data := db[0].Keys
		r.store.Restore(data)
	}

	// return fmt.Errorf("failed to decode RDB data: %w", err)
	return nil
}
