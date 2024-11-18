package config

import (
	"fmt"
	"path/filepath"
	"sync"
	"time"
)

// Config represents server configuration
type Config struct {
	mu sync.RWMutex
	// Server configs
	Address         string
	CleanupInterval time.Duration

	// RDB configs
	Dir          string        // The directory where RDB files are stored
	DbFilename   string        // The filename of the RDB file
	SaveInterval time.Duration // How often to save the RDB file
}

// NewConfig creates a new configuration with default values
func NewConfig() *Config {
	return &Config{
		Address:         "0.0.0.0:6379",
		CleanupInterval: time.Minute,
		Dir:             ".",
		DbFilename:      "dump.rdb",
		SaveInterval:    time.Minute * 15,
	}
}

// Get returns the value of a configuration parameter
func (c *Config) Get(param string) (string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	switch param {
	case "dir":
		return c.Dir, nil
	case "dbfilename":
		return c.DbFilename, nil
	case "save":
		return fmt.Sprintf("%d", c.SaveInterval/time.Second), nil
	default:
		return "", fmt.Errorf("unknown config parameter: %s", param)
	}
}

// GetRDBPath returns the full path to the RDB file
func (c *Config) GetRDBPath() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return filepath.Join(c.Dir, c.DbFilename)
}

// Set updates the value of a configuration parameter
func (c *Config) Set(param, value string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch param {
	case "dir":
		c.Dir = value
		return nil
	case "dbfilename":
		c.DbFilename = value
		return nil
	case "save":
		duration, err := time.ParseDuration(value + "s")
		if err != nil {
			return fmt.Errorf("invalid save interval: %w", err)
		}
		c.SaveInterval = duration
		return nil
	default:
		return fmt.Errorf("unknown config parameter: %s", param)
	}
}
