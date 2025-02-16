package config

import (
	"fmt"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

// Config represents server configuration
type Config struct {
	mu             sync.RWMutex
	// Server configs
	Address         string
	Port            uint
	CleanupInterval time.Duration

	// Replication configs
	Role         string // "master" or "slave"
	MasterHost   string // Master's hostname
	MasterPort   uint   // Master's port

	// RDB configs
	Dir          string        // The directory where RDB files are stored
	DbFilename   string        // The filename of the RDB file
	SaveInterval time.Duration // How often to save the RDB file
}

// NewConfig creates a new configuration with default values
func NewConfig() *Config {
	return &Config{
		Address:         "0.0.0.0",
		Port:            6379,
		CleanupInterval: time.Minute,
		Role:            "master", // Default role is master
		MasterHost:      "",
		MasterPort:      0,
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
		return fmt.Sprintf("%ds", c.SaveInterval/time.Second), nil
	case "port":
		return strconv.FormatUint(uint64(c.Port), 10), nil
	case "role":
		return c.Role, nil
	case "masterhost":
		return c.MasterHost, nil
	case "masterport":
		return strconv.FormatUint(uint64(c.MasterPort), 10), nil
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
		seconds, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("invalid save interval: %w", err)
		}
		duration := time.Duration(seconds) * time.Second
		if duration < 0 {
			return fmt.Errorf("save interval must be positive")
		}
		c.SaveInterval = duration
		return nil
	case "port":
		port, err := strconv.ParseUint(value, 10, 32)
		if err != nil {
			return fmt.Errorf("invalid port value: %s", value)
		}
		c.Port = uint(port)
		return nil
	case "role":
		if value != "master" && value != "slave" {
			return fmt.Errorf("invalid role: %s", value)
		}
		c.Role = value
		return nil
	case "masterhost":
		c.MasterHost = value
		return nil
	case "masterport":
		port, err := strconv.ParseUint(value, 10, 32)
		if err != nil {
			return fmt.Errorf("invalid masterport value: %s", value)
		}
		c.MasterPort = uint(port)
		return nil
	default:
		return fmt.Errorf("unknown config parameter: %s", param)
	}
}
