package config

import (
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestNewConfig(t *testing.T) {
	cfg := NewConfig()

	tests := []struct {
		name string
		want interface{}
		got  interface{}
	}{
		{"Address", "0.0.0.0", cfg.Address},
		{"Port", uint(6379), cfg.Port},
		{"CleanupInterval", time.Minute, cfg.CleanupInterval},
		{"Role", "master", cfg.Role},
		{"MasterHost", "", cfg.MasterHost},
		{"MasterPort", uint(0), cfg.MasterPort},
		{"Dir", ".", cfg.Dir},
		{"DbFilename", "dump.rdb", cfg.DbFilename},
		{"SaveInterval", time.Minute * 15, cfg.SaveInterval},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !reflect.DeepEqual(tt.got, tt.want) {
				t.Errorf("NewConfig() %s = %v, want %v", tt.name, tt.got, tt.want)
			}
		})
	}
}

func TestConfig_Get(t *testing.T) {
	cfg := NewConfig()
	tests := []struct {
		name    string
		key     string
		want    string
		wantErr bool
	}{
		{"Get dir", "dir", ".", false},
		{"Get dbfilename", "dbfilename", "dump.rdb", false},
		{"Get port", "port", "6379", false},
		{"Get role", "role", "master", false},
		{"Get masterhost", "masterhost", "", false},
		{"Get masterport", "masterport", "0", false},
		{"Get invalid key", "invalid", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := cfg.Get(tt.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("Config.Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Config.Get() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConfig_Set(t *testing.T) {
	cfg := NewConfig()
	tests := []struct {
		name    string
		key     string
		value   string
		wantErr bool
		check   func(*Config) bool
	}{
		{
			name:    "Set dir",
			key:     "dir",
			value:   "/tmp",
			wantErr: false,
			check: func(c *Config) bool {
				return c.Dir == "/tmp"
			},
		},
		{
			name:    "Set dbfilename",
			key:     "dbfilename",
			value:   "test.rdb",
			wantErr: false,
			check: func(c *Config) bool {
				return c.DbFilename == "test.rdb"
			},
		},
		{
			name:    "Set port",
			key:     "port",
			value:   "6380",
			wantErr: false,
			check: func(c *Config) bool {
				return c.Port == 6380
			},
		},
		{
			name:    "Set role",
			key:     "role",
			value:   "slave",
			wantErr: false,
			check: func(c *Config) bool {
				return c.Role == "slave"
			},
		},
		{
			name:    "Set masterhost",
			key:     "masterhost",
			value:   "localhost",
			wantErr: false,
			check: func(c *Config) bool {
				return c.MasterHost == "localhost"
			},
		},
		{
			name:    "Set masterport",
			key:     "masterport",
			value:   "6379",
			wantErr: false,
			check: func(c *Config) bool {
				return c.MasterPort == 6379
			},
		},
		{
			name:    "Set invalid port",
			key:     "port",
			value:   "invalid",
			wantErr: true,
			check: func(c *Config) bool {
				return c.Port == 6380 // Should not change from previous test
			},
		},
		{
			name:    "Set invalid masterport",
			key:     "masterport",
			value:   "invalid",
			wantErr: true,
			check: func(c *Config) bool {
				return c.MasterPort == 6379 // Should not change from previous test
			},
		},
		{
			name:    "Set invalid key",
			key:     "invalid",
			value:   "value",
			wantErr: true,
			check: func(c *Config) bool {
				return true // No state change expected
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := cfg.Set(tt.key, tt.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("Config.Set() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.check(cfg) {
				t.Errorf("Config.Set() did not set the correct value for %s", tt.key)
			}
		})
	}
}

func TestConfig_GetRDBPath(t *testing.T) {
	tests := []struct {
		name     string
		dir      string
		filename string
		want     string
	}{
		{
			name:     "Default path",
			dir:      ".",
			filename: "dump.rdb",
			want:     filepath.Join(".", "dump.rdb"),
		},
		{
			name:     "Custom path",
			dir:      "/tmp",
			filename: "test.rdb",
			want:     filepath.Join("/tmp", "test.rdb"),
		},
		{
			name:     "Relative path",
			dir:      "data",
			filename: "backup.rdb",
			want:     filepath.Join("data", "backup.rdb"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := NewConfig()
			cfg.Dir = tt.dir
			cfg.DbFilename = tt.filename

			got := cfg.GetRDBPath()
			if got != tt.want {
				t.Errorf("Config.GetRDBPath() = %v, want %v", got, tt.want)
			}
		})
	}
}
