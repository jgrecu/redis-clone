package config

import (
	"path/filepath"
	"reflect"
	"strconv"
	"sync"
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

func TestConfig_SaveInterval(t *testing.T) {
	cfg := NewConfig()
	tests := []struct {
		name    string
		value   string
		want    time.Duration
		wantErr bool
	}{
		{
			name:    "Valid save interval",
			value:   "300",
			want:    300 * time.Second,
			wantErr: false,
		},
		{
			name:    "Zero save interval",
			value:   "0",
			want:    0,
			wantErr: false,
		},
		{
			name:    "Negative save interval",
			value:   "-1",
			wantErr: true,
		},
		{
			name:    "Invalid format",
			value:   "invalid",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := cfg.Set("save", tt.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("Config.Set(save) error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && cfg.SaveInterval != tt.want {
				t.Errorf("Config.SaveInterval = %v, want %v", cfg.SaveInterval, tt.want)
			}
		})
	}
}

func TestConfig_ConcurrentAccess(t *testing.T) {
	cfg := NewConfig()
	const goroutines = 10
	const iterations = 100

	var wg sync.WaitGroup
	wg.Add(goroutines * 2) // For both readers and writers

	// Start reader goroutines
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				_, err := cfg.Get("port")
				if err != nil {
					t.Errorf("Concurrent Get failed: %v", err)
				}
			}
		}()
	}

	// Start writer goroutines
	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				port := 6380 + id
				err := cfg.Set("port", strconv.Itoa(port))
				if err != nil {
					t.Errorf("Concurrent Set failed: %v", err)
				}
			}
		}(i)
	}

	wg.Wait()
}

func TestConfig_InvalidRole(t *testing.T) {
	cfg := NewConfig()
	tests := []struct {
		name    string
		role    string
		wantErr bool
	}{
		{"Valid master role", "master", false},
		{"Valid slave role", "slave", false},
		{"Invalid role", "invalid", true},
		{"Empty role", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := cfg.Set("role", tt.role)
			if (err != nil) != tt.wantErr {
				t.Errorf("Config.Set(role) error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && cfg.Role != tt.role {
				t.Errorf("Config.Role = %v, want %v", cfg.Role, tt.role)
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
		{
			name:     "Path with spaces",
			dir:      "data dir",
			filename: "my backup.rdb",
			want:     filepath.Join("data dir", "my backup.rdb"),
		},
		{
			name:     "Nested path",
			dir:      filepath.Join("data", "redis", "backups"),
			filename: "dump.rdb",
			want:     filepath.Join("data", "redis", "backups", "dump.rdb"),
		},
		{
			name:     "Empty directory",
			dir:      "",
			filename: "dump.rdb",
			want:     "dump.rdb",
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

func TestConfig_CleanupInterval(t *testing.T) {
	cfg := NewConfig()

	// Test default value
	if cfg.CleanupInterval != time.Minute {
		t.Errorf("Default CleanupInterval = %v, want %v", cfg.CleanupInterval, time.Minute)
	}

	// Test setting custom cleanup interval
	tests := []struct {
		name     string
		interval time.Duration
		want     time.Duration
	}{
		{
			name:     "30 seconds interval",
			interval: 30 * time.Second,
			want:     30 * time.Second,
		},
		{
			name:     "5 minutes interval",
			interval: 5 * time.Minute,
			want:     5 * time.Minute,
		},
		{
			name:     "1 hour interval",
			interval: time.Hour,
			want:     time.Hour,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg.CleanupInterval = tt.interval
			if cfg.CleanupInterval != tt.want {
				t.Errorf("CleanupInterval = %v, want %v", cfg.CleanupInterval, tt.want)
			}
		})
	}
}
