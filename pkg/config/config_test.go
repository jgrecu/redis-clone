package config

import (
	"fmt"
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
		errMsg  string
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
			name:    "Set invalid port",
			key:     "port",
			value:   "invalid",
			wantErr: true,
			errMsg:  "invalid port value: invalid",
		},
		{
			name:    "Set role master",
			key:     "role",
			value:   "master",
			wantErr: false,
			check: func(c *Config) bool {
				return c.Role == "master"
			},
		},
		{
			name:    "Set role slave",
			key:     "role",
			value:   "slave",
			wantErr: false,
			check: func(c *Config) bool {
				return c.Role == "slave"
			},
		},
		{
			name:    "Set invalid role",
			key:     "role",
			value:   "invalid",
			wantErr: true,
			errMsg:  "invalid role: invalid",
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := cfg.Set(tt.key, tt.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("Config.Set() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				if err == nil {
					t.Error("Config.Set() expected error but got nil")
					return
				}
				if err.Error() != tt.errMsg {
					t.Errorf("Config.Set() error = %v, want %v", err.Error(), tt.errMsg)
				}
				return
			}
			if tt.check != nil && !tt.check(cfg) {
				t.Errorf("Config.Set() failed validation for %s = %s", tt.key, tt.value)
			}
		})
	}
}

func TestConfig_Set_SaveInterval(t *testing.T) {
	cfg := NewConfig()
	tests := []struct {
		name    string
		value   string
		wantErr bool
		errMsg  string
		want    time.Duration
	}{
		{
			name:    "Valid interval",
			value:   "60",
			wantErr: false,
			want:    time.Second * 60,
		},
		{
			name:    "Invalid format",
			value:   "invalid",
			wantErr: true,
			errMsg:  "invalid save interval: strconv.Atoi: parsing \"invalid\": invalid syntax",
		},
		{
			name:    "Negative interval",
			value:   "-60",
			wantErr: true,
			errMsg:  "save interval must be positive",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := cfg.Set("save", tt.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("Config.Set(save) error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				if err == nil {
					t.Error("Config.Set(save) expected error but got nil")
					return
				}
				if err.Error() != tt.errMsg {
					t.Errorf("Config.Set(save) error = %v, want %v", err.Error(), tt.errMsg)
				}
				return
			}
			if cfg.SaveInterval != tt.want {
				t.Errorf("Config.SaveInterval = %v, want %v", cfg.SaveInterval, tt.want)
			}
		})
	}
}

func TestConfig_Concurrent(t *testing.T) {
	cfg := NewConfig()
	var wg sync.WaitGroup
	numWorkers := 10
	numOperations := 100
	errChan := make(chan error, numWorkers*numOperations)

	// Concurrent reads and writes
	for i := 0; i < numWorkers; i++ {
		wg.Add(2) // One for reader, one for writer

		// Reader
		go func() {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				_, err := cfg.Get("port")
				if err != nil {
					errChan <- fmt.Errorf("read error: %v", err)
				}
			}
		}()

		// Writer
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				port := 6379 + id
				err := cfg.Set("port", strconv.Itoa(port))
				if err != nil {
					errChan <- fmt.Errorf("write error: %v", err)
				}
			}
		}(i)
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		t.Error(err)
	}
}
