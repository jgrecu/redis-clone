package rdb

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jgrecu/redis-clone/pkg/storage"
)

func TestParseRDB_ErrorCases(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "rdb-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	tests := []struct {
		name     string
		content  []byte
		wantErr  bool
	}{
		{
			name:     "empty file",
			content:  []byte{},
			wantErr:  true,
		},
		{
			name:     "invalid header",
			content:  []byte("INVALID00"),
			wantErr:  true,
		},
		{
			name:     "truncated file",
			content:  []byte("REDIS0011\xFE"),
			wantErr:  true,
		},
		{
			name:     "corrupted length",
			content:  append([]byte("REDIS0011\xFE"), 0xFF, 0xFF),
			wantErr:  true,
		},
		{
			name:     "invalid value type",
			content:  append([]byte("REDIS0011\xFE\x00"), 0xFF),
			wantErr:  true,
		},
		{
			name:     "invalid database index",
			content:  append([]byte("REDIS0011\xFE"), 0xFF, 0xFF, 0xFF, 0xFF),
			wantErr:  true,
		},
		{
			name:     "invalid checksum",
			content:  append([]byte("REDIS0011\xFE\x00"), make([]byte, 8)...),
			wantErr:  true,
		},
		{
			name:     "invalid expire time",
			content:  append([]byte("REDIS0011\xFE\x00\xFC"), make([]byte, 4)...),
			wantErr:  true,
		},
		{
			name:     "invalid hash table size",
			content:  append([]byte("REDIS0011\xFE\x00\xFB"), 0xFF),
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rdbPath := filepath.Join(tempDir, "test.rdb")
			if err := os.WriteFile(rdbPath, tt.content, 0644); err != nil {
				t.Fatalf("Failed to write test file: %v", err)
			}

			parser := NewRDBParser(rdbPath)
			_, err := parser.ParseRDB()
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseRDB() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestSaveRDB_ErrorCases(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "rdb-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	tests := []struct {
		name    string
		setup   func(string) error
		wantErr bool
	}{
		{
			name: "permission denied",
			setup: func(path string) error {
				dir := filepath.Dir(path)
				if err := os.MkdirAll(dir, 0755); err != nil {
					return err
				}
				if err := os.WriteFile(path, []byte{}, 0644); err != nil {
					return err
				}
				return os.Chmod(path, 0444)
			},
			wantErr: true,
		},
		{
			name: "directory not exists",
			setup: func(path string) error {
				dir := filepath.Dir(path)
				if err := os.MkdirAll(dir, 0755); err != nil {
					return err
				}
				return os.Chmod(dir, 0444)
			},
			wantErr: true,
		},
		{
			name: "invalid path",
			setup: func(path string) error {
				return os.MkdirAll(path, 0755) // path is treated as directory
			},
			wantErr: true,
		},
		{
			name: "parent directory not writable",
			setup: func(path string) error {
				dir := filepath.Dir(path)
				if err := os.MkdirAll(dir, 0755); err != nil {
					return err
				}
				return os.Chmod(dir, 0555)
			},
			wantErr: true,
		},
		{
			name: "disk full simulation",
			setup: func(path string) error {
				dir := filepath.Dir(path)
				if err := os.MkdirAll(dir, 0755); err != nil {
					return err
				}
				// Create a file that can't be written to
				f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0644)
				if err != nil {
					return err
				}
				f.Close()
				return os.Chmod(path, 0444)
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rdbPath := filepath.Join(tempDir, strings.ReplaceAll(tt.name, " ", "_"), "test.rdb")
			if tt.setup != nil {
				if err := tt.setup(rdbPath); err != nil {
					t.Fatalf("Setup failed: %v", err)
				}
			}

			parser := NewRDBParser(rdbPath)
			err := parser.SaveRDB([]Database{
				{
					Index: 0,
					Keys:  make(map[string]storage.Item),
				},
			})
			if (err != nil) != tt.wantErr {
				t.Errorf("SaveRDB() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestSaveRDB_ContentEdgeCases(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "rdb-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	tests := []struct {
		name     string
		database Database
		wantErr  bool
	}{
		{
			name: "large values",
			database: Database{
				Index: 0,
				Keys: map[string]storage.Item{
					"large_key": {
						Value:  string(make([]byte, 1024*1024)), // 1MB value
						Type:   storage.String,
						Expire: nil,
					},
				},
			},
			wantErr: false,
		},
		{
			name: "special characters",
			database: Database{
				Index: 0,
				Keys: map[string]storage.Item{
					"key\x00with\nnull": {
						Value:  "value\x00with\nnull",
						Type:   storage.String,
						Expire: nil,
					},
				},
			},
			wantErr: false,
		},
		{
			name: "empty values",
			database: Database{
				Index: 0,
				Keys: map[string]storage.Item{
					"": {
						Value:  "",
						Type:   storage.String,
						Expire: nil,
					},
					"empty_value": {
						Value:  "",
						Type:   storage.String,
						Expire: nil,
					},
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rdbPath := filepath.Join(tempDir, tt.name+".rdb")
			parser := NewRDBParser(rdbPath)

			// Save the database
			err := parser.SaveRDB([]Database{tt.database})
			if (err != nil) != tt.wantErr {
				t.Errorf("SaveRDB() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if err != nil {
				return
			}

			// Try to load and verify the content
			loadedDatabases, err := parser.ParseRDB()
			if err != nil {
				t.Errorf("ParseRDB() error = %v", err)
				return
			}

			if len(loadedDatabases) != 1 {
				t.Errorf("ParseRDB() got %v databases, want 1", len(loadedDatabases))
				return
			}

			loadedDB := loadedDatabases[0]
			if len(loadedDB.Keys) != len(tt.database.Keys) {
				t.Errorf("ParseRDB() got %v keys, want %v", len(loadedDB.Keys), len(tt.database.Keys))
				return
			}

			for k, v := range tt.database.Keys {
				loadedValue, exists := loadedDB.Keys[k]
				if !exists {
					t.Errorf("Key %q not found in loaded database", k)
					continue
				}
				if loadedValue.Value != v.Value {
					t.Errorf("Value mismatch for key %q: got %q, want %q", k, loadedValue.Value, v.Value)
				}
				if loadedValue.Type != v.Type {
					t.Errorf("Type mismatch for key %q: got %v, want %v", k, loadedValue.Type, v.Type)
				}
			}
		})
	}
}
