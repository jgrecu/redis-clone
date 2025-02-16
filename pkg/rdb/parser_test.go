package rdb

import (
	"bufio"
	"bytes"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jgrecu/redis-clone/pkg/storage"
)

func TestNewRDBParser(t *testing.T) {
	path := "test.rdb"
	parser := NewRDBParser(path)
	if parser == nil {
		t.Fatal("NewRDBParser returned nil")
	}

	if parser.filePath != path {
		t.Errorf("NewRDBParser filePath = %v, want %v", parser.filePath, path)
	}
}

func TestReadLength(t *testing.T) {
	tests := []struct {
		name    string
		input   []byte
		want    uint32
		wantErr bool
	}{
		{
			name:    "6-bit length",
			input:   []byte{0x12},
			want:    0x12,
			wantErr: false,
		},
		{
			name:    "14-bit length",
			input:   []byte{0x40, 0x02},
			want:    0x02,
			wantErr: false,
		},
		{
			name:    "32-bit length",
			input:   []byte{0x80, 0x00, 0x00, 0x01, 0x02},
			want:    0x102,
			wantErr: false,
		},
		{
			name:    "invalid encoding",
			input:   []byte{0xC0},
			want:    0,
			wantErr: true,
		},
	}

	parser := NewRDBParser("")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := bufio.NewReader(bytes.NewReader(tt.input))
			got, err := parser.ReadLength(reader)
			if (err != nil) != tt.wantErr {
				t.Errorf("ReadLength() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ReadLength() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestReadString(t *testing.T) {
	tests := []struct {
		name    string
		input   []byte
		want    string
		wantErr bool
	}{
		{
			name:    "simple string",
			input:   []byte{0x05, 'h', 'e', 'l', 'l', 'o'},
			want:    "hello",
			wantErr: false,
		},
		{
			name:    "empty string",
			input:   []byte{0x00},
			want:    "",
			wantErr: false,
		},
		{
			name:    "invalid length",
			input:   []byte{0xC0},
			want:    "",
			wantErr: true,
		},
	}

	parser := NewRDBParser("")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := bufio.NewReader(bytes.NewReader(tt.input))
			got, err := parser.ReadString(reader)
			if (err != nil) != tt.wantErr {
				t.Errorf("ReadString() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ReadString() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSaveAndParseRDB(t *testing.T) {
	tests := []struct {
		name     string
		testData map[string]storage.Item
		wantErr  bool
	}{
		{
			name: "basic string values",
			testData: map[string]storage.Item{
				"key1": {
					Value:  "value1",
					Expire: nil,
					Type:   storage.String,
				},
				"key2": {
					Value:  "value2",
					Expire: nil,
					Type:   storage.String,
				},
			},
			wantErr: false,
		},
		{
			name: "with expiration",
			testData: map[string]storage.Item{
				"key1": {
					Value:  "value1",
					Expire: ptr(time.Now().Add(time.Hour)),
					Type:   storage.String,
				},
			},
			wantErr: false,
		},
		{
			name: "with hash table sizes",
			testData: map[string]storage.Item{
				"key1": {
					Value:  "value1",
					Expire: nil,
					Type:   storage.String,
				},
			},
			wantErr: false,
		},
		{
			name:     "with empty database",
			testData: map[string]storage.Item{},
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tempDir, err := os.MkdirTemp("", "rdb-test")
			if err != nil {
				t.Fatalf("Failed to create temp dir: %v", err)
			}
			defer os.RemoveAll(tempDir)

			rdbPath := filepath.Join(tempDir, "test.rdb")
			err = os.MkdirAll(filepath.Dir(rdbPath), 0755)
			if err != nil {
				t.Fatalf("Failed to create parent directory: %v", err)
			}

			parser := NewRDBParser(rdbPath)

			htSize := uint32(len(tt.testData))
			databases := []Database{
				{
					Index:         0,
					Keys:          tt.testData,
					KeysHTSize:    &htSize,
					ExpiresHTSize: &htSize,
				},
			}

			err = parser.SaveRDB(databases)
			if (err != nil) != tt.wantErr {
				t.Fatalf("SaveRDB() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}

			loadedDatabases, err := parser.ParseRDB()
			if err != nil {
				t.Fatalf("ParseRDB() error = %v", err)
			}

			if len(loadedDatabases) < 1 {
				t.Errorf("ParseRDB() returned %d databases, want at least 1", len(loadedDatabases))
			}

			loadedData := loadedDatabases[0].Keys
			if len(loadedData) != len(tt.testData) {
				t.Errorf("ParseRDB() returned %d keys, want %d", len(loadedData), len(tt.testData))
			}

			for key, item := range tt.testData {
				loadedItem, exists := loadedData[key]
				if !exists {
					t.Errorf("Key %s not found in loaded data", key)
					continue
				}
				if loadedItem.Value != item.Value {
					t.Errorf("Value for key %s = %v, want %v", key, loadedItem.Value, item.Value)
				}
				if (loadedItem.Expire == nil) != (item.Expire == nil) {
					t.Errorf("Expire for key %s = %v, want %v", key, loadedItem.Expire, item.Expire)
				}
				if loadedItem.Type != item.Type {
					t.Errorf("Type for key %s = %v, want %v", key, loadedItem.Type, item.Type)
				}
			}
		})
	}
}

func TestParseRDB_InvalidFile(t *testing.T) {
	tests := []struct {
		name    string
		content []byte
		wantErr bool
	}{
		{
			name:    "invalid header",
			content: []byte("INVALID00"),
			wantErr: true,
		},
		{
			name:    "invalid database marker",
			content: append([]byte("REDIS0011"), 0xFF),
			wantErr: false,
		},
		{
			name:    "invalid length in key",
			content: append([]byte("REDIS0011"), 0xFE, 0xC0),
			wantErr: true,
		},
		{
			name:    "invalid expire time format",
			content: append([]byte("REDIS0011"), 0xFE, 0x00, 0xFD, 0x00),
			wantErr: true,
		},
		{
			name:    "invalid expire time ms format",
			content: append([]byte("REDIS0011"), 0xFE, 0x00, 0xFC, 0x00),
			wantErr: true,
		},
		{
			name:    "invalid value type",
			content: append([]byte("REDIS0011"), 0xFE, 0x00, 0xFF, 0x01, 'k'),
			wantErr: true,
		},
		{
			name:    "non-existent file",
			content: nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tempDir, err := os.MkdirTemp("", "rdb-test")
			if err != nil {
				t.Fatalf("Failed to create temp dir: %v", err)
			}
			defer os.RemoveAll(tempDir)

			rdbPath := filepath.Join(tempDir, "test.rdb")
			if tt.content != nil {
				err = os.WriteFile(rdbPath, tt.content, 0644)
				if err != nil {
					t.Fatalf("Failed to write test file: %v", err)
				}
			}

			parser := NewRDBParser(rdbPath)
			_, err = parser.ParseRDB()
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseRDB() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestWriteLength(t *testing.T) {
	tests := []struct {
		name    string
		length  uint32
		want    []byte
		wantErr bool
	}{
		{
			name:    "6-bit length",
			length:  0x3F,
			want:    []byte{0x3F},
			wantErr: false,
		},
		{
			name:    "14-bit length",
			length:  0x3FFF,
			want:    []byte{0x7F, 0xFF},
			wantErr: false,
		},
		{
			name:    "32-bit length",
			length:  0x10000,
			want:    []byte{0x80, 0x00, 0x01, 0x00, 0x00},
			wantErr: false,
		},
	}

	parser := NewRDBParser("")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			writer := bufio.NewWriter(&buf)
			err := parser.writeLength(writer, tt.length)
			writer.Flush()

			if (err != nil) != tt.wantErr {
				t.Errorf("writeLength() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !bytes.Equal(buf.Bytes(), tt.want) {
				t.Errorf("writeLength() = %v, want %v", buf.Bytes(), tt.want)
			}
		})
	}
}

func TestWriteString(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    []byte
		wantErr bool
	}{
		{
			name:    "simple string",
			input:   "hello",
			want:    []byte{0x05, 'h', 'e', 'l', 'l', 'o'},
			wantErr: false,
		},
		{
			name:    "empty string",
			input:   "",
			want:    []byte{0x00},
			wantErr: false,
		},
	}

	parser := NewRDBParser("")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			writer := bufio.NewWriter(&buf)
			err := parser.writeString(writer, tt.input)
			writer.Flush()

			if (err != nil) != tt.wantErr {
				t.Errorf("writeString() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !bytes.Equal(buf.Bytes(), tt.want) {
				t.Errorf("writeString() = %v, want %v", buf.Bytes(), tt.want)
			}
		})
	}
}

func TestCalculateChecksum(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "rdb-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	filePath := filepath.Join(tempDir, "test.rdb")
	err = os.MkdirAll(filepath.Dir(filePath), 0755)
	if err != nil {
		t.Fatalf("Failed to create parent directory: %v", err)
	}

	content := []byte("test data")
	err = os.WriteFile(filePath, content, 0644)
	if err != nil {
		t.Fatalf("Failed to write test file: %v", err)
	}

	parser := NewRDBParser(filePath)
	checksum, err := parser.calculateChecksum(filePath)
	if err != nil {
		t.Fatalf("calculateChecksum() error = %v", err)
	}

	expectedChecksum := uint64(crc32.ChecksumIEEE(content))
	if checksum != expectedChecksum {
		t.Errorf("calculateChecksum() = %v, want %v", checksum, expectedChecksum)
	}
}

func ptr(t time.Time) *time.Time {
	return &t
}
