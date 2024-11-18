package config

import (
	"path/filepath"
	"testing"
)

func TestConfig_GetRDBPath(t *testing.T) {
	tests := []struct {
		name     string
		dir      string
		filename string
		want     string
	}{
		{
			name:     "absolute path",
			dir:      "/tmp/redis",
			filename: "dump.rdb",
			want:     "/tmp/redis/dump.rdb",
		},
		{
			name:     "relative path",
			dir:      "data",
			filename: "dump.rdb",
			want:     filepath.Join("data", "dump.rdb"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := NewConfig()
			cfg.Dir = tt.dir
			cfg.DbFilename = tt.filename

			got := cfg.GetRDBPath()
			if got != tt.want {
				t.Errorf("GetRDBPath() = %v, want %v", got, tt.want)
			}
		})
	}
}
