package server

import (
	"fmt"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jgrecu/redis-clone/pkg/config"
	"github.com/jgrecu/redis-clone/pkg/rdb"
	"github.com/jgrecu/redis-clone/pkg/resp"
	"github.com/jgrecu/redis-clone/pkg/storage"
)

func TestEchoCommand_Execute(t *testing.T) {
	writer := resp.NewWriter()
	cmd := NewEchoCommand(writer)

	// Pre-compute expected responses using the same writer
	helloResp := writer.WriteSimpleString("hello")
	errResp := writer.WriteError("wrong number of argument")

	tests := []struct {
		name    string
		args    []string
		want    []byte
		wantErr bool
	}{
		{
			name:    "echo message",
			args:    []string{"ECHO", "hello"},
			want:    helloResp,
			wantErr: false,
		},
		{
			name:    "missing argument",
			args:    []string{"ECHO"},
			want:    errResp,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := cmd.Execute(tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("Execute() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Execute() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetCommand_Execute(t *testing.T) {
	writer := resp.NewWriter()
	store := storage.NewStore(time.Second)
	cmd := NewGetCommand(writer, store)

	// Set up test data
	store.Set("key1", "value1", 0)
	store.Set("key2", "value2", time.Hour)

	// Pre-compute expected responses using the same writer
	value1Resp := writer.WriteSimpleString("value1")
	value2Resp := writer.WriteSimpleString("value2")
	nullResp := writer.WriteNullBulk()
	errResp := writer.WriteError("wrong number of arguments")

	tests := []struct {
		name    string
		args    []string
		want    []byte
		wantErr bool
	}{
		{
			name:    "get existing key",
			args:    []string{"GET", "key1"},
			want:    value1Resp,
			wantErr: false,
		},
		{
			name:    "get non-existent key",
			args:    []string{"GET", "nonexistent"},
			want:    nullResp,
			wantErr: false,
		},
		{
			name:    "get with expiration",
			args:    []string{"GET", "key2"},
			want:    value2Resp,
			wantErr: false,
		},
		{
			name:    "missing key argument",
			args:    []string{"GET"},
			want:    errResp,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := cmd.Execute(tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("Execute() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Execute() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPingCommand_Execute(t *testing.T) {
	writer := resp.NewWriter()
	cmd := NewPingCommand(writer)

	// Pre-compute expected response using the same writer
	pongResp := writer.WriteSimpleString("PONG")

	tests := []struct {
		name    string
		args    []string
		want    []byte
		wantErr bool
	}{
		{
			name:    "simple ping",
			args:    []string{"PING"},
			want:    pongResp,
			wantErr: false,
		},
		{
			name:    "ping with extra args",
			args:    []string{"PING", "extra"},
			want:    pongResp,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := cmd.Execute(tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("Execute() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Execute() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestSetCommand_Execute(t *testing.T) {
	store := storage.NewStore(time.Second)
	writer := resp.NewWriter()
	srv := NewServer(config.NewConfig())
	cmd := NewSetCommand(writer, store, srv)

	// Pre-compute expected responses using the same writer
	okResp := writer.WriteSimpleString("OK")
	errResp := writer.WriteError("wrong number of arguments")

	tests := []struct {
		name    string
		args    []string
		want    []byte
		wantErr bool
	}{
		{
			name:    "simple set",
			args:    []string{"SET", "key", "value"},
			want:    okResp,
			wantErr: false,
		},
		{
			name:    "set with expiration",
			args:    []string{"SET", "key2", "value2", "px", "1000"},
			want:    okResp,
			wantErr: false,
		},
		{
			name:    "missing value",
			args:    []string{"SET", "key"},
			want:    errResp,
			wantErr: false,
		},
		{
			name:    "invalid expiration format",
			args:    []string{"SET", "key", "value", "px", "invalid"},
			want:    okResp, // Invalid expiration is ignored
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := cmd.Execute(tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("Execute() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Execute() = %q, want %q", got, tt.want)
			}

			// Verify the value was actually set (except for error cases)
			if len(tt.args) >= 3 && !tt.wantErr {
				if val, exists := store.Get(tt.args[1]); !exists {
					t.Errorf("Key %s not set", tt.args[1])
				} else if val != tt.args[2] {
					t.Errorf("Value mismatch for key %s: got %s, want %s", tt.args[1], val, tt.args[2])
				}
			}
		})
	}
}

func TestConfigCommand(t *testing.T) {
	cfg := config.NewConfig()
	writer := resp.NewWriter()
	cmd := NewConfigGetCommand(writer, cfg)

	tests := []struct {
		name    string
		args    []string
		want    []byte
		wantErr bool
	}{
		{
			name:    "get port",
			args:    []string{"CONFIG", "GET", "port"},
			want:    writer.WriteArray([]string{"port", fmt.Sprintf("%d", cfg.Port)}),
			wantErr: false,
		},
		{
			name:    "get dir",
			args:    []string{"CONFIG", "GET", "dir"},
			want:    writer.WriteArray([]string{"dir", cfg.Dir}),
			wantErr: false,
		},
		{
			name:    "get invalid key",
			args:    []string{"CONFIG", "GET", "invalid"},
			want:    writer.WriteError("unknown config parameter: invalid"),
			wantErr: false,
		},
		{
			name:    "wrong number of arguments",
			args:    []string{"CONFIG", "GET"},
			want:    writer.WriteError("wrong number of arguments for CONFIG GET"),
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := cmd.Execute(tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("Execute() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Execute() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestKeysCommand_Execute(t *testing.T) {
	writer := resp.NewWriter()
	store := storage.NewStore(time.Second)
	cmd := NewKeysCommand(writer, store)

	// Set up test data
	store.Set("key1", "value1", 0)
	store.Set("key2", "value2", 0)
	store.Set("other", "value3", 0)
	store.Set("test", "value4", 0)

	// Helper function to extract keys from RESP array response
	extractKeys := func(resp []byte) map[string]bool {
		keys := make(map[string]bool)
		parts := strings.Split(string(resp), "\r\n")
		for i := 2; i < len(parts)-1; i += 2 {
			if !strings.HasPrefix(parts[i], "$") {
				keys[parts[i]] = true
			}
		}
		return keys
	}

	tests := []struct {
		name    string
		args    []string
		want    []byte
		wantErr bool
		check   func([]byte) bool
	}{
		{
			name:    "all keys",
			args:    []string{"KEYS", "*"},
			want:    []byte("*4\r\n"), // Only check count
			wantErr: false,
			check: func(got []byte) bool {
				keys := extractKeys(got)
				expectedKeys := map[string]bool{"key1": true, "key2": true, "other": true, "test": true}
				if len(keys) != len(expectedKeys) {
					return false
				}
				for k := range expectedKeys {
					if !keys[k] {
						return false
					}
				}
				return true
			},
		},
		{
			name:    "key pattern",
			args:    []string{"KEYS", "key*"},
			want:    []byte("*2\r\n"), // Only check count
			wantErr: false,
			check: func(got []byte) bool {
				keys := extractKeys(got)
				expectedKeys := map[string]bool{"key1": true, "key2": true}
				if len(keys) != len(expectedKeys) {
					return false
				}
				for k := range expectedKeys {
					if !keys[k] {
						return false
					}
				}
				return true
			},
		},
		{
			name:    "no matches",
			args:    []string{"KEYS", "nomatch*"},
			want:    []byte("$-1\r\n"),
			wantErr: false,
		},
		{
			name:    "missing pattern",
			args:    []string{"KEYS"},
			want:    []byte("-wrong number of arguments\r\n"),
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := cmd.Execute(tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("Execute() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.check != nil {
				if !tt.check(got) {
					t.Errorf("Execute() failed key validation")
				}
			} else if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Execute() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSaveCommand_Execute(t *testing.T) {
	writer := resp.NewWriter()
	cfg := config.NewConfig()
	store := storage.NewStore(time.Second)
	rdb := rdb.NewRDB(cfg, store)
	cmd := NewSaveCommand(writer, rdb)

	tests := []struct {
		name    string
		args    []string
		want    []byte
		wantErr bool
	}{
		{
			name:    "basic save",
			args:    []string{"SAVE"},
			want:    []byte("+OK\r\n"),
			wantErr: false,
		},
		{
			name:    "save with extra args",
			args:    []string{"SAVE", "extra"},
			want:    []byte("+OK\r\n"),  // The SAVE command ignores extra arguments
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := cmd.Execute(tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("Execute() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Execute() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestReplConfCommand_Execute(t *testing.T) {
	writer := resp.NewWriter()
	cfg := config.NewConfig()
	cfg.Role = "master" // Set server as master to accept replica connections
	server := NewServer(cfg)
	server.currentConn = &net.TCPConn{} // Mock connection for replica
	cmd := NewReplConfCommand(writer, server)

	tests := []struct {
		name    string
		args    []string
		want    []byte
		wantErr bool
	}{
		{
			name:    "listening-port",
			args:    []string{"REPLCONF", "listening-port", "6380"},
			want:    []byte("+OK\r\n"),
			wantErr: false,
		},
		{
			name:    "capa",
			args:    []string{"REPLCONF", "capa", "psync2"},
			want:    []byte("+OK\r\n"),
			wantErr: false,
		},
		{
			name:    "missing subcommand",
			args:    []string{"REPLCONF"},
			want:    []byte("-wrong number of arguments for REPLCONF\r\n"),
			wantErr: false,
		},
		{
			name:    "unknown subcommand",
			args:    []string{"REPLCONF", "unknown"},
			want:    []byte("-unknown REPLCONF subcommand 'unknown'\r\n"),
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := cmd.Execute(tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("Execute() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Execute() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPSyncCommand_Execute(t *testing.T) {
	writer := resp.NewWriter()
	cfg := config.NewConfig()
	cfg.Role = "master" // Set server as master to accept replica connections
	server := NewServer(cfg)
	// Skip PSYNC test as it requires actual network connection
	t.Skip("Skipping PSYNC test as it requires actual network connection")
	cmd := NewPSyncCommand(writer, server)

	tests := []struct {
		name    string
		args    []string
		want    []byte
		wantErr bool
	}{
		{
			name:    "psync with arguments",
			args:    []string{"PSYNC", "?", "-1"},
			want:    []byte("+FULLRESYNC 0000000000000000000000000000000000000000 0\r\n"),
			wantErr: false,
		},
		{
			name:    "missing arguments",
			args:    []string{"PSYNC"},
			want:    []byte("-ERR wrong number of arguments for PSYNC\r\n"),
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := cmd.Execute(tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("Execute() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Execute() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestInfoCommand(t *testing.T) {
	writer := resp.NewWriter()
	cfg := config.NewConfig()
	server := NewServer(cfg)
	cmd := NewInfoCommand(writer, cfg, server)

	// Pre-compute expected responses
	masterInfo := "# Replication\r\nrole:master\r\nconnected_slaves:0\r\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\r\nmaster_repl_offset:0\r\n"
	slaveInfo := "# Replication\r\nrole:slave\r\nmaster_host:localhost\r\nmaster_port:6379\r\nmaster_link_status:up\r\n"
	masterInfoResp := writer.WriteBulkString(masterInfo)
	slaveInfoResp := writer.WriteBulkString(slaveInfo)
	wrongArgsResp := writer.WriteBulkString(masterInfo) // INFO command ignores extra arguments

	tests := []struct {
		name       string
		role       string
		masterHost string
		masterPort uint
		args       []string
		want       []byte
		wantErr    bool
	}{
		{
			name:       "master info",
			role:       "master",
			masterHost: "",
			masterPort: 0,
			args:      []string{"INFO"},
			want:      masterInfoResp,
			wantErr:   false,
		},
		{
			name:       "slave info",
			role:       "slave",
			masterHost: "localhost",
			masterPort: 6379,
			args:      []string{"INFO"},
			want:      slaveInfoResp,
			wantErr:   false,
		},
		{
			name:       "info with section",
			role:       "master",
			args:      []string{"INFO", "replication"},
			want:      masterInfoResp,
			wantErr:   false,
		},
		{
			name:       "info with invalid section",
			role:       "master",
			args:      []string{"INFO", "invalid"},
			want:      masterInfoResp,
			wantErr:   false,
		},
		{
			name:       "info with too many args",
			role:       "master",
			args:      []string{"INFO", "replication", "extra"},
			want:      wrongArgsResp,
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Update config for this test
			cfg.Role = tt.role
			cfg.MasterHost = tt.masterHost
			cfg.MasterPort = tt.masterPort

			got, err := cmd.Execute(tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("Execute() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Execute() = %v, want %v", got, tt.want)
			}
		})
	}
}
