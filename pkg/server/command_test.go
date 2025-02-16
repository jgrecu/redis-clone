package server

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jgrecu/redis-clone/pkg/config"
	"github.com/jgrecu/redis-clone/pkg/resp"
	"github.com/jgrecu/redis-clone/pkg/storage"
)

func TestPingCommand_Execute(t *testing.T) {
	writer := resp.NewWriter()
	cmd := NewPingCommand(writer)

	got, err := cmd.Execute([]string{"PING"})
	if err != nil {
		t.Errorf("PingCommand.Execute() error = %v", err)
		return
	}

	want := writer.WriteSimpleString("PONG")
	if !reflect.DeepEqual(got, want) {
		t.Errorf("PingCommand.Execute() = %v, want %v", got, want)
	}
}

func TestSetCommand_Execute(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		want    []byte
		wantErr bool
	}{
		{
			name: "simple set",
			args: []string{"SET", "key", "value"},
			want: []byte("+OK\r\n"),
		},
		{
			name:    "invalid args",
			args:    []string{"SET"},
			want:    []byte("-wrong number of arguments\r\n"),
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := storage.NewStore(time.Second)
			writer := resp.NewWriter()
			cmd := NewSetCommand(writer, store)

			got, err := cmd.Execute(tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("SetCommand.Execute() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("SetCommand.Execute() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestInfoCommand(t *testing.T) {
	tests := []struct {
		name       string
		role       string
		masterHost string
		masterPort uint
		want       []string
	}{
		{
			name:       "Master role",
			role:       "master",
			masterHost: "",
			masterPort: 0,
			want: []string{
				"role:master",
				"connected_slaves:0",
				"master_replid:",
				"master_repl_offset:0",
			},
		},
		{
			name:       "Slave role",
			role:       "slave",
			masterHost: "localhost",
			masterPort: 6379,
			want: []string{
				"role:slave",
				"master_host:localhost",
				"master_port:6379",
				"master_link_status:up",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := config.NewConfig()
			cfg.Role = tt.role
			cfg.MasterHost = tt.masterHost
			cfg.MasterPort = tt.masterPort

			writer := resp.NewWriter()
			cmd := NewInfoCommand(writer, cfg)

			got, err := cmd.Execute([]string{"INFO", "replication"})
			if err != nil {
				t.Errorf("InfoCommand.Execute() error = %v", err)
				return
			}

			// Convert bytes to string for easier inspection
			info := string(got)
			for _, want := range tt.want {
				if !strings.Contains(info, want) {
					t.Errorf("InfoCommand.Execute() = %v, want %v", info, want)
				}
			}
		})
	}
}
