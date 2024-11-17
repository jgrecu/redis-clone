package server

import (
	"reflect"
	"testing"
	"time"

	"github.com/jgrecu/redis-clone/pkg/resp"
	"github.com/jgrecu/redis-clone/pkg/storage"
)

func TestPingCommand_Execute(t *testing.T) {
	writer := resp.NewWriter()
	cmd := NewPingCommand(writer)

	got, err := cmd.Execute([]string{"PING"})
	if err != nil {
		t.Errorf("Execute() error = %v", err)
		return
	}

	want := []byte("+PONG\r\n")
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Execute() got = %v, want %v", got, want)
	}
}

func TestSetCommand_Execute(t *testing.T) {
	writer := resp.NewWriter()
	store := storage.NewStore(time.Minute)
	cmd := NewSetCommand(writer, store)

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
			got, err := cmd.Execute(tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("Execute() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Execute() got = %v, want %v", got, tt.want)
			}
		})
	}
}
