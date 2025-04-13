package handlers

import (
	"github.com/jgrecu/redis-clone/app/resp"
	"reflect"
	"testing"
)

func TestGetHandler(t *testing.T) {
	tests := []struct {
		name          string
		command       string
		shouldBeFound bool
	}{
		{
			name:          "Existing command - PING",
			command:       "PING",
			shouldBeFound: true,
		},
		{
			name:          "Existing command - GET",
			command:       "GET",
			shouldBeFound: true,
		},
		{
			name:          "Non-existing command",
			command:       "NONEXISTENT",
			shouldBeFound: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := GetHandler(tt.command)
			
			// Check if handler is found or not
			if tt.shouldBeFound {
				if reflect.ValueOf(handler).Pointer() == reflect.ValueOf(notFound).Pointer() {
					t.Errorf("GetHandler(%s) returned notFound, expected a valid handler", tt.command)
				}
			} else {
				if reflect.ValueOf(handler).Pointer() != reflect.ValueOf(notFound).Pointer() {
					t.Errorf("GetHandler(%s) returned a handler, expected notFound", tt.command)
				}
			}
		})
	}
}

func TestPing(t *testing.T) {
	tests := []struct {
		name     string
		params   []resp.RESP
		expected []byte
	}{
		{
			name:     "Simple PING",
			params:   []resp.RESP{},
			expected: resp.String("PONG").Marshal(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ping(tt.params)
			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("ping() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestEcho(t *testing.T) {
	tests := []struct {
		name     string
		params   []resp.RESP
		expected []byte
	}{
		{
			name: "Echo hello",
			params: []resp.RESP{
				{Type: "bulk", Bulk: "hello"},
			},
			expected: resp.String("hello").Marshal(),
		},
		{
			name: "Echo empty string",
			params: []resp.RESP{
				{Type: "bulk", Bulk: ""},
			},
			expected: resp.String("").Marshal(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := echo(tt.params)
			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("echo() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestNotFound(t *testing.T) {
	result := notFound([]resp.RESP{})
	expected := resp.Error("Command not found").Marshal()
	
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("notFound() = %v, want %v", result, expected)
	}
}