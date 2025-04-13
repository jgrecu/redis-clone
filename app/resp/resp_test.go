package resp

import (
	"bufio"
	"bytes"
	"reflect"
	"testing"
)

func TestMarshal(t *testing.T) {
	tests := []struct {
		name     string
		resp     RESP
		expected []byte
	}{
		{
			name:     "String",
			resp:     String("OK"),
			expected: []byte("+OK\r\n"),
		},
		{
			name:     "Error",
			resp:     Error("Error message"),
			expected: []byte("-Error message\r\n"),
		},
		{
			name:     "Integer",
			resp:     Integer(42),
			expected: []byte(":42\r\n"),
		},
		{
			name:     "Bulk",
			resp:     Bulk("hello"),
			expected: []byte("$5\r\nhello\r\n"),
		},
		{
			name:     "Nil",
			resp:     Nil(),
			expected: []byte("$-1\r\n"),
		},
		{
			name: "Array",
			resp: Array(
				Bulk("SET"),
				Bulk("key"),
				Bulk("value"),
			),
			expected: []byte("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.resp.Marshal()
			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("Marshal() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestRespReader_Read(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected RESP
		wantErr  bool
	}{
		{
			name:     "String",
			input:    "+OK\r\n",
			expected: String("OK"),
			wantErr:  false,
		},
		{
			name:     "Integer",
			input:    ":42\r\n",
			expected: Integer(42),
			wantErr:  false,
		},
		{
			name:     "Bulk",
			input:    "$5\r\nhello\r\n",
			expected: Bulk("hello"),
			wantErr:  false,
		},
		{
			name: "Array",
			input: "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n",
			expected: Array(
				Bulk("SET"),
				Bulk("key"),
				Bulk("value"),
			),
			wantErr: false,
		},
		{
			name:    "Invalid type",
			input:   "X42\r\n",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := NewRespReader(bufio.NewReader(bytes.NewBufferString(tt.input)))
			result, err := reader.Read()

			if (err != nil) != tt.wantErr {
				t.Errorf("Read() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr && !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("Read() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestCommand(t *testing.T) {
	tests := []struct {
		name     string
		cmd      string
		args     []string
		expected RESP
	}{
		{
			name: "Simple command",
			cmd:  "PING",
			args: []string{},
			expected: Array(
				Bulk("PING"),
			),
		},
		{
			name: "Command with args",
			cmd:  "SET",
			args: []string{"key", "value"},
			expected: Array(
				Bulk("SET"),
				Bulk("key"),
				Bulk("value"),
			),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Command(tt.cmd, tt.args...)
			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("Command() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// TestRoundTrip tests that marshaling and then reading a RESP object results in the original object
func TestRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		resp RESP
	}{
		{
			name: "String",
			resp: String("OK"),
		},
		{
			name: "Integer",
			resp: Integer(42),
		},
		{
			name: "Bulk",
			resp: Bulk("hello"),
		},
		{
			name: "Array",
			resp: Array(
				Bulk("SET"),
				Bulk("key"),
				Bulk("value"),
			),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Marshal the RESP object
			marshaled := tt.resp.Marshal()

			// Create a reader and read it back
			reader := NewRespReader(bufio.NewReader(bytes.NewBuffer(marshaled)))
			result, err := reader.Read()

			if err != nil {
				t.Errorf("Round trip failed with error: %v", err)
				return
			}

			// Compare the result with the original
			if !reflect.DeepEqual(result, tt.resp) {
				t.Errorf("Round trip failed: got %v, want %v", result, tt.resp)
			}
		})
	}
}