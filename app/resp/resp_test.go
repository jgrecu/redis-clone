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

func TestMarshal_EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		resp     RESP
		expected []byte
	}{
		{
			name:     "Empty string",
			resp:     String(""),
			expected: []byte("+\r\n"),
		},
		{
			name:     "Empty bulk string",
			resp:     Bulk(""),
			expected: []byte("$0\r\n\r\n"),
		},
		{
			name:     "Negative integer",
			resp:     Integer(-1),
			expected: []byte(":-1\r\n"),
		},
		{
			name:     "Zero integer",
			resp:     Integer(0),
			expected: []byte(":0\r\n"),
		},
		{
			name:     "Large integer",
			resp:     Integer(999999999),
			expected: []byte(":999999999\r\n"),
		},
		{
			name:     "Empty array",
			resp:     Array(),
			expected: []byte("*0\r\n"),
		},
		{
			name:     "Nested array",
			resp:     Array(Array(Bulk("inner"))),
			expected: []byte("*1\r\n*1\r\n$5\r\ninner\r\n"),
		},
		{
			name:     "Array with nil element",
			resp:     Array(Nil(), Bulk("after")),
			expected: []byte("*2\r\n$-1\r\n$5\r\nafter\r\n"),
		},
		{
			name:     "Bulk with spaces",
			resp:     Bulk("hello world"),
			expected: []byte("$11\r\nhello world\r\n"),
		},
		{
			name:     "Error with empty message",
			resp:     Error(""),
			expected: []byte("-\r\n"),
		},
		{
			name:     "Bulk with unicode",
			resp:     Bulk("héllo"),
			expected: []byte("$6\r\nhéllo\r\n"),
		},
		{
			name:     "Unknown type returns nil",
			resp:     RESP{Type: "unknown"},
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.resp.Marshal()
			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("Marshal() = %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestRespReader_Read_EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected RESP
		wantErr  bool
	}{
		{
			name:     "Empty simple string",
			input:    "+\r\n",
			expected: String(""),
			wantErr:  false,
		},
		{
			name:     "Negative integer",
			input:    ":-42\r\n",
			expected: Integer(-42),
			wantErr:  false,
		},
		{
			name:     "Zero integer",
			input:    ":0\r\n",
			expected: Integer(0),
			wantErr:  false,
		},
		{
			name:     "Empty bulk string",
			input:    "$0\r\n\r\n",
			expected: Bulk(""),
			wantErr:  false,
		},
		{
			name:     "Empty array",
			input:    "*0\r\n",
			expected: RESP{Type: "array", Array: []RESP{}},
			wantErr:  false,
		},
		{
			name: "Nested array",
			input: "*1\r\n*1\r\n$5\r\ninner\r\n",
			expected: Array(Array(Bulk("inner"))),
			wantErr: false,
		},
		{
			name:    "Empty input (EOF)",
			input:   "",
			wantErr: true,
		},
		{
			name:    "Truncated bulk (missing data line)",
			input:   "$5\r\n",
			wantErr: true,
		},
		{
			name:    "Truncated array (fewer elements than declared)",
			input:   "*3\r\n$3\r\nfoo\r\n",
			wantErr: true,
		},
		{
			name:    "Error type byte",
			input:   "!garbage\r\n",
			wantErr: true,
		},
		{
			name:    "Non-numeric integer",
			input:   ":abc\r\n",
			wantErr: true,
		},
		{
			name:    "Non-numeric bulk length",
			input:   "$abc\r\nhello\r\n",
			wantErr: true,
		},
		{
			name: "Single element array",
			input: "*1\r\n$4\r\nPING\r\n",
			expected: Array(Bulk("PING")),
			wantErr: false,
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

func TestCommand_EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		cmd      string
		args     []string
		expected RESP
	}{
		{
			name: "Empty command",
			cmd:  "",
			args: []string{},
			expected: Array(Bulk("")),
		},
		{
			name: "Command with empty args",
			cmd:  "SET",
			args: []string{"", ""},
			expected: Array(Bulk("SET"), Bulk(""), Bulk("")),
		},
		{
			name: "Command with many args",
			cmd:  "MSET",
			args: []string{"k1", "v1", "k2", "v2", "k3", "v3"},
			expected: Array(Bulk("MSET"), Bulk("k1"), Bulk("v1"), Bulk("k2"), Bulk("v2"), Bulk("k3"), Bulk("v3")),
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

func TestRoundTrip_EdgeCases(t *testing.T) {
	tests := []struct {
		name string
		resp RESP
	}{
		{name: "Negative integer", resp: Integer(-100)},
		{name: "Zero integer", resp: Integer(0)},
		{name: "Empty bulk", resp: Bulk("")},
		{name: "Empty string", resp: String("")},
		{name: "Empty array", resp: RESP{Type: "array", Array: []RESP{}}},
		{name: "Nested array", resp: Array(Array(Bulk("a"), Bulk("b")), Array(Bulk("c")))},
		{name: "Array with mixed types", resp: Array(Bulk("key"), Integer(42))},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			marshaled := tt.resp.Marshal()
			reader := NewRespReader(bufio.NewReader(bytes.NewBuffer(marshaled)))
			result, err := reader.Read()

			if err != nil {
				t.Errorf("Round trip failed with error: %v", err)
				return
			}

			if !reflect.DeepEqual(result, tt.resp) {
				t.Errorf("Round trip failed: got %v, want %v", result, tt.resp)
			}
		})
	}
}