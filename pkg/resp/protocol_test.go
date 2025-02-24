package resp

import (
	"reflect"
	"testing"
)

func TestParser_Parse(t *testing.T) {
	tests := []struct {
		name    string
		input   []byte
		want    *Message
		wantErr bool
	}{
		{
			name:  "parse simple array",
			input: []byte("*2\r\n$4\r\nPING\r\n$4\r\nPONG\r\n"),
			want: &Message{
				Type:    '*',
				Length:  2,
				Content: []string{"PING", "PONG"},
			},
			wantErr: false,
		},
		{
			name:    "parse invalid input",
			input:   []byte("invalid"),
			want:    nil,
			wantErr: true,
		},
		{
			name:    "parse empty input",
			input:   []byte{},
			want:    nil,
			wantErr: true,
		},
		{
			name:  "parse bulk string",
			input: []byte("$4\r\nPING\r\n"),
			want: &Message{
				Type:    '$',
				Content: []string{"PING"},
			},
			wantErr: false,
		},
		{
			name:    "parse invalid array length",
			input:   []byte("*invalid\r\n"),
			want:    nil,
			wantErr: true,
		},
		{
			name:    "parse incomplete array",
			input:   []byte("*2\r\n$4\r\nPING\r\n"),
			want:    nil,
			wantErr: true,
		},
		{
			name:    "parse invalid bulk string",
			input:   []byte("$invalid\r\n"),
			want:    nil,
			wantErr: true,
		},
	}

	parser := NewParser()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parser.Parse(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Parse() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestWriter_WriteSimpleString(t *testing.T) {
	w := NewWriter()
	got := w.WriteSimpleString("OK")
	want := []byte("+OK\r\n")
	if !reflect.DeepEqual(got, want) {
		t.Errorf("WriteSimpleString() = %v, want %v", got, want)
	}
}

func TestWriter_WriteBulkString(t *testing.T) {
	w := NewWriter()
	got := w.WriteBulkString("hello")
	want := []byte("$5\r\nhello\r\n")
	if !reflect.DeepEqual(got, want) {
		t.Errorf("WriteBulkString() = %v, want %v", got, want)
	}
}

func TestWriter_WriteError(t *testing.T) {
	w := NewWriter()
	got := w.WriteError("Error message")
	want := []byte("-Error message\r\n")
	if !reflect.DeepEqual(got, want) {
		t.Errorf("WriteError() = %v, want %v", got, want)
	}
}

func TestWriter_WriteNullBulk(t *testing.T) {
	w := NewWriter()
	got := w.WriteNullBulk()
	want := []byte("$-1\r\n")
	if !reflect.DeepEqual(got, want) {
		t.Errorf("WriteNullBulk() = %v, want %v", got, want)
	}
}

func TestWriter_WriteArray(t *testing.T) {
	tests := []struct {
		name  string
		input []string
		want  []byte
	}{
		{
			name:  "empty array",
			input: []string{},
			want:  []byte("*0\r\n"),
		},
		{
			name:  "single element",
			input: []string{"hello"},
			want:  []byte("*1\r\n$5\r\nhello\r\n"),
		},
		{
			name:  "multiple elements",
			input: []string{"hello", "world"},
			want:  []byte("*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n"),
		},
	}

	w := NewWriter()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := w.WriteArray(tt.input)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("WriteArray() = %v, want %v", got, tt.want)
			}
		})
	}
}
