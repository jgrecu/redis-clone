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
