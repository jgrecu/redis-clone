package respConnection

import (
	"bytes"
	"io"
	"net"
	"testing"
	"time"
)

// MockConn implements the net.Conn interface for testing
type MockConn struct {
	ReadData  []byte
	WriteData bytes.Buffer
	Closed    bool
}

func (m *MockConn) Read(b []byte) (n int, err error) {
	if len(m.ReadData) == 0 {
		return 0, io.EOF
	}
	n = copy(b, m.ReadData)
	m.ReadData = m.ReadData[n:]
	return n, nil
}

func (m *MockConn) Write(b []byte) (n int, err error) {
	return m.WriteData.Write(b)
}

func (m *MockConn) Close() error {
	m.Closed = true
	return nil
}

func (m *MockConn) LocalAddr() net.Addr                { return &net.TCPAddr{} }
func (m *MockConn) RemoteAddr() net.Addr               { return &net.TCPAddr{} }
func (m *MockConn) SetDeadline(t time.Time) error      { return nil }
func (m *MockConn) SetReadDeadline(t time.Time) error  { return nil }
func (m *MockConn) SetWriteDeadline(t time.Time) error { return nil }

func TestNewRespConn(t *testing.T) {
	mockConn := &MockConn{}
	conn := NewRespConn(mockConn)

	if conn.Conn != mockConn {
		t.Errorf("NewRespConn() did not set Conn correctly")
	}

	if conn.offset != 0 {
		t.Errorf("NewRespConn() did not initialize offset to 0")
	}

	if conn.id != mockConn.RemoteAddr().String() {
		t.Errorf("NewRespConn() did not set id correctly")
	}

	if len(conn.AckChans) != 0 {
		t.Errorf("NewRespConn() did not initialize AckChans correctly")
	}

	if conn.TxQueue != nil {
		t.Errorf("NewRespConn() did not initialize TxQueue correctly")
	}
}

func TestRespConn_Close(t *testing.T) {
	mockConn := &MockConn{}
	conn := NewRespConn(mockConn)

	conn.Close()

	if !mockConn.Closed {
		t.Errorf("Close() did not close the underlying connection")
	}
}

func TestRespConn_Id(t *testing.T) {
	mockConn := &MockConn{}
	conn := NewRespConn(mockConn)

	if conn.Id() != mockConn.RemoteAddr().String() {
		t.Errorf("Id() did not return the correct id")
	}
}

func TestRespConn_Write(t *testing.T) {
	mockConn := &MockConn{}
	conn := NewRespConn(mockConn)

	data := []byte("test data")
	n, err := conn.Write(data)

	if err != nil {
		t.Errorf("Write() returned an error: %v", err)
	}

	if n != len(data) {
		t.Errorf("Write() returned wrong number of bytes: got %v, want %v", n, len(data))
	}

	if !bytes.Equal(mockConn.WriteData.Bytes(), data) {
		t.Errorf("Write() did not write the correct data: got %v, want %v", mockConn.WriteData.Bytes(), data)
	}
}

func TestRespConn_AddOffset(t *testing.T) {
	conn := NewRespConn(&MockConn{})

	initialOffset := conn.GetOffset()
	conn.AddOffset(10)
	newOffset := conn.GetOffset()

	if newOffset != initialOffset+10 {
		t.Errorf("AddOffset() did not add the offset correctly: got %v, want %v", newOffset, initialOffset+10)
	}
}

func TestRespConn_GetOffset(t *testing.T) {
	conn := NewRespConn(&MockConn{})

	initialOffset := conn.GetOffset()
	if initialOffset != 0 {
		t.Errorf("GetOffset() did not return the correct initial offset: got %v, want 0", initialOffset)
	}

	conn.AddOffset(10)
	newOffset := conn.GetOffset()
	if newOffset != 10 {
		t.Errorf("GetOffset() did not return the correct offset after adding: got %v, want 10", newOffset)
	}
}

func TestIsWriteCommand(t *testing.T) {
	tests := []struct {
		name     string
		command  string
		expected bool
	}{
		{
			name:     "SET command",
			command:  "SET",
			expected: true,
		},
		{
			name:     "DEL command",
			command:  "DEL",
			expected: true,
		},
		{
			name:     "GET command",
			command:  "GET",
			expected: false,
		},
		{
			name:     "PING command",
			command:  "PING",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isWriteCommand(tt.command)
			if result != tt.expected {
				t.Errorf("isWriteCommand(%s) = %v, want %v", tt.command, result, tt.expected)
			}
		})
	}
}
