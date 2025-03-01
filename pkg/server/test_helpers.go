package server

import (
	"fmt"
	"net"
	"sync"
	"time"
)

// mockConn implements net.Conn for testing with enhanced functionality for tracking writes
type mockConn struct {
	closed      bool
	writeFail   bool
	writtenData [][]byte
	mu          sync.Mutex
}

func newMockConn() *mockConn {
	return &mockConn{
		writtenData: make([][]byte, 0),
	}
}

func (m *mockConn) Read(b []byte) (n int, err error) {
	return 0, nil
}

func (m *mockConn) Write(b []byte) (n int, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.writeFail {
		return 0, fmt.Errorf("write failed")
	}
	data := make([]byte, len(b))
	copy(data, b)
	m.writtenData = append(m.writtenData, data)
	return len(b), nil
}

func (m *mockConn) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

func (m *mockConn) LocalAddr() net.Addr                { return nil }
func (m *mockConn) RemoteAddr() net.Addr               { return nil }
func (m *mockConn) SetDeadline(t time.Time) error      { return nil }
func (m *mockConn) SetReadDeadline(t time.Time) error  { return nil }
func (m *mockConn) SetWriteDeadline(t time.Time) error { return nil }

// getWrittenCommands returns the commands that were written to this connection
func (m *mockConn) getWrittenCommands() [][]byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([][]byte, len(m.writtenData))
	copy(result, m.writtenData)
	return result
}

// isClosed returns whether the connection has been closed
func (m *mockConn) isClosed() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closed
}

// setWriteFail sets whether writes should fail
func (m *mockConn) setWriteFail(fail bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.writeFail = fail
}