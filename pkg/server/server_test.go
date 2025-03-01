package server

import (
	"os"
	"testing"

	"github.com/jgrecu/redis-clone/pkg/config"
	"github.com/jgrecu/redis-clone/pkg/resp"
)

func TestMain(m *testing.M) {
	// Run the tests
	code := m.Run()

	// Cleanup
	os.Remove("dump.rdb")

	// Exit with the test status code
	os.Exit(code)
}

// TestNewServer tests server initialization
func TestNewServer(t *testing.T) {
	cfg := config.NewConfig()
	server := NewServer(cfg)

	if server == nil {
		t.Fatal("NewServer returned nil")
	}

	// Check if all components are properly initialized
	if server.config != cfg {
		t.Error("Server config not properly initialized")
	}
	if server.store == nil {
		t.Error("Server store not initialized")
	}
	if server.parser == nil {
		t.Error("Server parser not initialized")
	}
	if server.writer == nil {
		t.Error("Server writer not initialized")
	}
	if server.commands == nil {
		t.Error("Server commands map not initialized")
	}
	if server.replication == nil {
		t.Error("Server replication manager not initialized")
	}
	if server.rdb == nil {
		t.Error("Server RDB not initialized")
	}
}

// TestCommandRegistration tests if all commands are properly registered and initialized
func TestCommandRegistration(t *testing.T) {
	cfg := config.NewConfig()
	server := NewServer(cfg)

	expectedCommands := []string{
		"PING", "ECHO", "SET", "GET", "CONFIG",
		"SAVE", "KEYS", "INFO", "REPLCONF", "PSYNC",
	}

	for _, cmd := range expectedCommands {
		t.Run(cmd, func(t *testing.T) {
			handler, exists := server.commands[cmd]
			if !exists {
				t.Errorf("Command %s not registered", cmd)
				return
			}
			if handler == nil {
				t.Errorf("Command %s has nil handler", cmd)
			}
		})
	}
}

// TestServerReplication tests the server's replication functionality
func TestServerReplication(t *testing.T) {
	cfg := config.NewConfig()
	server := NewServer(cfg)
	mockConn := newMockConn()

	t.Log("[DEBUG_LOG] Testing replica management")

	// Test adding replica
	if err := server.AddReplica(mockConn); err != nil {
		t.Errorf("Failed to add replica: %v", err)
	}
	if server.replication.GetReplicaCount() != 1 {
		t.Error("Failed to add replica to server")
	}

	// Test command propagation in master mode
	cfg.Role = "master"
	msg := &resp.Message{
		Type:    resp.ArrayPrefix,
		Length:  3,
		Content: []string{"SET", "key", "value"},
	}

	t.Log("[DEBUG_LOG] Testing command propagation in master mode")
	response, err := server.handleMessage(msg)
	if err != nil {
		t.Errorf("Failed to handle SET command: %v", err)
	}
	if response == nil {
		t.Error("Expected non-nil response from SET command")
	}

	// Verify the command was propagated to replica
	commands := mockConn.getWrittenCommands()
	if len(commands) == 0 {
		t.Error("Expected command to be propagated to replica")
	}

	// Test non-write command (should not be propagated)
	readMsg := &resp.Message{
		Type:    resp.ArrayPrefix,
		Length:  2,
		Content: []string{"GET", "key"},
	}

	t.Log("[DEBUG_LOG] Testing read command handling")
	commandCount := len(mockConn.getWrittenCommands())
	response, err = server.handleMessage(readMsg)
	if err != nil {
		t.Errorf("Failed to handle GET command: %v", err)
	}
	if response == nil {
		t.Error("Expected non-nil response from GET command")
	}
	if len(mockConn.getWrittenCommands()) != commandCount {
		t.Error("Read command should not be propagated to replicas")
	}

	// Test write failure handling
	t.Log("[DEBUG_LOG] Testing write failure handling")
	mockConn.setWriteFail(true)
	_, err = server.handleMessage(msg)
	if err == nil {
		t.Error("Expected error when replica write fails")
	} else {
		// Verify it's the expected error type
		if _, ok := err.(*ErrReplication); !ok {
			t.Errorf("Expected ErrReplication, got %T", err)
		}
	}

	// Verify the failed replica was removed
	if server.replication.GetReplicaCount() != 0 {
		t.Error("Failed replica should be automatically removed")
	}
	if !mockConn.isClosed() {
		t.Error("Failed connection should be closed")
	}

	// Reset connection for further tests
	mockConn = newMockConn()
	mockConn.setWriteFail(false)

	// Test explicit replica removal
	if err := server.AddReplica(mockConn); err != nil {
		t.Errorf("Failed to add new replica: %v", err)
	}
	server.RemoveReplica(mockConn)
	if server.replication.GetReplicaCount() != 0 {
		t.Error("Failed to remove replica from server")
	}
	if !mockConn.isClosed() {
		t.Error("Connection should be closed when removing replica")
	}

	// Test replica role
	cfg.Role = "slave"
	cfg.MasterHost = "localhost"
	cfg.MasterPort = 6379

	t.Log("[DEBUG_LOG] Testing replica mode connection")
	if err := server.connectToMaster(); err == nil {
		t.Error("Expected error when connecting to non-existent master")
	}
}
