package server

import (
	"os"
	"testing"

	"github.com/jgrecu/redis-clone/pkg/config"
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
	if server.replicas == nil {
		t.Error("Server replicas map not initialized")
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
