// Package server provides Redis server implementation
package server

import (
	"fmt"
	"net"
)

// ServerInterface defines the main server operations
type ServerInterface interface {
	// Start starts the server and begins listening for connections
	Start() error
	// Stop gracefully stops the server
	Stop() error
	// HandleConnection handles a new client connection
	HandleConnection(conn net.Conn)
}

// CommandHandler defines the interface for command execution
type CommandHandler interface {
	// Execute executes the command with given arguments
	Execute(args []string) error
	// RequiredArgs returns the number of required arguments
	RequiredArgs() int
}

// Storage defines the interface for data storage operations
type Storage interface {
	// Get retrieves a value by key
	Get(key string) (interface{}, bool)
	// Set stores a value with key
	Set(key string, value interface{}, expiry int64) error
	// Delete removes a key
	Delete(key string) bool
}

// Persistence defines the interface for data persistence operations
type Persistence interface {
	// Save persists the current state
	Save() error
	// Load loads the persisted state
	Load() error
}

// ReplicationManager defines the interface for replication operations
type ReplicationManager interface {
	// AddReplica adds a new replica
	AddReplica(conn net.Conn) error
	// RemoveReplica removes a replica
	RemoveReplica(conn net.Conn)
	// PropagateCommand propagates a command to all replicas
	PropagateCommand(cmd []string) error
	// SetMaster sets the master connection
	SetMaster(conn net.Conn)
	// GetMaster returns the current master connection
	GetMaster() net.Conn
	// GetReplicaCount returns the number of connected replicas
	GetReplicaCount() int
	// DisconnectAll closes all connections
	DisconnectAll()
	// HasReplica checks if the given connection is a replica
	HasReplica(conn net.Conn) bool
}

// Custom error types
type (
	// ErrInvalidCommand represents an invalid command error
	ErrInvalidCommand struct {
		Command string
	}

	// ErrInvalidArgCount represents an invalid argument count error
	ErrInvalidArgCount struct {
		Expected int
		Got      int
	}

	// ErrReplication represents a replication-related error
	ErrReplication struct {
		Operation string
		Err       error
	}
)

// Error implementations
func (e *ErrInvalidCommand) Error() string {
	return "invalid command: " + e.Command
}

func (e *ErrInvalidArgCount) Error() string {
	return fmt.Sprintf("invalid argument count: expected %d, got %d", e.Expected, e.Got)
}

func (e *ErrReplication) Error() string {
	return "replication error during " + e.Operation + ": " + e.Err.Error()
}
