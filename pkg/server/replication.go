// Package server provides Redis server implementation
package server

import (
	"fmt"
	"net"
	"sync"
)

// ReplicationManagerImpl implements the ReplicationManager interface providing thread-safe
// management of master-replica relationships and command propagation in a Redis-like system.
// It handles:
// - Master connection management
// - Replica connections tracking
// - Command propagation to replicas
// - Thread-safe operations on the replication state
type ReplicationManagerImpl struct {
	replicas    map[net.Conn]bool // Active replica connections
	masterConn  net.Conn         // Current master connection (if any)
	mu          sync.RWMutex     // Mutex for thread-safe operations
	writer      CommandWriter    // Interface for writing commands in RESP format
}

// CommandWriter defines the interface for writing commands in RESP format.
// This interface abstracts the RESP protocol implementation details from the replication logic.
type CommandWriter interface {
	// WriteArray converts a string array command into RESP format bytes
	WriteArray(cmd []string) []byte
}

// NewReplicationManager creates a new ReplicationManager instance with the provided command writer.
// It initializes an empty replica set and sets up the command writer for RESP protocol handling.
func NewReplicationManager(writer CommandWriter) *ReplicationManagerImpl {
	return &ReplicationManagerImpl{
		replicas: make(map[net.Conn]bool),
		writer:   writer,
	}
}

// AddReplica adds a new replica connection to the replication set.
// It performs thread-safe operations and validates the connection.
// Returns an error if the connection is nil or if there's an issue adding the replica.
func (rm *ReplicationManagerImpl) AddReplica(conn net.Conn) error {
	if conn == nil {
		return &ErrReplication{
			Operation: "add replica",
			Err:       fmt.Errorf("connection is nil"),
		}
	}

	rm.mu.Lock()
	defer rm.mu.Unlock()
	rm.replicas[conn] = true
	return nil
}

// RemoveReplica safely removes a replica connection from the replication set.
// It handles nil connections gracefully and performs thread-safe removal.
// The connection is closed before being removed from the set.
func (rm *ReplicationManagerImpl) RemoveReplica(conn net.Conn) {
	if conn == nil {
		return
	}

	rm.mu.Lock()
	defer rm.mu.Unlock()
	if _, exists := rm.replicas[conn]; exists {
		conn.Close()
		delete(rm.replicas, conn)
	}
}

// PropagateCommand sends a command to all connected replicas in RESP format.
// It handles failures gracefully by removing failed replicas and continuing with remaining ones.
// Returns an error if the command is empty or if there are issues with command propagation.
func (rm *ReplicationManagerImpl) PropagateCommand(cmd []string) error {
	if len(cmd) == 0 {
		return &ErrReplication{
			Operation: "propagate command",
			Err:       fmt.Errorf("empty command"),
		}
	}

	message := rm.writer.WriteArray(cmd)

	rm.mu.Lock() // Use Lock instead of RLock since we might modify the map
	defer rm.mu.Unlock()

	var lastErr error
	failedReplicas := make([]net.Conn, 0)

	// First try to write to all replicas
	for conn := range rm.replicas {
		if _, err := conn.Write(message); err != nil {
			failedReplicas = append(failedReplicas, conn)
			lastErr = &ErrReplication{
				Operation: "write to replica",
				Err:       err,
			}
		}
	}

	// Then remove and close failed connections
	for _, conn := range failedReplicas {
		conn.Close()
		delete(rm.replicas, conn)
	}

	return lastErr
}

// SetMaster sets the master connection in a thread-safe manner.
// This method is used when the instance needs to connect to a master server.
// The connection can be nil to indicate that this instance is no longer a replica.
func (rm *ReplicationManagerImpl) SetMaster(conn net.Conn) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	rm.masterConn = conn
}

// GetMaster returns the current master connection in a thread-safe manner.
// Returns nil if this instance is not connected to a master (i.e., it's a master itself).
// This method is useful for checking the current replication status.
func (rm *ReplicationManagerImpl) GetMaster() net.Conn {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	return rm.masterConn
}

// GetReplicaCount returns the current number of connected replicas.
// This method is thread-safe and is useful for monitoring and logging purposes.
// Returns 0 if there are no connected replicas.
func (rm *ReplicationManagerImpl) GetReplicaCount() int {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	return len(rm.replicas)
}

// DisconnectAll closes all connections (both master and replicas) in a thread-safe manner.
// This method should be called during shutdown or when resetting the replication state.
// It ensures all connections are properly closed to prevent resource leaks.
func (rm *ReplicationManagerImpl) DisconnectAll() {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	for conn := range rm.replicas {
		conn.Close()
	}
	rm.replicas = make(map[net.Conn]bool)

	if rm.masterConn != nil {
		rm.masterConn.Close()
		rm.masterConn = nil
	}
}

// HasReplica checks if the given connection is a replica
// Returns false if the connection is nil or not in the replica set
func (rm *ReplicationManagerImpl) HasReplica(conn net.Conn) bool {
	if conn == nil {
		return false
	}
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	return rm.replicas[conn]
}
