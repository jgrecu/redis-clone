package server

import (
	"sync"
	"testing"
)

// mockCommandWriter implements CommandWriter for testing
type mockCommandWriter struct{}

func (m *mockCommandWriter) WriteArray(cmd []string) []byte {
	return []byte(cmd[0])
}

func TestNewReplicationManager(t *testing.T) {
	writer := &mockCommandWriter{}
	rm := NewReplicationManager(writer)

	if rm == nil {
		t.Fatal("Expected non-nil ReplicationManager")
	}
	if rm.writer != writer {
		t.Error("Expected writer to be set correctly")
	}
	if rm.replicas == nil {
		t.Error("Expected replicas map to be initialized")
	}
	if len(rm.replicas) != 0 {
		t.Error("Expected empty replicas map")
	}
}

func TestReplicationManager_AddRemoveReplica(t *testing.T) {
	rm := NewReplicationManager(&mockCommandWriter{})
	conn := newMockConn()

	t.Log("[DEBUG_LOG] Testing replica addition")
	if err := rm.AddReplica(conn); err != nil {
		t.Errorf("Failed to add replica: %v", err)
	}
	if rm.GetReplicaCount() != 1 {
		t.Errorf("Expected 1 replica, got %d", rm.GetReplicaCount())
	}

	t.Log("[DEBUG_LOG] Testing nil replica addition")
	if err := rm.AddReplica(nil); err == nil {
		t.Error("Expected error when adding nil replica")
	}

	t.Log("[DEBUG_LOG] Testing replica removal")
	rm.RemoveReplica(conn)
	if rm.GetReplicaCount() != 0 {
		t.Errorf("Expected 0 replicas after removal, got %d", rm.GetReplicaCount())
	}
	if !conn.isClosed() {
		t.Error("Expected connection to be closed after removal")
	}

	// Test removing nil replica (should not panic)
	rm.RemoveReplica(nil)
}

func TestReplicationManager_PropagateCommand(t *testing.T) {
	rm := NewReplicationManager(&mockCommandWriter{})
	conn1 := newMockConn()
	conn2 := newMockConn()

	t.Log("[DEBUG_LOG] Testing command propagation to multiple replicas")
	rm.AddReplica(conn1)
	rm.AddReplica(conn2)

	// Test propagating command
	cmd := []string{"SET", "key", "value"}
	if err := rm.PropagateCommand(cmd); err != nil {
		t.Errorf("Failed to propagate command: %v", err)
	}

	// Verify commands were written to both replicas
	if len(conn1.getWrittenCommands()) == 0 {
		t.Error("Expected command to be written to first replica")
	}
	if len(conn2.getWrittenCommands()) == 0 {
		t.Error("Expected command to be written to second replica")
	}

	t.Log("[DEBUG_LOG] Testing empty command propagation")
	if err := rm.PropagateCommand([]string{}); err == nil {
		t.Error("Expected error when propagating empty command")
	}

	// Test write failure
	t.Log("[DEBUG_LOG] Testing write failure handling")
	conn1.setWriteFail(true)
	if err := rm.PropagateCommand(cmd); err == nil {
		t.Error("Expected error when write fails")
	}
}

func TestReplicationManager_MasterConnection(t *testing.T) {
	rm := NewReplicationManager(&mockCommandWriter{})
	conn := newMockConn()

	t.Log("[DEBUG_LOG] Testing master connection management")
	rm.SetMaster(conn)
	if got := rm.GetMaster(); got != conn {
		t.Error("Expected master connection to be set correctly")
	}

	rm.SetMaster(nil)
	if got := rm.GetMaster(); got != nil {
		t.Error("Expected master connection to be cleared")
	}
}

func TestReplicationManager_DisconnectAll(t *testing.T) {
	rm := NewReplicationManager(&mockCommandWriter{})
	master := newMockConn()
	replica1 := newMockConn()
	replica2 := newMockConn()

	t.Log("[DEBUG_LOG] Testing disconnect all functionality")
	rm.SetMaster(master)
	rm.AddReplica(replica1)
	rm.AddReplica(replica2)

	rm.DisconnectAll()

	if !master.isClosed() {
		t.Error("Expected master connection to be closed")
	}
	if !replica1.isClosed() {
		t.Error("Expected replica1 connection to be closed")
	}
	if !replica2.isClosed() {
		t.Error("Expected replica2 connection to be closed")
	}
	if rm.GetReplicaCount() != 0 {
		t.Error("Expected all replicas to be removed")
	}
	if rm.GetMaster() != nil {
		t.Error("Expected master connection to be cleared")
	}
}

func TestReplicationManager_ThreadSafety(t *testing.T) {
	rm := NewReplicationManager(&mockCommandWriter{})
	var wg sync.WaitGroup
	iterations := 100

	t.Log("[DEBUG_LOG] Testing concurrent replication operations")

	// Create a set of test connections
	conns := make([]*mockConn, iterations)
	for i := 0; i < iterations; i++ {
		conns[i] = newMockConn()
	}

	// Concurrent addition and removal of replicas
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			rm.AddReplica(conns[i])
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			rm.RemoveReplica(conns[i])
		}
	}()

	// Concurrent master operations
	masterConn := newMockConn()
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			rm.SetMaster(masterConn)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			rm.GetMaster()
		}
	}()

	// Concurrent command propagation with connection management
	wg.Add(2)
	go func() {
		defer wg.Done()
		cmd := []string{"TEST"}
		for i := 0; i < iterations; i++ {
			rm.PropagateCommand(cmd)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			if i%2 == 0 {
				rm.AddReplica(newMockConn())
			} else {
				rm.DisconnectAll()
			}
		}
	}()

	wg.Wait()

	t.Log("[DEBUG_LOG] Verifying final state after concurrent operations")

	// Verify final state
	rm.DisconnectAll()
	if rm.GetReplicaCount() != 0 {
		t.Error("Expected all replicas to be removed after DisconnectAll")
	}
	if rm.GetMaster() != nil {
		t.Error("Expected master to be cleared after DisconnectAll")
	}

	// Verify all connections were properly closed
	for _, conn := range conns {
		if !conn.isClosed() {
			t.Error("Expected all test connections to be closed")
		}
	}
	if !masterConn.isClosed() {
		t.Error("Expected master connection to be closed")
	}
}
