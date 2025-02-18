package server

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/jgrecu/redis-clone/pkg/config"
	"github.com/jgrecu/redis-clone/pkg/resp"
)

func startMasterServer(t *testing.T, port uint, expectedMsg string) chan bool {
	msgReceived := make(chan bool)

	go func() {
		l, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
		if err != nil {
			t.Errorf("Failed to start master server: %v", err)
			msgReceived <- false
			return
		}
		defer l.Close()

		conn, err := l.Accept()
		if err != nil {
			t.Errorf("Failed to accept connection: %v", err)
			msgReceived <- false
			return
		}
		defer conn.Close()

		buf := make([]byte, 512)
		n, err := conn.Read(buf)
		if err != nil {
			t.Errorf("Failed to read from connection: %v", err)
			msgReceived <- false
			return
		}

		parser := resp.NewParser()
		msg, err := parser.Parse(buf[:n])
		if err != nil {
			t.Errorf("Failed to parse message: %v", err)
			msgReceived <- false
			return
		}

		if len(msg.Content) == 1 && msg.Content[0] == expectedMsg {
			msgReceived <- true
		} else {
			t.Errorf("Expected %s command, got %v", expectedMsg, msg.Content)
			msgReceived <- false
		}
	}()

	return msgReceived
}

func TestReplicationHandshake(t *testing.T) {
	tests := []struct {
		name        string
		masterPort  uint
		replicaPort uint
		expectedMsg string
		shouldFail  bool
	}{
		{
			name:        "Successful PING handshake",
			masterPort:  6379,
			replicaPort: 6380,
			expectedMsg: "PING",
			shouldFail:  false,
		},
		{
			name:        "Connection to non-existent master",
			masterPort:  6381, // Port where no master is running
			replicaPort: 6382,
			expectedMsg: "PING",
			shouldFail:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var msgReceived chan bool
			if !tt.shouldFail {
				msgReceived = startMasterServer(t, tt.masterPort, tt.expectedMsg)
				// Give master server time to start
				time.Sleep(100 * time.Millisecond)
			}

			replicaCfg := config.NewConfig()
			replicaCfg.Port = tt.replicaPort
			replicaCfg.Role = "slave"
			replicaCfg.MasterHost = "localhost"
			replicaCfg.MasterPort = tt.masterPort

			replica := NewServer(replicaCfg)

			err := replica.connectToMaster()

			if tt.shouldFail {
				if err == nil {
					t.Error("Expected connection to fail, but it succeeded")
				}
				return
			}

			if err != nil {
				t.Fatalf("Failed to connect to master: %v", err)
			}

			// Wait for command with timeout
			select {
			case success := <-msgReceived:
				if !success {
					t.Error("Command verification failed")
				}
			case <-time.After(2 * time.Second):
				t.Error("Timeout waiting for command")
			}
		})
	}
}