package respConnection

import (
	"github.com/jgrecu/redis-clone/app/resp"
	"sync"
	"time"
)

type ReplicaManager struct {
	mu       sync.RWMutex
	Replicas map[string]*RespConn
}

var replicaManager *ReplicaManager = &ReplicaManager{
	Replicas: make(map[string]*RespConn),
}

func GetReplicaManager() *ReplicaManager {
	return replicaManager
}

func (r *ReplicaManager) AddReplica(conn *RespConn) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Replicas[conn.Id()] = conn
}

func (r *ReplicaManager) RemoveReplica(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.Replicas, id)
}

func (r *ReplicaManager) GetReplica(id string) *RespConn {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.Replicas[id]
}

func (r *ReplicaManager) GetReplicas() []*RespConn {
	r.mu.RLock()
	defer r.mu.RUnlock()
	replicas := make([]*RespConn, 0, len(r.Replicas))
	for _, replica := range r.Replicas {
		replicas = append(replicas, replica)
	}
	return replicas
}

func (r *ReplicaManager) SendAck(timeout int, maxCount int) int {
	r.mu.RLock()
	ackChan := make(chan int)
	count := 0

	for _, replica := range r.Replicas {
		if replica.GetOffset() > 0 {
			go replica.SendAck(ackChan)
		} else {
			count++
		}
	}
	r.mu.RUnlock()

loop:
	for count < maxCount {
		select {
		case <-ackChan:
			count++
		case <-time.After(time.Duration(timeout) * time.Millisecond):
			break loop
		}
	}

	r.ClearAckChans(ackChan)

	return count
}

func (r *ReplicaManager) ClearAckChans(ackChan chan int) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, replica := range r.Replicas {
		for i := 0; i < len(replica.AckChans); i++ {
			if replica.AckChans[i] == ackChan {
				replica.AckChans = append(replica.AckChans[:i], replica.AckChans[i+1:]...)
				break
			}
		}
	}
}

func (r *ReplicaManager) PropagateCommand(args []resp.RESP) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, replica := range r.Replicas {
		writtenSize, _ := replica.Write(resp.Array(args...).Marshal())
		replica.AddOffset(writtenSize)
	}
}
