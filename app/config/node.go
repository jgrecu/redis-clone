package config

import (
	"bufio"
	"github.com/jgrecu/redis-clone/app/resp"
	"net"
	"sync"
)

type Node struct {
	Conn     net.Conn
	Reader   *resp.RespReader
	offset   int
	id       string
	mu       sync.Mutex
	AckChans []chan int
}

func NewNode(conn net.Conn) *Node {
	return &Node{
		Conn:     conn,
		Reader:   resp.NewRespReader(bufio.NewReader(conn)),
		offset:   0,
		id:       conn.RemoteAddr().String(),
		mu:       sync.Mutex{},
		AckChans: make([]chan int, 0),
	}
}

func (n *Node) Close() {
	n.Conn.Close()
}

func (n *Node) AddOffset(offset int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.offset += offset
}

func (n *Node) GetOffset() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.offset
}

func (n *Node) Read() (resp.RESP, error) {
	return n.Reader.Read()
}

func (n *Node) ReadRDB() (resp.RESP, error) {
	return n.Reader.ReadRDB()
}

func (n *Node) Write(data []byte) (int, error) {
	return n.Conn.Write(data)
}

func (n *Node) SendAck(ack chan int) (int, error) {
	n.mu.Lock()
	n.AckChans = append(n.AckChans, ack)
	n.mu.Unlock()
	s, err := n.Conn.Write(
		resp.Command("REPLCONF", "GETACK", "*").Marshal(),
	)
	n.AddOffset(s)
	return s, err
}

func (n *Node) ReceiveAck(offset int) {
	n.mu.Lock()
	defer n.mu.Unlock()

	chann := n.AckChans[0]
	n.AckChans = n.AckChans[1:]
	chann <- offset
}
