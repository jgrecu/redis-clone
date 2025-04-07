package config

import (
	"bufio"
	"github.com/jgrecu/redis-clone/app/resp"
	"net"
	"sync"
)

type Node struct {
	Conn    net.Conn
	Reader  *resp.RespReader
	offset  int
	id      string
	mu      sync.Mutex
	AckChan chan int
}

func NewNode(conn net.Conn) *Node {
	return &Node{
		Conn:    conn,
		Reader:  resp.NewRespReader(bufio.NewReader(conn)),
		offset:  0,
		id:      conn.RemoteAddr().String(),
		mu:      sync.Mutex{},
		AckChan: make(chan int),
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
