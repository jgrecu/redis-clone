package config

import (
    "bufio"
    "github.com/jgrecu/redis-clone/app/resp"
    "log"
    "net"
    "sync"
)

type Node struct {
    Conn    net.Conn
    Reader  *resp.RespReader
    offset  int
    id      string
    mu      sync.Mutex
    AckChan []chan<- int
}

func NewNode(conn net.Conn) *Node {
    return &Node{
        Conn:    conn,
        Reader:  resp.NewRespReader(bufio.NewReader(conn)),
        offset:  0,
        id:      conn.RemoteAddr().String(),
        mu:      sync.Mutex{},
        AckChan: make([]chan<- int, 1),
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

func (n *Node) SendAck(ack chan<- int) (int, error) {
    n.mu.Lock()
    n.AckChan = append(n.AckChan, ack)
    defer n.mu.Unlock()
    log.Println("Sending from replica : ", n.id)
    return n.Conn.Write(
        resp.Command("REPLCONF", "GETACK", "*").Marshal(),
    )
}

func (n *Node) ReceiveAck(offset int) {
    log.Println("Received ack from replica : ", n.id)
    n.mu.Lock()
    n.mu.Unlock()
    if len(n.AckChan) == 0 {
        return
    }

    n.AckChan[0] <- offset
    n.AckChan = n.AckChan[1:]

    log.Println("Receive func: Ack sent through the channel")
}
