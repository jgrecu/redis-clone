package config

import (
	"bufio"
	"github.com/jgrecu/redis-clone/app/resp"
	"net"
)

type Node struct {
	Conn   net.Conn
	Reader *resp.RespReader
	Offset int
	id     string
}

func NewNode(conn net.Conn) Node {
	return Node{
		Conn:   conn,
		Reader: resp.NewRespReader(bufio.NewReader(conn)),
		Offset: 0,
		id:     conn.RemoteAddr().String(),
	}
}

func (n Node) Close() {
	n.Conn.Close()
}

func (n Node) Read() (resp.RESP, error) {
	return n.Reader.Read()
}

func (n Node) ReadRDB() (resp.RESP, error) {
	return n.Reader.ReadRDB()
}

func (n Node) Write(data []byte) (int, error) {
	return n.Conn.Write(data)
}
