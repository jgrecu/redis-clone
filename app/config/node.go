package config

import (
	"bufio"
	"github.com/jgrecu/redis-clone/app/resp"
	"net"
)

type Node struct {
	Conn   net.Conn
	Reader *resp.RespReader
}

func NewNode(conn net.Conn) Node {
	return Node{
		Conn:   conn,
		Reader: resp.NewRespReader(bufio.NewReader(conn)),
	}
}

func (n Node) Close() {
	n.Conn.Close()
}

func (n Node) Send(commands ...string) error {
	commandsArray := make([]resp.RESP, len(commands))
	for i, command := range commands {
		commandsArray[i] = resp.Bulk(command)
	}

	message := resp.Array(commandsArray...).Marshal()

	n.Conn.Write(message)
	return nil
}

func (n Node) Read() (resp.RESP, error) {
	return n.Reader.Read()
}

func (n Node) ReadRDB() (resp.RESP, error) {
	return n.Reader.ReadRDB()
}

func (n Node) Write(data []byte) error {
	n.Conn.Write(data)
	return nil
}
