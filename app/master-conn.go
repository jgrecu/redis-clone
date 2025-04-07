package main

import (
	"bufio"
	"fmt"
	"github.com/jgrecu/redis-clone/app/config"
	"github.com/jgrecu/redis-clone/app/handlers"
	"github.com/jgrecu/redis-clone/app/resp"
	"net"
	"strings"
)

type Master struct {
	masterAddress string
	conn          net.Conn
	reader        *resp.RespReader
}

func NewMaster() (*Master, error) {
	address := config.Get("master_host") + ":" + config.Get("master_port")
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return nil, err
	}
	return &Master{
		masterAddress: address,
		conn:          conn,
		reader:        resp.NewRespReader(bufio.NewReader(conn)),
	}, nil
}

func (m *Master) HandShake() error {
	m.send("PING")
	value, _ := m.reader.Read()

	m.send("REPLCONF", "listening-port", config.Get("port"))
	value, _ = m.reader.Read()

	m.send("REPLCONF", "capa", "psync2")
	value, _ = m.reader.Read()

	m.send("PSYNC", "?", "-1")
	value, _ = m.reader.Read()

	fmt.Printf("Received response from master: %v\n", value.Bulk)

	return nil
}

func (m *Master) Listen(errChan chan error) {
	for {
		value, err := m.reader.Read()
		if err != nil {
			errChan <- err
			continue
		}

		if value.Type == "array" && len(value.Array) > 0 {
			command := strings.ToUpper(value.Array[0].Bulk)
			handler := handlers.GetHandler(command)
			m.conn.Write(handler(value.Array[1:]))
		} else {
			errChan <- fmt.Errorf("invalid command")
		}
	}
}

func (m *Master) Close() {
	m.conn.Close()
}

func (m *Master) send(commands ...string) error {
	commandsArray := make([]resp.RESP, len(commands))
	for i, command := range commands {
		commandsArray[i] = resp.Bulk(command)
	}

	message := resp.Array(commandsArray...).Marshal()

	m.conn.Write(message)
	return nil
}
