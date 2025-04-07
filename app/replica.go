package main

import (
	"bufio"
	"fmt"
	"github.com/jgrecu/redis-clone/app/config"
	"github.com/jgrecu/redis-clone/app/resp"
	"net"
)

func handShake() error {
	link := config.Get("master_host") + ":" + config.Get("master_port")
	conn, err := net.Dial("tcp", link)
	if err != nil {
		return err
	}
	// defer conn.Close()

	reader := resp.NewRespReader(bufio.NewReader(conn))

	send(conn, "PING")
	value, _ := reader.Read()

	send(conn, "REPLCONF", "listening-port", config.Get("port"))
	value, _ = reader.Read()

	send(conn, "REPLCONF", "capa", "psync2")
	value, _ = reader.Read()

	send(conn, "PSYNC", "?", "-1")
	value, _ = reader.Read()
	fmt.Printf("Received response from master: %v\n", value.Bulk)

	return nil
}

func send(conn net.Conn, commands ...string) error {
	commandsArray := make([]resp.RESP, len(commands))
	for i, command := range commands {
		commandsArray[i] = resp.Bulk(command)
	}

	message, err := resp.Array(commandsArray).Marshal()

	if err != nil {
		return err
	}

	conn.Write(message)
	return nil
}
