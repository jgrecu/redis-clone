package main

import (
	"github.com/jgrecu/redis-clone/app/config"
	"github.com/jgrecu/redis-clone/app/resp"
	"net"
)

func handShake(link string) error {

	send("PING")
	send("REPLCONF", "listening-port", config.Get("port"))
	send("REPLCONF", "capa", "psync2")

	return nil
}

func send(commands ...string) error {
	link := config.Get("master_host") + ":" + config.Get("master_port")
	conn, err := net.Dial("tcp", link)
	if err != nil {
		return err
	}
	defer conn.Close()

	// send commands
	commandsArray := make([]resp.RESP, len(commands))
	for i, command := range commands {
		commandsArray[i] = resp.RESP{
			Type: "bulk",
			Bulk: command,
		}
	}

	message, err := resp.RESP{
		Type:  "array",
		Array: commandsArray,
	}.Marshal()

	if err != nil {
		return err
	}

	conn.Write(message)
	return nil
}
