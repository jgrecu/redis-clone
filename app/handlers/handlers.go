package handlers

import (
	"fmt"
	"github.com/jgrecu/redis-clone/app/config"
	"github.com/jgrecu/redis-clone/app/resp"
	"github.com/jgrecu/redis-clone/app/structures"
	"log"
	"net"
	"strconv"
	"strings"
)

type CommandHandler func([]resp.RESP) []byte

var handlers = map[string]func([]resp.RESP) []byte{
	"GET":      structures.Get,
	"SET":      structures.Set,
	"CONFIG":   config.GetConfigHandler,
	"PING":     ping,
	"ECHO":     echo,
	"KEYS":     structures.Keys,
	"INFO":     info,
	"PSYNC":    psync,
	"REPLCONF": replconf,
	"WAIT":     wait,
	"TYPE":     structures.Typ,
	"XADD":     structures.Xadd,
	"XRANGE":   structures.XRange,
	"XREAD":    structures.XRead,
}

func Handle(conn net.Conn, args []resp.RESP) error {
	command := strings.ToUpper(args[0].Bulk)
	log.Println("Received command: ", command, " with args: ", args)
	handler, ok := handlers[command]
	if !ok {
		handler = notFound
	}

	if command == "REPLCONF" && strings.ToUpper(args[1].Bulk) == "ACK" {
		log.Println("warning: recieved replica ack from client handler ")
		offset, _ := strconv.Atoi(args[2].Bulk)
		log.Println("offset: ", offset)
		go config.Replica(conn).ReceiveAck(offset)
		return nil
	}

	conn.Write(handler(args[1:]))

	if command == "PSYNC" {
		config.AddReplica(conn)
		return fmt.Errorf("PSYNC")
	}

	// Propagate the command to all replicas
	if isWriteCommand(command) {
		log.Println("Propagating command to replicas: ", string(resp.Array(args...).Marshal()))
		for i := 0; i < len(config.Get().Replicas); i++ {
			replica := config.Get().Replicas[i]
			writtenSize, _ := replica.Write(resp.Array(args...).Marshal())
			//if err != nil {
			//	// disconnected
			//	config.RemoveReplica(replica)
			//	i--
			//	return
			//}
			replica.AddOffset(writtenSize)
		}

	}

	return nil
}

func HandleMaster(conn net.Conn, args []resp.RESP) {
	command := strings.ToUpper(args[0].Bulk)
	handler, ok := handlers[command]
	if !ok {
		handler = notFound
	}

	data := handler(args[1:])
	if command == "REPLCONF" && strings.ToUpper(args[1].Bulk) == "GETACK" {
		conn.Write(data)
	}
}

func ping(params []resp.RESP) []byte {
	return resp.String("PONG").Marshal()
}

func echo(params []resp.RESP) []byte {
	return resp.String(params[0].Bulk).Marshal()
}

func notFound(params []resp.RESP) []byte {
	return resp.Error("Command not found").Marshal()
}

func isWriteCommand(command string) bool {
	return command == "SET"
}
