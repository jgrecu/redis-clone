package handlers

import (
	"fmt"
	"github.com/jgrecu/redis-clone/app/config"
	"github.com/jgrecu/redis-clone/app/resp"
	"github.com/jgrecu/redis-clone/app/structures"
	"github.com/jgrecu/redis-clone/app/transactions"
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
	"PSYNC":    Psync,
	"REPLCONF": Replconf,
	"WAIT":     Wait,
	"TYPE":     structures.Typ,
	"XADD":     structures.Xadd,
	"XRANGE":   structures.XRange,
	"XREAD":    structures.XRead,
	"INCR":     transactions.Incr,
	"MULTI":    transactions.Multi,
	"EXEC":     transactions.Exec,
}

func Handle(conn net.Conn, args []resp.RESP) error {
	command := strings.ToUpper(args[0].Bulk)
	handler, ok := handlers[command]
	if !ok {
		handler = notFound
	}

	if command == "REPLCONF" && strings.ToUpper(args[1].Bulk) == "ACK" {
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
		for i := 0; i < len(config.Get().Replicas); i++ {
			replica := config.Get().Replicas[i]
			writtenSize, _ := replica.Write(resp.Array(args...).Marshal())
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
