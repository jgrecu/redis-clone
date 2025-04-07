package handlers

import (
	"github.com/jgrecu/redis-clone/app/config"
	"github.com/jgrecu/redis-clone/app/resp"
	"github.com/jgrecu/redis-clone/app/structures"
	"net"
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
}

func Handle(conn net.Conn, args []resp.RESP) {
	command := strings.ToUpper(args[0].Bulk)
	handler, ok := handlers[command]
	if !ok {
		handler = notFound
	}

	if command == "PSYNC" {
		config.AddReplica(conn)
	}

	conn.Write(handler(args[1:]))

	// Propagate the command to all replicas
	if isWriteCommand(command) {
		for _, replica := range config.Get().Replicas {
			replica.Write(resp.Array(args...).Marshal())
		}
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
