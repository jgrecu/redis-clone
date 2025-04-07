package handlers

import (
	"github.com/jgrecu/redis-clone/app/config"
	"github.com/jgrecu/redis-clone/app/resp"
	"github.com/jgrecu/redis-clone/app/structures"
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

func GetHandler(command string) CommandHandler {
	handler, ok := handlers[strings.ToUpper(command)]
	if !ok {
		return notFound
	}
	return handler
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
