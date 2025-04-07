package handlers

import (
	"github.com/jgrecu/redis-clone/app/config"
	"github.com/jgrecu/redis-clone/app/resp"
	"github.com/jgrecu/redis-clone/app/structures"
	"strings"
)

type CommandHandler func([]resp.RESP) resp.RESP

var handlers = map[string]CommandHandler{
	"GET":    structures.Get,
	"SET":    structures.Set,
	"CONFIG": config.GetConfigHandler,
	"PING":   ping,
	"ECHO":   echo,
	"KEYS":   structures.Keys,
	"INFO":   info,
}

func GetHandler(command string) CommandHandler {
	handler, ok := handlers[strings.ToUpper(command)]
	if !ok {
		return notFound
	}
	return handler
}

func ping(params []resp.RESP) resp.RESP {
	return resp.RESP{
		Type: "string",
		Bulk: "PONG",
	}
}

func echo(params []resp.RESP) resp.RESP {
	return resp.RESP{
		Type: "bulk",
		Bulk: params[0].Bulk,
	}
}

func notFound(params []resp.RESP) resp.RESP {
	return resp.Error("Command not found")
}
