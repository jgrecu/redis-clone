package main

import (
	"fmt"
	"github.com/jgrecu/redis-clone/app/config"
	"github.com/jgrecu/redis-clone/app/resp"
	"github.com/jgrecu/redis-clone/app/structures"
	"strings"
)

var Handlers = map[string]func([]resp.RESP) resp.RESP{
	"GET":    structures.Get,
	"SET":    structures.Set,
	"CONFIG": config.GetConfigHandler,
	"PING":   ping,
	"ECHO":   echo,
	"KEYS":   structures.Keys,
	"INFO":   info,
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

func info(params []resp.RESP) resp.RESP {
	if len(params) < 1 {
		return resp.RESP{
			Type: "error",
			Bulk: "ERR wrong number of arguments for 'info' command",
		}
	}

	if strings.ToUpper(params[0].Bulk) == "REPLICATION" {
		role := config.Get("role")
		masterReplId := config.Get("master_replid")
		masterReplOffset := config.Get("master_repl_offset")

		replInfo := fmt.Sprintf(
			"role:%s\nmaster_replid:%s\nmaster_repl_offset:%s",
			role,
			masterReplId,
			masterReplOffset,
		)
		return resp.RESP{
			Type: "bulk",
			Bulk: replInfo,
		}
	}

	return resp.RESP{
		Type: "nil",
	}
}
