package handlers

import (
	"fmt"
	"github.com/jgrecu/redis-clone/app/config"
	"github.com/jgrecu/redis-clone/app/resp"
	"strings"
)

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
