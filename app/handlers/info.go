package handlers

import (
	"fmt"
	"github.com/jgrecu/redis-clone/app/config"
	"github.com/jgrecu/redis-clone/app/resp"
	"strings"
)

func info(params []resp.RESP) []byte {
	if len(params) < 1 {
		return resp.Error("ERR wrong number of arguments for 'info' command").Marshal()
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
		return resp.Bulk(replInfo).Marshal()
	}

	return resp.Nil().Marshal()
}
