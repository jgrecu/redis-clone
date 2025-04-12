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
		replInfo := fmt.Sprintf(
			"role:%s\nmaster_replid:%s\nmaster_repl_offset:%s",
			config.Get().Role,
			config.Get().MasterReplId,
			config.Get().MasterReplOffset,
		)
		return resp.Bulk(replInfo).Marshal()
	}

	return resp.Nil().Marshal()
}
