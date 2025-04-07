package handlers

import (
	"fmt"
	"github.com/jgrecu/redis-clone/app/config"
	"github.com/jgrecu/redis-clone/app/resp"
)

func replconf(params []resp.RESP) []byte {
	return resp.String("OK").Marshal()
}

func psync(params []resp.RESP) []byte {
	valid := len(params) > 1 && params[0].Bulk == "?" && params[1].Bulk == "-1"

	if valid {
		message := fmt.Sprintf("FULLRESYNC %s 0\r\n", config.Get("master_replid"))
		return resp.String(message).Marshal()
	}

	return resp.Error("Uncompleted command").Marshal()
}
