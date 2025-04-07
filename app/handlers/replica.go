package handlers

import (
	"encoding/hex"
	"fmt"
	"github.com/jgrecu/redis-clone/app/config"
	"github.com/jgrecu/redis-clone/app/resp"
	"strconv"
)

func replconf(params []resp.RESP) []byte {
	if params[0].Bulk == "GETACK" {
		return resp.Command("REPLCONF", "ACK", strconv.Itoa(config.Get().Offset)).Marshal()
	}
	return resp.String("OK").Marshal()
}

func psync(params []resp.RESP) []byte {
	valid := len(params) > 1 && params[0].Bulk == "?" && params[1].Bulk == "-1"

	if valid {
		message := resp.String(
			fmt.Sprintf("FULLRESYNC %s 0", config.Get().MasterReplId),
		).Marshal()

		dbFile := getRDBFile()
		message = append(message, []byte(fmt.Sprintf("$%d\r\n", len(dbFile)))...)
		message = append(message, dbFile...)
		return message
	}

	return resp.Error("Uncompleted command").Marshal()
}

func getRDBFile() []byte {
	file := "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
	data, err := hex.DecodeString(file)
	if err != nil {
		return nil
	}
	return data
}

func wait(params []resp.RESP) []byte {
	numberOfReplicas := len(config.Get().Replicas)
	return resp.Integer(numberOfReplicas).Marshal()
}
