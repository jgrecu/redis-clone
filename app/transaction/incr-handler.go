package transaction

import (
	"github.com/jgrecu/redis-clone/app/resp"
	"github.com/jgrecu/redis-clone/app/structures"
)

func Incr(params []resp.RESP) []byte {
	if len(params) != 1 {
		return resp.Error("ERR wrong number of arguments for 'incr' command").Marshal()
	}

	value := params[0].Bulk

	intValue, err := structures.Incr(value)
	if err != nil {
		return resp.Error("ERR value is not an integer or out of range").Marshal()
	}

	return resp.Integer(intValue).Marshal()
}
