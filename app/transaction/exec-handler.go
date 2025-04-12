package transaction

import "github.com/jgrecu/redis-clone/app/resp"

func Exec(params []resp.RESP) []byte {
	return resp.Error("ERR EXEC without MULTI").Marshal()
}
