package transaction

import "github.com/jgrecu/redis-clone/app/resp"

func Multi(params []resp.RESP) []byte {
	return resp.String("OK").Marshal()
}
