package transaction

import "github.com/jgrecu/redis-clone/app/resp"

type TransQueue struct {
	Commands []resp.RESP
}
