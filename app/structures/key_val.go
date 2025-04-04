package structures

import (
	"github.com/jgrecu/redis-clone/app/resp"
	"sync"
)

var mapStore = make(map[string]string)
var mut sync.RWMutex

func Get(params []*resp.RESP) *resp.RESP {
	if len(params) != 1 {
		return &resp.RESP{
			Type: "error",
			Bulk: "ERR wrong number of arguments for 'get' command",
		}
	}

	mut.RLock()
	value, ok := mapStore[params[0].Bulk]
	mut.RUnlock()

	if !ok {
		return &resp.RESP{
			Type: "bulk",
			Bulk: "-1",
		}
	}

	return &resp.RESP{
		Type: "bulk",
		Bulk: value,
	}
}

func Set(params []*resp.RESP) *resp.RESP {
	if len(params) != 2 {
		return &resp.RESP{
			Type: "error",
			Bulk: "ERR wrong number of arguments for 'set' command",
		}
	}

	mut.Lock()
	mapStore[params[0].Bulk] = params[1].Bulk
	mut.Unlock()

	return &resp.RESP{
		Type: "string",
		Bulk: "OK",
	}
}
