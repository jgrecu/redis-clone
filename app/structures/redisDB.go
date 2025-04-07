package structures

import (
	"github.com/jgrecu/redis-clone/app/resp"
	"strings"
	"sync"
	"time"
)

type mapValue struct {
	Value  string
	Expiry time.Time
}

var mapStore = make(map[string]mapValue, 0)
var mut = sync.RWMutex{}

func Get(params []resp.RESP) resp.RESP {
	if len(params) != 1 {
		return resp.RESP{
			Type: "error",
			Bulk: "ERR wrong number of arguments for 'get' command",
		}
	}

	mut.RLock()
	value, ok := mapStore[params[0].Bulk]
	mut.RUnlock()

	if !ok {
		return resp.RESP{
			Type: "nil",
		}
	}

	if !value.Expiry.IsZero() && value.Expiry.Before(time.Now()) {
		mut.Lock()
		delete(mapStore, params[0].Bulk)
		mut.Unlock()

		return resp.RESP{
			Type: "nil",
		}
	}

	return resp.RESP{
		Type: "bulk",
		Bulk: value.Value,
	}
}

func Set(params []resp.RESP) resp.RESP {
	if len(params) < 2 {
		return resp.RESP{
			Type: "error",
			Bulk: "ERR wrong number of arguments for 'set' command",
		}
	}

	expirationDate := time.Time{}

	if len(params) >= 4 && strings.ToUpper(params[2].Bulk) == "PX" {
		expiry, err := time.ParseDuration(params[3].Bulk + "ms")
		if err != nil {
			return resp.RESP{
				Type: "error",
				Bulk: "ERR value is not an integer or out of range",
			}
		}
		expirationDate = time.Now().Add(expiry)
	}

	mut.Lock()
	mapStore[params[0].Bulk] = mapValue{
		Value:  params[1].Bulk,
		Expiry: expirationDate,
	}
	mut.Unlock()

	return resp.RESP{
		Type: "string",
		Bulk: "OK",
	}
}

func Keys(params []resp.RESP) resp.RESP {
	if len(params) != 1 {
		return resp.RESP{
			Type: "error",
			Bulk: "ERR wrong number of arguments for 'keys' command",
		}
	}

	mut.RLock()
	defer mut.RUnlock()

	keys := make([]resp.RESP, 0)
	for key := range mapStore {
		keys = append(keys, resp.RESP{
			Type: "bulk",
			Bulk: key,
		})
	}

	return resp.RESP{
		Type:  "array",
		Array: keys,
	}
}
