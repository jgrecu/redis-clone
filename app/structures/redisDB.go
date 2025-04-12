package structures

import (
    "github.com/jgrecu/redis-clone/app/resp"
    "strings"
    "sync"
    "time"
)

type MapValue struct {
    Typ    string
    Stream *Stream
    String string
    Expiry time.Time
}

type RedisDB = map[string]MapValue

var mapStore = make(RedisDB, 0)
var mut = sync.RWMutex{}

func Get(params []resp.RESP) []byte {
    if len(params) != 1 {
        return resp.Error("ERR wrong number of arguments for 'get' command").Marshal()
    }

    mut.RLock()
    value, ok := mapStore[params[0].Bulk]
    mut.RUnlock()

    if !ok {
        resp.Nil().Marshal()
    }

    if !value.Expiry.IsZero() && value.Expiry.Before(time.Now()) {
        mut.Lock()
        delete(mapStore, params[0].Bulk)
        mut.Unlock()

        return resp.Nil().Marshal()
    }

    return resp.Bulk(value.String).Marshal()
}

func Set(params []resp.RESP) []byte {
    if len(params) < 2 {
        return resp.Error("ERR wrong number of arguments for 'set' command").Marshal()
    }

    expirationDate := time.Time{}

    if len(params) >= 4 && strings.ToUpper(params[2].Bulk) == "PX" {
        expiry, err := time.ParseDuration(params[3].Bulk + "ms")
        if err != nil {
            return resp.Error("ERR invalid expire time in set command").Marshal()
        }
        expirationDate = time.Now().Add(expiry)
    }

    mut.Lock()
    mapStore[params[0].Bulk] = MapValue{
        Typ:    "string",
        String: params[1].Bulk,
        Expiry: expirationDate,
    }
    mut.Unlock()

    return resp.String("OK").Marshal()
}

func Keys(params []resp.RESP) []byte {
    if len(params) != 1 {
        return resp.Error("ERR wrong number of arguments for 'keys' command").Marshal()
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

    return resp.Array(keys...).Marshal()
}

func LoadKeys(redisDb RedisDB) {
    mut.Lock()
    mapStore = redisDb
    mut.Unlock()
}

func Typ(params []resp.RESP) []byte {
    if len(params) != 1 {
        return resp.Error("ERR wrong number of arguments for 'type' command").Marshal()
    }

    mut.RLock()
    defer mut.RUnlock()

    value, ok := mapStore[params[0].Bulk]
    if !ok {
        return resp.String("none").Marshal()
    }

    return resp.Bulk(value.Typ).Marshal()
}

func getMapValue(key string) (MapValue, bool) {
    mut.RLock()
    defer mut.RUnlock()

    value, ok := mapStore[key]
    return value, ok
}

func setMapValue(key string, value MapValue) {
    mut.Lock()
    mapStore[key] = value
    mut.Unlock()
}
