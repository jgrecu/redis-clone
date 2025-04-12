package structures

import (
	"github.com/jgrecu/redis-clone/app/resp"
	"math"
	"strconv"
)

func Xadd(params []resp.RESP) []byte {
	if len(params) < 2 {
		return resp.Error("ERR wrong number of arguments for 'xadd' command").Marshal()
	}

	mut.Lock()
	defer mut.Unlock()

	stream, ok := mapStore[params[0].Bulk]
	entryKey := params[1].Bulk

	newMAp := make(map[string]string, 0)
	for i := 2; i < len(params); i += 2 {
		if i+1 < len(params) {
			newMAp[params[i].Bulk] = params[i+1].Bulk
		}
	}

	if !ok {
		stream = MapValue{
			Typ:    "stream",
			Stream: NewStream(),
		}
	}

	key, err := stream.Stream.Add(entryKey, newMAp)
	if err != nil {
		return resp.Error(err.Error()).Marshal()
	}

	mapStore[params[0].Bulk] = stream

	return resp.Bulk(key).Marshal()
}

func XRange(params []resp.RESP) []byte {
	if len(params) < 3 {
		return resp.Error("ERR wrong number of arguments for 'xrange' command").Marshal()
	}

	mut.RLock()
	defer mut.RUnlock()

	value, ok := mapStore[params[0].Bulk]
	if !ok || value.Typ != "stream" {
		return resp.Nil().Marshal()
	}

	start := params[1].Bulk
	end := params[2].Bulk

	if start == "-" {
		start = "0-0"
	}

	if end == "+" {
		end = strconv.Itoa(math.MaxInt) + "-" + strconv.Itoa(math.MaxInt)
	}

	entries := value.Stream.Range(start, end)

	res := []resp.RESP{}
	for _, entry := range entries {
		pairs := []resp.RESP{}
		for k, v := range entry.Pairs {
			pairs = append(pairs, resp.Bulk(k), resp.Bulk(v))
		}

		res = append(res, resp.Array(resp.Bulk(entry.Key()), resp.Array(pairs...)))
	}

	return resp.Array(res...).Marshal()
}
