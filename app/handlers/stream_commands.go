package handlers

import (
	"fmt"
	"github.com/jgrecu/redis-clone/app/resp"
	"github.com/jgrecu/redis-clone/app/structures"
	"math"
	"strconv"
	"strings"
	"time"
)

func (r *CommandRouter) xadd(params []resp.RESP) []byte {
	if len(params) < 2 {
		return resp.Error("ERR wrong number of arguments for 'xadd' command").Marshal()
	}

	entryKey := params[1].Bulk
	pairs := make(map[string]string)
	for i := 2; i < len(params); i += 2 {
		if i+1 < len(params) {
			pairs[params[i].Bulk] = params[i+1].Bulk
		}
	}

	key, err := r.Store.XAdd(params[0].Bulk, entryKey, pairs)
	if err != nil {
		return resp.Error(err.Error()).Marshal()
	}

	return resp.Bulk(key).Marshal()
}

func (r *CommandRouter) xrange(params []resp.RESP) []byte {
	if len(params) < 3 {
		return resp.Error("ERR wrong number of arguments for 'xrange' command").Marshal()
	}

	start := params[1].Bulk
	end := params[2].Bulk

	if start == "-" {
		start = "0-0"
	}
	if end == "+" {
		end = strconv.Itoa(math.MaxInt) + "-" + strconv.Itoa(math.MaxInt)
	}

	entries, ok := r.Store.XRange(params[0].Bulk, start, end)
	if !ok {
		return resp.Nil().Marshal()
	}

	return formatEntries(entries).Marshal()
}

func (r *CommandRouter) xread(params []resp.RESP) []byte {
	if len(params) < 1 {
		return resp.Error("ERR wrong number of arguments for 'xread' command").Marshal()
	}

	if strings.ToUpper(params[0].Bulk) == "STREAMS" {
		streamKeys, ids, err := r.formatStreamKeys(params[1:])
		if err != nil {
			return resp.Error(err.Error()).Marshal()
		}
		return r.xReadStreams(streamKeys, ids).Marshal()
	}

	if strings.ToUpper(params[0].Bulk) == "BLOCK" {
		wait, err := strconv.Atoi(params[1].Bulk)
		if err != nil {
			return resp.Error("ERR invalid timeout").Marshal()
		}

		streamKeys, ids, err := r.formatStreamKeys(params[3:])
		if err != nil {
			return resp.Error(err.Error()).Marshal()
		}

		if wait == 0 {
			ch := make(chan bool)
			go r.waitForNewEntry(streamKeys, ch)
			<-ch
		} else {
			<-time.After(time.Duration(wait) * time.Millisecond)
		}

		result := r.xReadStreams(streamKeys, ids)
		if result.Type == "array" && len(result.Array) == 0 {
			return resp.Nil().Marshal()
		}
		return result.Marshal()
	}

	return resp.Nil().Marshal()
}

func (r *CommandRouter) xReadStreams(streamKeys, ids []string) resp.RESP {
	data := r.Store.XRead(streamKeys, ids)

	streams := []resp.RESP{}
	// Iterate in order of streamKeys to preserve input order
	for _, key := range streamKeys {
		entries, ok := data[key]
		if !ok {
			continue
		}
		for _, entry := range entries {
			pairs := formatPairs(entry)
			streams = append(streams,
				resp.Array(
					resp.Bulk(key),
					resp.Array(
						resp.Array(
							resp.Bulk(entry.Key()),
							resp.Array(pairs...),
						),
					),
				),
			)
		}
	}

	return resp.Array(streams...)
}

func (r *CommandRouter) waitForNewEntry(streamKeys []string, ch chan bool) {
	originalSize := r.Store.StreamSize(streamKeys)
	for {
		newSize := r.Store.StreamSize(streamKeys)
		if newSize > originalSize {
			ch <- true
			return
		}
		<-time.After(50 * time.Millisecond)
	}
}

func (r *CommandRouter) formatStreamKeys(params []resp.RESP) ([]string, []string, error) {
	if len(params)%2 != 0 {
		return nil, nil, fmt.Errorf("ERR wrong number of arguments for 'xread' command")
	}

	streams := []string{}
	ids := []string{}

	halfLen := len(params) / 2
	for i := 0; i < halfLen; i++ {
		streamKey := params[i].Bulk
		id := params[i+halfLen].Bulk

		if id == "$" {
			id = r.Store.LastStreamID(streamKey)
		}

		streams = append(streams, streamKey)
		ids = append(ids, id)
	}

	return streams, ids, nil
}

// formatEntries converts a slice of entries into a RESP array response.
func formatEntries(entries []structures.Entry) resp.RESP {
	res := []resp.RESP{}
	for _, entry := range entries {
		pairs := formatPairs(entry)
		res = append(res, resp.Array(resp.Bulk(entry.Key()), resp.Array(pairs...)))
	}
	return resp.Array(res...)
}

// formatPairs converts an entry's key-value pairs to RESP bulk strings.
func formatPairs(entry structures.Entry) []resp.RESP {
	pairs := []resp.RESP{}
	for k, v := range entry.Pairs {
		pairs = append(pairs, resp.Bulk(k), resp.Bulk(v))
	}
	return pairs
}
