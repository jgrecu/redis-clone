package structures

import (
    "fmt"
    "github.com/jgrecu/redis-clone/app/resp"
    "log"
    "math"
    "strconv"
    "strings"
    "time"
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

func xReadStreams(streamKeys, ids []string) resp.RESP {
    mut.RLock()
    defer mut.RUnlock()

    streams := []resp.RESP{}

    streamLen := len(streamKeys)
    for i := 1; i < streamLen; i++ {
        streamKey := streamKeys[i]
        val, ok := mapStore[streamKey]
        if !ok || val.Typ != "stream" {
            continue
        }

        stream := val.Stream

        startKey := ids[i]
        entries := stream.Read(startKey)
        for _, entry := range entries {
            pairs := []resp.RESP{}
            for k, v := range entry.Pairs {
                pairs = append(pairs, resp.Bulk(k), resp.Bulk(v))
            }

            streams = append(
                streams,
                resp.Array(
                    resp.Bulk(streamKey),
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

func XRead(params []resp.RESP) []byte {
    if len(params) < 1 {
        return resp.Error("ERR wrong number of arguments for 'xread' command").Marshal()
    }

    if strings.ToUpper(params[0].Bulk) == "STREAMS" {
        streamKeys, ids, err := formatKeys(params[1:])
        if err != nil {
            return resp.Error(err.Error()).Marshal()
        }
        return xReadStreams(streamKeys, ids).Marshal()
    }

    if strings.ToUpper(params[0].Bulk) == "BLOCK" {
        wait, err := strconv.Atoi(params[1].Bulk)
        if err != nil {
            return resp.Error("ERR invalid timeout").Marshal()
        }

        streamKeys, ids, err := formatKeys(params[3:])
        if err != nil {
            return resp.Error(err.Error()).Marshal()
        }

        if wait == 0 {
            ch := make(chan bool)
            go waitForNewEntry(streamKeys, ch)
            <-ch
        } else {
            <-time.After(time.Duration(wait) * time.Millisecond)
        }
        res := xReadStreams(streamKeys, ids)

        if res.Type == "array" && len(res.Array) == 0 {
            return resp.Nil().Marshal()
        }
        return res.Marshal()
    }

    return resp.Nil().Marshal()
}

func waitForNewEntry(streamKeys []string, ch chan bool) {
    originalSize := streamSize(streamKeys)
    for {
        newSize := streamSize(streamKeys)
        if newSize > originalSize {
            ch <- true
            return
        }

        <-time.After(50 * time.Millisecond)
    }
}

func streamSize(streams []string) int {
    size := 0
    for _, streamKey := range streams {
        stream, ok := mapStore[streamKey]
        if !ok || stream.Typ != "stream" {
            continue
        }

        size += stream.Stream.Len()
    }
    return size
}

func formatKeys(params []resp.RESP) ([]string, []string, error) {
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
            stream, ok := mapStore[streamKey]
            if !ok || stream.Typ != "stream" {
                id = "0-0"
            } else {
                lastTimestamp := stream.Stream.LastTimestamp()
                lastSeq := stream.Stream.LastSeq(lastTimestamp)

                id = fmt.Sprintf("%d-%d", lastTimestamp, lastSeq)
            }
        }

        streams = append(streams, streamKey)
        log.Println(" || streamkey (", streamKey, ") id (", id, ") || ")
        ids = append(ids, id)
    }

    return streams, ids, nil
}
