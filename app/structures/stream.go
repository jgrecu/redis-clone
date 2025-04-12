package structures

import (
    "fmt"
    "strings"
)

type Entry struct {
    Key   string
    Pairs map[string]string
}

type Stream struct {
    Entries []Entry
}

func NewStream() *Stream {
    return &Stream{Entries: []Entry{}}
}

func (s *Stream) Add(key string, pairs map[string]string) error {
    err := s.validateKey(key)
    if err != nil {
        return err
    }

    s.Entries = append(s.Entries, Entry{Key: key, Pairs: pairs})
    return nil
}

func (s *Stream) Get(key string) (map[string]string, bool) {
    for _, e := range s.Entries {
        if e.Key == key {
            return e.Pairs, true
        }
    }
    return nil, false
}

func (s *Stream) validateKey(key string) error {
    ids := strings.Split(key, "-")
    if ids[0] < "0" || (ids[0] == "0" && ids[1] <= "0") {
        return fmt.Errorf("ERR The ID specified in XADD must be greater than 0-0")
    }

    len := len(s.Entries)
    if len == 0 {
        return nil
    }

    lastIds := strings.Split(s.Entries[len-1].Key, "-")

    if (ids[0] < lastIds[0]) || (ids[0] == lastIds[0] && ids[1] <= lastIds[1]) {
        return fmt.Errorf("ERR The ID specified in XADD is equal or smaller than the target stream top item")
    }

    return nil
}
