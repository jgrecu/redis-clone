package structures

import "time"

// MapValue represents a value stored in the Redis database.
type MapValue struct {
	Typ    string
	Stream *Stream
	String string
	Expiry time.Time
}

// RedisDB is the underlying map type for the store.
type RedisDB = map[string]MapValue
