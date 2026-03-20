package handlers

import (
	"github.com/jgrecu/redis-clone/app/config"
	"github.com/jgrecu/redis-clone/app/resp"
	"github.com/jgrecu/redis-clone/app/structures"
	"strings"
	"time"
)

// CommandHandler is a function that processes a Redis command and returns
// the RESP-encoded response.
type CommandHandler func([]resp.RESP) []byte

// CommandRouter routes Redis commands to their handler functions.
// It holds a reference to the Store, keeping command logic decoupled
// from storage internals.
type CommandRouter struct {
	Store    *structures.Store
	commands map[string]CommandHandler
}

// NewRouter creates a CommandRouter with all commands registered.
func NewRouter(store *structures.Store) *CommandRouter {
	r := &CommandRouter{Store: store}
	r.commands = map[string]CommandHandler{
		"PING":     r.ping,
		"ECHO":     r.echo,
		"GET":      r.get,
		"SET":      r.set,
		"KEYS":     r.keys,
		"TYPE":     r.typ,
		"INCR":     r.incr,
		"INFO":     r.info,
		"REPLCONF": r.replconf,
		"PSYNC":    r.psync,
		"XADD":     r.xadd,
		"XRANGE":   r.xrange,
		"XREAD":    r.xread,
		"CONFIG":   config.GetConfigHandler,
	}
	return r
}

// GetHandler returns the handler for the given command, or notFound if unknown.
func (r *CommandRouter) GetHandler(command string) CommandHandler {
	handler, ok := r.commands[command]
	if !ok {
		return notFound
	}
	return handler
}

func (r *CommandRouter) ping(params []resp.RESP) []byte {
	return resp.String("PONG").Marshal()
}

func (r *CommandRouter) echo(params []resp.RESP) []byte {
	return resp.String(params[0].Bulk).Marshal()
}

func notFound(params []resp.RESP) []byte {
	return resp.Error("Command not found").Marshal()
}

func (r *CommandRouter) get(params []resp.RESP) []byte {
	if len(params) != 1 {
		return resp.Error("ERR wrong number of arguments for 'get' command").Marshal()
	}

	value, ok := r.Store.Get(params[0].Bulk)
	if !ok {
		return resp.Nil().Marshal()
	}

	return resp.Bulk(value).Marshal()
}

func (r *CommandRouter) set(params []resp.RESP) []byte {
	if len(params) < 2 {
		return resp.Error("ERR wrong number of arguments for 'set' command").Marshal()
	}

	expiry := time.Time{}
	if len(params) >= 4 && strings.ToUpper(params[2].Bulk) == "PX" {
		d, err := time.ParseDuration(params[3].Bulk + "ms")
		if err != nil {
			return resp.Error("ERR invalid expire time in set command").Marshal()
		}
		expiry = time.Now().Add(d)
	}

	r.Store.Set(params[0].Bulk, params[1].Bulk, expiry)
	return resp.String("OK").Marshal()
}

func (r *CommandRouter) keys(params []resp.RESP) []byte {
	if len(params) != 1 {
		return resp.Error("ERR wrong number of arguments for 'keys' command").Marshal()
	}

	keys := r.Store.Keys()
	result := make([]resp.RESP, len(keys))
	for i, k := range keys {
		result[i] = resp.Bulk(k)
	}
	return resp.Array(result...).Marshal()
}

func (r *CommandRouter) typ(params []resp.RESP) []byte {
	if len(params) != 1 {
		return resp.Error("ERR wrong number of arguments for 'type' command").Marshal()
	}

	typeName := r.Store.Type(params[0].Bulk)
	if typeName == "none" {
		return resp.String("none").Marshal()
	}
	return resp.Bulk(typeName).Marshal()
}

func (r *CommandRouter) incr(params []resp.RESP) []byte {
	if len(params) != 1 {
		return resp.Error("ERR wrong number of arguments for 'incr' command").Marshal()
	}

	value, err := r.Store.Incr(params[0].Bulk)
	if err != nil {
		return resp.Error("ERR value is not an integer or out of range").Marshal()
	}
	return resp.Integer(value).Marshal()
}
