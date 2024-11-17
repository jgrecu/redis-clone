package server

import (
	"strconv"
	"strings"
	"time"

	"github.com/jgrecu/redis-clone/pkg/resp"
	"github.com/jgrecu/redis-clone/pkg/storage"
)

// Command represents a Redis command
type Command interface {
	Execute(args []string) ([]byte, error)
}

// PingCommand implements the PING command
type PingCommand struct {
	writer *resp.Writer
}

func NewPingCommand(writer *resp.Writer) *PingCommand {
	return &PingCommand{writer: writer}
}

func (c *PingCommand) Execute(args []string) ([]byte, error) {
	return c.writer.WriteSimpleString("PONG"), nil
}

// EchoCommand implements the ECHO command
type EchoCommand struct {
	writer *resp.Writer
}

func NewEchoCommand(writer *resp.Writer) *EchoCommand {
	return &EchoCommand{writer: writer}
}

func (c *EchoCommand) Execute(args []string) ([]byte, error) {
	if len(args) < 2 {
		return c.writer.WriteError("wrong number of argument"), nil
	}
	return c.writer.WriteSimpleString(args[1]), nil
}

// SetCommand implements the SET command
type SetCommand struct {
	writer *resp.Writer
	store  *storage.Store
}

func NewSetCommand(writer *resp.Writer, store *storage.Store) *SetCommand {
	return &SetCommand{writer: writer, store: store}
}

func (c *SetCommand) Execute(args []string) ([]byte, error) {
	if len(args) < 3 {
		return c.writer.WriteError("wrong number of arguments"), nil
	}

	var expiration time.Duration
	if len(args) == 5 && strings.ToLower(args[3]) == "px" {
		ms, err := strconv.Atoi(args[4])
		if err == nil {
			expiration = time.Duration(ms) * time.Millisecond
		}
	}

	c.store.Set(args[1], args[2], expiration)
	return c.writer.WriteSimpleString("OK"), nil
}

// GetCommand implements the GET command
type GetCommand struct {
	writer *resp.Writer
	store  *storage.Store
}

func NewGetCommand(writer *resp.Writer, store *storage.Store) *GetCommand {
	return &GetCommand{writer: writer, store: store}
}

func (c *GetCommand) Execute(args []string) ([]byte, error) {
	if len(args) < 2 {
		return c.writer.WriteError("wrong number of arguments"), nil
	}

	val, ok := c.store.Get(args[1])
	if !ok {
		return c.writer.WriteNullBulk(), nil
	}
	return c.writer.WriteSimpleString(val), nil
}
