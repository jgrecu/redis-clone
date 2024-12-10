package server

import (
	"strconv"
	"strings"
	"time"

	"github.com/jgrecu/redis-clone/pkg/config"
	"github.com/jgrecu/redis-clone/pkg/rdb"
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

// ConfigGetCommand implements the CONFIG GET command
type ConfigGetCommand struct {
	writer *resp.Writer
	config *config.Config
}

func NewConfigGetCommand(writer *resp.Writer, config *config.Config) *ConfigGetCommand {
	return &ConfigGetCommand{
		writer: writer,
		config: config,
	}
}

func (c *ConfigGetCommand) Execute(args []string) ([]byte, error) {
	if len(args) != 3 {
		return c.writer.WriteError("wrong number of arguments for CONFIG GET"), nil
	}

	param := strings.ToLower(args[2])
	value, err := c.config.Get(param)
	if err != nil {
		return c.writer.WriteError(err.Error()), nil
	}

	// Return array with parameter name and value
	response := []string{param, value}
	return c.writer.WriteArray(response), nil
}

// SaveCommand implements SAVE command
type SaveCommand struct {
	writer *resp.Writer
	rdb    *rdb.RDB
}

func NewSaveCommand(writer *resp.Writer, rdb *rdb.RDB) *SaveCommand {
	return &SaveCommand{
		writer: writer,
		rdb:    rdb,
	}
}

func (c *SaveCommand) Execute(args []string) ([]byte, error) {
	err := c.rdb.Save()
	if err != nil {
		return c.writer.WriteError(err.Error()), nil
	}
	return c.writer.WriteSimpleString("OK"), nil
}

// KeysCommand implements the KEYS command
type KeysCommand struct {
	writer *resp.Writer
	store  *storage.Store
}

func NewKeysCommand(writer *resp.Writer, store *storage.Store) *KeysCommand {
	return &KeysCommand{writer: writer, store: store}
}

func (c *KeysCommand) Execute(args []string) ([]byte, error) {
	if len(args) < 2 {
		return c.writer.WriteError("wrong number of arguments"), nil
	}

	keys := c.store.Keys(args[1])
	if len(keys) == 0 {
		return c.writer.WriteNullBulk(), nil
	}
	return c.writer.WriteArray(keys), nil
}

// InfoCommand implements INFO command
type InfoCommand struct {
	writer *resp.Writer
}

func NewInfoCommand(writer *resp.Writer) *InfoCommand {
	return &InfoCommand{
		writer: writer,
	}
}

func (i *InfoCommand) Execute(args []string) ([]byte, error) {
	str := `# Replication
role:master
connected_slaves:0
master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb
master_repl_offset:0
second_repl_offset:-1
repl_backlog_active:0
repl_backlog_size:1048576
repl_backlog_first_byte_offset:0
repl_backlog_histlen:`
	return i.writer.WriteBulkString(str), nil
}
