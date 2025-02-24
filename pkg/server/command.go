package server

import (
	"fmt"
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

// ReplConfCommand implements the REPLCONF command
type ReplConfCommand struct {
	writer *resp.Writer
	server *Server
}

func NewReplConfCommand(writer *resp.Writer, server *Server) *ReplConfCommand {
	return &ReplConfCommand{writer: writer, server: server}
}

func (c *ReplConfCommand) Execute(args []string) ([]byte, error) {
	// Store the connection when first REPLCONF command is received
	if c.server.replicaConn == nil {
		// Get the connection from the current client
		c.server.replicaConn = c.server.currentConn
	}
	return c.writer.WriteSimpleString("OK"), nil
}

// PSyncCommand implements the PSYNC command
type PSyncCommand struct {
	writer *resp.Writer
	server *Server
}

func NewPSyncCommand(writer *resp.Writer, server *Server) *PSyncCommand {
	return &PSyncCommand{writer: writer, server: server}
}

func (c *PSyncCommand) getEmptyRDBFile() []byte {
	// Minimal valid empty RDB file
	rdb := []byte{
		'R', 'E', 'D', 'I', 'S', '0', '0', '0', '9', // RDB version 9 header
		0xFF,                                           // EOF marker
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // CRC64
	}

	// Format as per Redis protocol: $<length>\r\n<contents>
	return []byte(fmt.Sprintf("$%d\r\n%s", len(rdb), rdb))
}

func (c *PSyncCommand) Execute(args []string) ([]byte, error) {
	if len(args) != 3 {
		return c.writer.WriteError("wrong number of arguments for PSYNC"), nil
	}
	// Store the connection if not already stored
	if c.server.replicaConn == nil {
		c.server.replicaConn = c.server.currentConn
	}

	// Hardcoded replication ID as per requirements
	replID := "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
	response := fmt.Sprintf("FULLRESYNC %s 0", replID)

	// Combine FULLRESYNC response with RDB file
	fullResponse := append(c.writer.WriteSimpleString(response), c.getEmptyRDBFile()...)
	return fullResponse, nil
}

// InfoCommand implements INFO command
type InfoCommand struct {
	writer *resp.Writer
	config *config.Config
	server *Server
}

func NewInfoCommand(writer *resp.Writer, config *config.Config, server *Server) *InfoCommand {
	return &InfoCommand{
		writer: writer,
		config: config,
		server: server,
	}
}

func (i *InfoCommand) Execute(args []string) ([]byte, error) {
	var info strings.Builder
	info.WriteString("# Replication\r\n")

	// Add role information
	info.WriteString(fmt.Sprintf("role:%s\r\n", i.config.Role))

	if i.config.Role == "master" {
		connectedSlaves := 0
		if i.server.replicaConn != nil {
			connectedSlaves = 1
		}
		info.WriteString(fmt.Sprintf("connected_slaves:%d\r\n", connectedSlaves))
		info.WriteString("master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\r\n")
		info.WriteString("master_repl_offset:0\r\n")
	} else {
		info.WriteString(fmt.Sprintf("master_host:%s\r\n", i.config.MasterHost))
		info.WriteString(fmt.Sprintf("master_port:%d\r\n", i.config.MasterPort))
		info.WriteString("master_link_status:up\r\n")
	}

	return i.writer.WriteBulkString(info.String()), nil
}
