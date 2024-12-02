package server

import (
	"fmt"
	"log"
	"net"
	"strings"

	"github.com/jgrecu/redis-clone/pkg/config"
	"github.com/jgrecu/redis-clone/pkg/rdb"
	"github.com/jgrecu/redis-clone/pkg/resp"
	"github.com/jgrecu/redis-clone/pkg/storage"
)

// Server represents the Redis server
type Server struct {
	config   *config.Config
	store    *storage.Store
	rdb      *rdb.RDB
	parser   *resp.Parser
	writer   *resp.Writer
	commands map[string]Command
}

// NewServer creates a new Redis server
func NewServer(cfg *config.Config) *Server {
	store := storage.NewStore(cfg.CleanupInterval)
	writer := resp.NewWriter()
	parser := resp.NewParser()

	s := &Server{
		config:   cfg,
		store:    store,
		parser:   parser,
		writer:   writer,
		commands: make(map[string]Command),
	}

	// Initialize RDB
	s.rdb = rdb.NewRDB(cfg, store)

	// Register commandss
	s.registerCommands()

	return s
}

func (s *Server) registerCommands() {
	s.commands["ping"] = NewPingCommand(s.writer)
	s.commands["echo"] = NewEchoCommand(s.writer)
	s.commands["set"] = NewSetCommand(s.writer, s.store)
	s.commands["get"] = NewGetCommand(s.writer, s.store)
	s.commands["config"] = NewConfigGetCommand(s.writer, s.config)
	s.commands["save"] = NewSaveCommand(s.writer, s.rdb)
	s.commands["keys"] = NewKeysCommand(s.writer, s.store)
}

// Start starts the Redis server
func (s *Server) Start() error {
	// Load existing RDB file if it exists
	if err := s.rdb.Load(); err != nil {
		return fmt.Errorf("failed to load RDB: %w", err)
	}

	address := fmt.Sprintf("%s:%d", s.config.Address, s.config.Port)
	l, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	defer l.Close()

	log.Printf("Server listening on %s", address)

	for {
		conn, err := l.Accept()
		if err != nil {
			log.Printf("Error accepting connection: %v", err)
			continue
		}

		go s.handleConnection(conn)
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()

	buf := make([]byte, 512)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			return
		}

		msg, err := s.parser.Parse(buf[:n])
		if err != nil {
			conn.Write(s.writer.WriteError(err.Error()))
			continue
		}

		response, err := s.handleMessage(msg)
		if err != nil {
			conn.Write(s.writer.WriteError(err.Error()))
			continue
		}

		conn.Write(response)
	}
}

func (s *Server) handleMessage(msg *resp.Message) ([]byte, error) {
	if len(msg.Content) == 0 {
		return nil, fmt.Errorf("empty command")
	}

	cmdName := strings.ToLower(msg.Content[0])
	cmd, exists := s.commands[cmdName]
	if !exists {
		return s.writer.WriteError("unknown command"), nil
	}

	return cmd.Execute(msg.Content)
}
