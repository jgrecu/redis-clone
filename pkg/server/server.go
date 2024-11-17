package server

import (
	"fmt"
	"log"
	"net"
	"strings"
	"time"

	"github.com/jgrecu/redis-clone/pkg/resp"
	"github.com/jgrecu/redis-clone/pkg/storage"
)

// Server represents the Redis server
type Server struct {
	addr     string
	store    *storage.Store
	parser   *resp.Parser
	writer   *resp.Writer
	commands map[string]Command
}

// NewServer creates a new Redis server
func NewServer(addr string, cleanupInterval time.Duration) *Server {
	store := storage.NewStore(cleanupInterval)
	writer := resp.NewWriter()
	parser := resp.NewParser()

	s := &Server{
		addr:     addr,
		store:    store,
		parser:   parser,
		writer:   writer,
		commands: make(map[string]Command),
	}

	// Register commandss
	s.registerCommands()

	return s
}

func (s *Server) registerCommands() {
	s.commands["ping"] = NewPingCommand(s.writer)
	s.commands["echo"] = NewEchoCommand(s.writer)
	s.commands["set"] = NewSetCommand(s.writer, s.store)
	s.commands["get"] = NewGetCommand(s.writer, s.store)
}

// Start starts the Redis server
func (s *Server) Start() error {
	l, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	defer l.Close()

	log.Printf("Server listening on %s", s.addr)

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
