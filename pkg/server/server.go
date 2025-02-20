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
	config     *config.Config
	store      *storage.Store
	rdb        *rdb.RDB
	parser     *resp.Parser
	writer     *resp.Writer
	commands   map[string]Command
	masterConn net.Conn
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

	// Register commands
	s.registerCommands()

	return s
}

func (s *Server) registerCommands() {
	s.commands["PING"] = NewPingCommand(s.writer)
	s.commands["ECHO"] = NewEchoCommand(s.writer)
	s.commands["SET"] = NewSetCommand(s.writer, s.store)
	s.commands["GET"] = NewGetCommand(s.writer, s.store)
	s.commands["CONFIG"] = NewConfigGetCommand(s.writer, s.config)
	s.commands["SAVE"] = NewSaveCommand(s.writer, s.rdb)
	s.commands["KEYS"] = NewKeysCommand(s.writer, s.store)
	s.commands["INFO"] = NewInfoCommand(s.writer, s.config)
	s.commands["REPLCONF"] = NewReplConfCommand(s.writer)
}

// connectToMaster establishes connection to master and performs handshake
func (s *Server) connectToMaster() error {
	masterAddr := fmt.Sprintf("%s:%d", s.config.MasterHost, s.config.MasterPort)
	conn, err := net.Dial("tcp", masterAddr)
	if err != nil {
		return fmt.Errorf("failed to connect to master: %w", err)
	}

	// Store the connection for future use
	s.masterConn = conn

	// Send PING command
	pingCmd := []string{"PING"}
	pingMsg := s.writer.WriteArray(pingCmd)
	_, err = conn.Write(pingMsg)
	if err != nil {
		conn.Close()
		s.masterConn = nil
		return fmt.Errorf("failed to send PING to master: %w", err)
	}

	// Read PING response
	respBuf := make([]byte, 512)
	_, err = conn.Read(respBuf)
	if err != nil {
		conn.Close()
		s.masterConn = nil
		return fmt.Errorf("failed to read PING response from master: %w", err)
	}

	// Send first REPLCONF command (listening-port)
	replconfPortCmd := []string{"REPLCONF", "listening-port", fmt.Sprintf("%d", s.config.Port)}
	replconfPortMsg := s.writer.WriteArray(replconfPortCmd)
	_, err = conn.Write(replconfPortMsg)
	if err != nil {
		conn.Close()
		s.masterConn = nil
		return fmt.Errorf("failed to send REPLCONF listening-port to master: %w", err)
	}

	// Read first REPLCONF response
	_, err = conn.Read(respBuf)
	if err != nil {
		conn.Close()
		s.masterConn = nil
		return fmt.Errorf("failed to read REPLCONF listening-port response from master: %w", err)
	}

	// Send second REPLCONF command (capabilities)
	replconfCapaCmd := []string{"REPLCONF", "capa", "psync2"}
	replconfCapaMsg := s.writer.WriteArray(replconfCapaCmd)
	_, err = conn.Write(replconfCapaMsg)
	if err != nil {
		conn.Close()
		s.masterConn = nil
		return fmt.Errorf("failed to send REPLCONF capa to master: %w", err)
	}

	// Read second REPLCONF response
	_, err = conn.Read(respBuf)
	if err != nil {
		conn.Close()
		s.masterConn = nil
		return fmt.Errorf("failed to read REPLCONF capa response from master: %w", err)
	}

	// Send PSYNC command
	psyncCmd := []string{"PSYNC", "?", "-1"}
	psyncMsg := s.writer.WriteArray(psyncCmd)
	_, err = conn.Write(psyncMsg)
	if err != nil {
		conn.Close()
		s.masterConn = nil
		return fmt.Errorf("failed to send PSYNC to master: %w", err)
	}

	// Read PSYNC response (ignored for now as per requirements)
	_, err = conn.Read(respBuf)
	if err != nil {
		conn.Close()
		s.masterConn = nil
		return fmt.Errorf("failed to read PSYNC response from master: %w", err)
	}

	log.Printf("Connected to master at %s and completed handshake", masterAddr)
	return nil
}

// Start starts the Redis server
func (s *Server) Start() error {
	// Load existing RDB file if it exists
	if err := s.rdb.Load(); err != nil {
		return fmt.Errorf("failed to load RDB: %w", err)
	}

	// If we're a replica, connect to master first
	if s.config.Role == "slave" {
		if err := s.connectToMaster(); err != nil {
			return err
		}
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

	cmdName := strings.ToUpper(msg.Content[0])
	cmd, exists := s.commands[cmdName]
	if !exists {
		return s.writer.WriteError(fmt.Sprintf("ERR unknown command '%s'", cmdName)), nil
	}

	return cmd.Execute(msg.Content)
}
