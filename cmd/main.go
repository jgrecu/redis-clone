package main

import (
	"flag"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/jgrecu/redis-clone/pkg/config"
	"github.com/jgrecu/redis-clone/pkg/server"
)

func setupConfig() *config.Config {
	cfg := config.NewConfig()

	// Define command line flags
	flag.StringVar(&cfg.Dir, "dir", cfg.Dir, "Directory for RDB files")
	flag.StringVar(&cfg.DbFilename, "dbfilename", cfg.DbFilename, "Filename for the RDB file")
	flag.UintVar(&cfg.Port, "port", cfg.Port, "Port for running the server")
	
	var replicaof string
	flag.StringVar(&replicaof, "replicaof", "", "Master server in the format 'host port'")
	
	flag.Parse()

	// Handle replicaof flag
	if replicaof != "" {
		parts := strings.Fields(replicaof)
		if len(parts) != 2 {
			log.Fatalf("Invalid replicaof format. Expected 'host port', got %s", replicaof)
		}
		cfg.Role = "slave"
		cfg.MasterHost = parts[0]
		masterPort, err := strconv.ParseUint(parts[1], 10, 32)
		if err != nil {
			log.Fatalf("Invalid master port: %v", err)
		}
		cfg.MasterPort = uint(masterPort)
	}

	// Convert relative path to absolute path
	absDir, err := filepath.Abs(cfg.Dir)
	if err != nil {
		log.Fatalf("Failed to get absolute path: %v", err)
	}
	cfg.Dir = absDir

	// Create directory if it doesn't exist
	if err := os.MkdirAll(cfg.Dir, 0755); err != nil {
		log.Fatalf("Failed to create directory %s: %v", cfg.Dir, err)
	}

	return cfg
}

func main() {
	cfg := setupConfig()

	// Create and start server
	srv := server.NewServer(cfg)
	if err := srv.Start(); err != nil {
		log.Fatal(err)
	}
}
