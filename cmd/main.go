package main

import (
	"flag"
	"log"
	"os"
	"path/filepath"

	"github.com/jgrecu/redis-clone/pkg/config"
	"github.com/jgrecu/redis-clone/pkg/server"
)

func main() {
	cfg := config.NewConfig()

	// Define command line flags
	flag.StringVar(&cfg.Dir, "dir", cfg.Dir, "Directorey for RDB files")
	flag.StringVar(&cfg.DbFilename, "dbfilename", cfg.DbFilename, "Filename for the RDB file")
	flag.UintVar(&cfg.Port, "port", cfg.Port, "Port for running the server")
	flag.Parse()

	// Convert relative path to absolute path
	absDir, err := filepath.Abs(cfg.Dir)
	if err != nil {
		log.Fatalf("Failed to resolve absolute path: %v", err)
	}
	cfg.Dir = absDir

	// Ensure directory exists
	if err := os.MkdirAll(cfg.Dir, 0755); err != nil {
		log.Fatalf("Failed to create directory %s: %v", cfg.Dir, err)
	}

	// Create and start server
	srv := server.NewServer(cfg)

	if err := srv.Start(); err != nil {
		log.Fatalf("Server failed to start: %v", err)
	}
}
