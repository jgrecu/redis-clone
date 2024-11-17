package main

import (
	"log"
	"time"

	"github.com/jgrecu/redis-clone/pkg/server"
)

func main() {
	srv := server.NewServer("0.0.0.0:6379", time.Minute)

	if err := srv.Start(); err != nil {
		log.Fatalf("Server failed to start: %v", err)
	}
}
