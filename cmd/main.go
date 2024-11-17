package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"os"
)

func main() {
	run()
}

func run() {
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		slog.Error("failed to bind to port 6379: %v", err)
		os.Exit(1)
	}

	defer l.Close()

	log.Printf("listening %v", l.Addr())

	for {
		// Block until we receive an incoming connection
		conn, err := l.Accept()
		if err != nil {
			slog.Error("Error accepting connections: %w", err)
			continue
		}

		// Handle client connection
		go handleClient(conn)
	}
}

func closeIt(c io.Closer, errp *error, msg string) {
	err := c.Close()
	if *errp == nil {
		*errp = fmt.Errorf("%s: %w", msg, err)
	}
}

func handleClient(conn net.Conn) {
	// Ensure we close the connection after we're done
	defer conn.Close()

	// Read data
	buf := make([]byte, 1024)
	for {
		n, err := conn.Read(buf)

		if err != nil {
			if !errors.Is(err, io.EOF) {
				slog.Error("reading error: %w", err)
			}

			return
		}

		// Write the data back
		_, err = conn.Write([]byte("+PONG\r\n"))
		if err != nil {
			slog.Error("writing error: %w", err)
		}

		slog.Info("Received %d bytes, message: %s", n, buf[:n])
	}
}
