package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
)

func main() {
	err := run()
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		return fmt.Errorf("failed to bind to port 6379: %w", err)
	}

	defer closeIt(l, &err, "close listener")

	log.Printf("listening %v", l.Addr())

	conn, err := l.Accept()
	if err != nil {
		return fmt.Errorf("accept: %w", err)
	}

	defer closeIt(conn, &err, "close connection")

	buf := make([]byte, 128)

	_, err = conn.Read(buf)
	if err != nil {
		return fmt.Errorf("read command: %w", err)
	}

	log.Printf("read command:\n%s", buf)

	_, err = conn.Write([]byte("+PONG\r\n"))
	if err != nil {
		return fmt.Errorf("write response: %w", err)
	}

	return nil
}

func closeIt(c io.Closer, errp *error, msg string) {
	err := c.Close()
	if *errp == nil {
		*errp = fmt.Errorf("%s: %w", msg, err)
	}
}
