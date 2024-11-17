package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
)

const ARRAY = '*'
const BULK = '$'

var store sync.Map

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		log.Fatalf("failed to bind to port 6379: %v", err)
	}

	defer func(l net.Listener) {
		err := l.Close()
		if err != nil {
			log.Fatal("failed closing the listener")
		}
	}(l)
	log.Printf("listening on %v", l.Addr())

	for {
		// Block until we receive an incoming connection
		conn, err := l.Accept()
		if err != nil {
			log.Printf("Error accepting connections: %v", err)
			continue
		}

		// Handle client connection
		go func() {
			log.Println("accepted new connection")
			handleStream(conn)
		}()
	}
}

func handleStream(conn net.Conn) {
	// Ensure we close the connection after we're done
	defer func(conn net.Conn) {
		err := conn.Close()
		if err != nil {
			log.Fatalf("failed to close the connection: %v", err)
		}
	}(conn)

	// Read data
	buf := make([]byte, 128)
	for {
		// Read data from client
		_, err := conn.Read(buf)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				log.Println(fmt.Errorf("reading error: %w", err))
			}
			return
		}
		//log.Printf("Message received: %s", buf)
		response := evaluateResp(buf)

		_, err = conn.Write(response)
	}
}

func evaluateResp(msg []byte) []byte {
	var length = 0
	if ARRAY == msg[0] {
		length = int(msg[1]) - '0'

		msg = msg[4:]
		//log.Printf("it is an array: %s, %d", msg, length)
	}
	switch msg[0] {
	case BULK:
		//log.Printf("it is a bulk string: %s", msg)
		args := processBulkStrings(msg, length) // if [echo, hello] or [ping]
		log.Printf("commands: %v", args)
		switch cmd := strings.ToLower(args[0]); {
		case "ping" == cmd:
			return appendSimpleString(msg[:0], "PONG")
		case "echo" == cmd:
			return appendSimpleString(msg[:0], args[1])
		case "set" == cmd:
			err := processSetCommand(args)
			if err != nil {
				return appendSimpleString(msg[:0], "-ERR"+err.Error())
			}
			return appendSimpleString(msg[:0], "OK")
		case "get" == cmd:
			result, err := processGetCommand(args)
			if err != nil {
				return appendSimpleString(msg[:0], "-ERR"+err.Error())
			}
			return appendSimpleString(msg[:0], result)
		default:
			return appendSimpleString(msg[:0], "-ERR unknown command")
		}

	default:
		return appendSimpleString(msg[:0], "-ERR unknown command")
	}

}

func processGetCommand(args []string) (string, error) {
	if len(args) < 2 {
		return "", fmt.Errorf("not enough args")
	}
	val, ok := store.Load(args[1])
	if !ok {
		return "", nil
	}
	return val.(string), nil
}

func processSetCommand(args []string) error {
	if len(args) < 3 {
		return fmt.Errorf("not enough args")
	}
	store.Store(args[1], args[2])
	return nil
}

func processBulkStrings(msg []byte, length int) []string {
	args := make([]string, 0, length)

	for length > 0 {
		argLen := int(msg[1]) - '0'
		arg := string(msg[4 : argLen+4])
		args = append(args, arg)

		msg = msg[argLen+6:]

		length--
	}
	return args
}

func appendSimpleString(b []byte, s string) []byte {
	return fmt.Appendf(b, "+%s\r\n", s)
}
