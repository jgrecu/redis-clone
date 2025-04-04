package main

import (
	"bufio"
	"fmt"
	"github.com/jgrecu/redis-clone/app/resp"
	"net"
	"os"
	"strings"
)

func main() {

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			break
		}

		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	reader := resp.NewRespReader(bufio.NewReader(conn))
	for {
		readMsg, err := reader.Read()
		if err != nil {
			fmt.Println("Error reading from connection: ", err.Error())
			break
		}

		if readMsg.Type != "array" {
			fmt.Println("expected to receive an array")
			break
		}

		command := strings.ToUpper(readMsg.Array[0].Bulk)

		handler, ok := handlers[command]
		if !ok {
			fmt.Println("Unknown command: ", command)
			break
		}
		res, _ := handler(readMsg.Array[1:]).Marshal()
		conn.Write(res)
	}
}
