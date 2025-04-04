package main

import (
	"bufio"
	"fmt"
	"github.com/jgrecu/redis-clone/app/resp"
	"log"
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
	defer l.Close()

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
	defer conn.Close()
	fmt.Println("Received connection: ", conn.RemoteAddr().String())

	reader := resp.NewRespReader(bufio.NewReader(conn))
	for {
		readMsg, err := reader.Read()
		if err != nil {
			log.Println("Error reading from connection: ", err.Error())
			break
		}

		if readMsg.Type != "array" || len(readMsg.Array) < 1 {
			fmt.Println("Invalid command")
			break
		}

		command := strings.ToUpper(readMsg.Array[0].Bulk)

		handler, ok := GetHandler(command)
		if !ok {
			fmt.Println("Unknown command: ", command)
			break
		}
		res, err := handler(readMsg.Array[1:]).Marshal()
		if err != nil {
			fmt.Println("Error marshaling response for command:", command, "-", err.Error())
			continue
		}
		conn.Write(res)
	}
}
