package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/jgrecu/redis-clone/app/rdb"
	"github.com/jgrecu/redis-clone/app/resp"
	"github.com/jgrecu/redis-clone/app/structures"
	"log"
	"net"
	"os"
	"strings"
)

func main() {
	// read configs flag
	dir := flag.String("dir", "", "Directory to serve files from")
	dbFileName := flag.String("dbFileName", "dump.rdb", "Filename to save the Database to")
	flag.Parse()

	SetConfig("dir", *dir)
	SetConfig("dbFileName", *dbFileName)

	initializeMapStore(*dir, *dbFileName)

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

		handler, ok := Handlers[command]
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

func initializeMapStore(dir, dbFileName string) {
	// load rdb
	redisDB, err := rdb.ReadFromRDB(dir, dbFileName)
	if err != nil {
		fmt.Println("Error loading Database from file: ", err.Error())
	} else {
		structures.LoadKeys(redisDB)
	}
}
