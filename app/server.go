package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/jgrecu/redis-clone/app/config"
	"github.com/jgrecu/redis-clone/app/handlers"
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
	port := flag.String("port", "6379", "Port to serve on")
	replicaof := flag.String("replicaof", "", "Address of master of server")
	flag.Parse()

	config.Set("dir", *dir)
	config.Set("dbFileName", *dbFileName)
	config.Set("port", *port)
	config.Set("replicaof", *replicaof)

	if *replicaof != "" {
		config.Set("role", "slave")
		masterHost := strings.Split(*replicaof, " ")[0]
		masterPort := strings.Split(*replicaof, " ")[1]
		config.Set("master_host", masterHost)
		config.Set("master_port", masterPort)
	} else {
		config.Set("role", "master")
	}

	config.Set("master_replid", "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb")
	config.Set("master_repl_offset", "0")

	setup()

	l, err := net.Listen("tcp", "0.0.0.0:"+*port)
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

		fmt.Printf("Received command: %s\n", command)

		handler := handlers.GetHandler(command)

		conn.Write(handler(readMsg.Array[1:]))
	}
}

func setup() error {
	// initialise the map if edb file is found
	initializeMapStore()

	// handle the replica if it's a slave
	if config.Get("role") == "slave" {
		masterConn, err := NewMaster()
		if err != nil {
			return err
		}

		masterConn.HandShake()
		errChan := make(chan error)
		masterConn.Listen(errChan)
		go func() {
			err := <-errChan
			fmt.Println("Error reading from master: ", err.Error())
		}()
	}

	return nil
}
func initializeMapStore() {
	// load rdb
	dir := config.Get("dir")
	dbFileName := config.Get("dbFileName")
	redisDB, err := rdb.ReadFromRDB(dir, dbFileName)
	if err != nil {
		fmt.Println("Error loading Database from file: ", err.Error())
	} else {
		structures.LoadKeys(redisDB)
	}
}
