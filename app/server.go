package main

import (
	"bufio"
	"github.com/jgrecu/redis-clone/app/config"
	"github.com/jgrecu/redis-clone/app/handlers"
	"github.com/jgrecu/redis-clone/app/rdb"
	"github.com/jgrecu/redis-clone/app/resp"
	"github.com/jgrecu/redis-clone/app/structures"
	"log"
	"net"
	"os"
)

func main() {
	conf := config.Get()

	setup()

	l, err := net.Listen("tcp", "0.0.0.0:"+conf.Port)
	if err != nil {
		log.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer l.Close()

	for {
		conn, err := l.Accept()
		if err != nil {
			log.Println("Error accepting connection: ", err.Error())
			break
		}

		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	//fmt.Println("Received connection: ", conn.RemoteAddr().String())

	reader := resp.NewRespReader(bufio.NewReader(conn))
	for {
		readMsg, err := reader.Read()
		if err != nil {
			log.Println("Error reading from connection: ", err.Error())
			break
		}

		if readMsg.Type != "array" || len(readMsg.Array) < 1 {
			log.Println("Invalid command")
			break
		}

		err = handlers.Handle(conn, readMsg.Array)
		if err != nil {
			log.Println("this is a replica")
			return
		}
	}
}

func setup() error {
	// initialise the map if edb file is found
	initializeMapStore()

	// handle the replica if it's a slave
	if config.Get().Role == "slave" {
		HandShake()
		errChan := make(chan error)
		ListenMaster(errChan)

		go func() {
			err := <-errChan
			log.Println("Error reading from master: ", err.Error())
		}()
	}

	return nil
}
func initializeMapStore() {
	redisDB, err := rdb.ReadFromRDB(config.Get().Dir, config.Get().DbFileName)
	if err != nil {
		log.Println("Error loading Database from file: ", err.Error())
	} else {
		structures.LoadKeys(redisDB)
	}
}
