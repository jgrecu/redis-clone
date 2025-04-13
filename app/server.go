package main

import (
	"github.com/jgrecu/redis-clone/app/config"
	"github.com/jgrecu/redis-clone/app/rdb"
	respConnection "github.com/jgrecu/redis-clone/app/resp-connection"
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

		client := respConnection.NewRespConn(conn)
		go client.Listen()
	}
}

func setup() error {
	// initialise the map if edb file is found
	initializeMapStore()

	// if handle the replica if it's a slave
	if config.Get().Role == "slave" {
		masterHost := config.Get().MasterHost
		masterPort := config.Get().MasterPort
		masterConn, err := net.Dial("tcp", masterHost+":"+masterPort)
		if err != nil {
			log.Println("Failed to connect to master: ", err.Error())
			return err
		}

		master := respConnection.NewRespConn(masterConn)
		master.HandleShake()
		errChan := make(chan error)
		go master.ListenOnMaster(errChan)

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
