package main

import (
	"github.com/jgrecu/redis-clone/app/config"
	"github.com/jgrecu/redis-clone/app/handlers"
	"github.com/jgrecu/redis-clone/app/rdb"
	respConnection "github.com/jgrecu/redis-clone/app/resp-connection"
	"github.com/jgrecu/redis-clone/app/structures"
	"log"
	"net"
	"os"
)

func main() {
	conf := config.Get()

	store := structures.NewStore()
	router := handlers.NewRouter(store)

	initializeMapStore(store)

	// handle the replica if it's a slave
	if conf.Role == "slave" {
		masterConn, err := net.Dial("tcp", conf.MasterHost+":"+conf.MasterPort)
		if err != nil {
			log.Println("Failed to connect to master: ", err.Error())
			os.Exit(1)
		}

		master := respConnection.NewRespConn(masterConn, router)
		master.HandleShake()
		errChan := make(chan error)
		go master.ListenOnMaster(errChan)

		go func() {
			err := <-errChan
			log.Println("Error reading from master: ", err.Error())
		}()
	}

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

		client := respConnection.NewRespConn(conn, router)
		go client.Listen()
	}
}

func initializeMapStore(store *structures.Store) {
	redisDB, err := rdb.ReadFromRDB(config.Get().Dir, config.Get().DbFileName)
	if err != nil {
		log.Println("Error loading Database from file: ", err.Error())
	} else {
		store.LoadKeys(redisDB)
	}
}
