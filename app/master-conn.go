package main

import (
	"fmt"
	"github.com/jgrecu/redis-clone/app/config"
	"github.com/jgrecu/redis-clone/app/handlers"
	"github.com/jgrecu/redis-clone/app/resp"
)

func HandShake() error {
	r := config.Get().Master

	r.Write(resp.Command("PING").Marshal())
	value, _ := r.Read()

	r.Write(resp.Command("REPLCONF", "listening-port", config.Get().Port).Marshal())
	value, _ = r.Read()

	r.Write(resp.Command("REPLCONF", "capa", "psync2").Marshal())
	value, _ = r.Read()

	r.Write(resp.Command("PSYNC", "?", "-1").Marshal())
	value, _ = r.Read()    // +FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0
	value, _ = r.ReadRDB() // +RDBFIILE
	fmt.Printf("Received response from master: %v\n", value.Bulk)

	return nil
}

func ListenMaster(errChan chan error) {
	go listening(errChan)
}

func listening(errChan chan error) {
	master := config.Get().Master
	for {
		value, err := master.Read()
		if err != nil {
			errChan <- err
			continue
		}

		if value.Type == "array" && len(value.Array) > 0 {
			handlers.HandleMaster(master.Conn, value.Array)
		} else {
			errChan <- fmt.Errorf("invalid command")
		}
	}
}
