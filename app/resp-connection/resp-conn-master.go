package respConnection

import (
	"errors"
	"github.com/jgrecu/redis-clone/app/config"
	"github.com/jgrecu/redis-clone/app/handlers"
	"github.com/jgrecu/redis-clone/app/resp"
	"strings"
)

func (r *RespConn) HandleShake() error {
	r.Write(resp.Command("PING").Marshal())
	r.Read()

	r.Write(resp.Command("REPLCONF", "listening-port", config.Get().Port).Marshal())
	r.Read()

	r.Write(resp.Command("REPLCONF", "capa", "psync2").Marshal())
	r.Read()

	r.Write(resp.Command("PSYNC", "?", "-1").Marshal())
	r.Read()    // +FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0
	r.ReadRDB() // +RDBFIILE

	return nil
}

func (r *RespConn) handleMaster(args []resp.RESP) {
	command := strings.ToUpper(args[0].Bulk)
	handler := handlers.GetHandler(command)

	data := handler(args[1:])

	if command == "REPLCONF" && strings.ToUpper(args[1].Bulk) == "GETACK" {
		r.Write(data)
	}
}

func (r *RespConn) ListenOnMaster(errChan chan error) {
	for {
		value, err := r.Read()
		if err != nil {
			errChan <- err
			continue
		}

		if value.Type == "array" && len(value.Array) > 0 {
			r.handleMaster(value.Array)
		} else {
			errChan <- errors.New("invalid command")
		}

		config.IncreaseOffset(len(value.Marshal()))
	}
}
