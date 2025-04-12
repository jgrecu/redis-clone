package respConnection

import (
	"github.com/jgrecu/redis-clone/app/handlers"
	"github.com/jgrecu/redis-clone/app/resp"
)

func (c *RespConn) GetTxHandler(command string) handlers.CommandHandler {
	switch command {
	case "MULTI":
		return c.Multi
	case "EXEC":
		return c.Exec
	default:
		return nil
	}
}

func (c *RespConn) Multi(params []resp.RESP) []byte {
	c.TxQueue = make([]*resp.RESP, 0)
	return resp.String("OK").Marshal()
}

func (c *RespConn) Exec(params []resp.RESP) []byte {
	if c.TxQueue != nil {
		c.TxQueue = nil
		return resp.Array().Marshal()
	}

	return resp.Error("ERR EXEC without MULTI").Marshal()
}
