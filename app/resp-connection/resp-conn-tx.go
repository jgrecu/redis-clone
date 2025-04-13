package respConnection

import (
	"fmt"
	"github.com/jgrecu/redis-clone/app/handlers"
	"github.com/jgrecu/redis-clone/app/resp"
	"strings"
)

func (c *RespConn) GetTxHandler(command string) handlers.CommandHandler {
	switch command {
	case "MULTI":
		return c.Multi
	case "EXEC":
		return c.Exec
	case "DISCARD":
		return c.Discard
	default:
		return nil
	}
}

func (c *RespConn) Multi(params []resp.RESP) []byte {
	c.TxQueue = make([][]resp.RESP, 0)
	return resp.String("OK").Marshal()
}

func (c *RespConn) Exec(params []resp.RESP) []byte {
	if c.TxQueue != nil {

		buf := []byte(fmt.Sprintf("*%d\r\n", len(c.TxQueue)))

		for _, agrs := range c.TxQueue {
			handler := handlers.GetHandler(strings.ToUpper(agrs[0].Bulk))
			handlerResponse := handler(agrs[1:])

			buf = append(buf, handlerResponse...)
		}

		c.TxQueue = nil
		return buf
	}

	return resp.Error("ERR EXEC without MULTI").Marshal()
}

func (c *RespConn) Discard(params []resp.RESP) []byte {
	if c.TxQueue != nil {
		c.TxQueue = nil
		return resp.String("OK").Marshal()
	}

	return resp.Error("ERR Discard without MULTI").Marshal()
}
