package respConnection

import "github.com/jgrecu/redis-clone/app/resp"

func (c *RespConn) SendAck(ack chan int) (int, error) {
	c.mu.Lock()
	c.AckChans = append(c.AckChans, ack)
	c.mu.Unlock()

	s, err := c.Conn.Write(
		resp.Command("REPLCONF", "GETACK", "*").Marshal(),
	)
	c.AddOffset(s)
	return s, err
}

func (c *RespConn) AckReceived(offset int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	cha := c.AckChans[0]
	c.AckChans = c.AckChans[1:]
	cha <- offset
}
