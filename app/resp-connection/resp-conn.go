package respConnection

import (
    "bufio"
    "github.com/jgrecu/redis-clone/app/handlers"
    "github.com/jgrecu/redis-clone/app/resp"
    "log"
    "net"
    "strconv"
    "strings"
    "sync"
)

type RespConn struct {
    Conn     net.Conn
    Reader   *resp.RespReader
    offset   int
    id       string
    mu       sync.Mutex
    AckChans []chan int
    TxQueue  []*resp.RESP
}

func NewRespConn(conn net.Conn) *RespConn {
    log.Println("New connection from: ", conn.RemoteAddr().String())
    return &RespConn{
        Conn:     conn,
        Reader:   resp.NewRespReader(bufio.NewReader(conn)),
        offset:   0,
        id:       conn.RemoteAddr().String(),
        mu:       sync.Mutex{},
        AckChans: make([]chan int, 0),
        TxQueue:  nil,
    }
}

func (c *RespConn) Close() {
    c.Conn.Close()
}

func (c *RespConn) Id() string {
    return c.id
}

func (c *RespConn) Listen() {
    for {
        value, err := c.Reader.Read()
        if err != nil {
            break
        }

        if value.Type != "array" || len(value.Array) < 1 {
            break
        }
        c.handleClient(value.Array)
    }

    c.Close()
}

func (c *RespConn) handleClient(args []resp.RESP) error {
    command := strings.ToUpper(args[0].Bulk)

    // handle replication commands
    if command == "REPLCONF" && strings.ToUpper(args[1].Bulk) == "ACK" {
        offset, _ := strconv.Atoi(args[2].Bulk)
        go c.AckReceived(offset)
        return nil
    }

    if command == "WAIT" {
        c.Write(Wait(args[1:]))
    }

    // handle tx commands
    handler := c.GetTxHandler(command)
    if handler == nil {
        // if no tx handler is found, use the default handler
        handler = handlers.GetHandler(command)
    }

    c.Conn.Write(handler(args[1:]))

    if command == "PSYNC" {
        GetReplicaManager().AddReplica(c)
        return nil
    }

    // Propagate the command to all replicas
    if isWriteCommand(command) {
        GetReplicaManager().PropagateCommand(args)
    }

    return nil
}

func isWriteCommand(command string) bool {
    return command == "SET" || command == "DEL"
}

func (c *RespConn) AddOffset(offset int) {
    c.mu.Lock()
    defer c.mu.Unlock()
    c.offset += offset
}

func (c *RespConn) GetOffset() int {
    c.mu.Lock()
    defer c.mu.Unlock()
    return c.offset
}

func (c *RespConn) Read() (resp.RESP, error) {
    return c.Reader.Read()
}

func (c *RespConn) Write(data []byte) (int, error) {
    return c.Conn.Write(data)
}

func (c *RespConn) ReadRDB() (resp.RESP, error) {
    return c.Reader.ReadRDB()
}

func Wait(params []resp.RESP) []byte {
    log.Println("Received WAIT command: ", params)
    count, _ := strconv.Atoi(params[0].Bulk)
    timeout, _ := strconv.Atoi(params[1].Bulk)
    acks := GetReplicaManager().SendAck(timeout, count)

    return resp.Integer(acks).Marshal()
}
