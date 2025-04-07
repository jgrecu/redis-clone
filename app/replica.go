package main

import (
	"github.com/jgrecu/redis-clone/app/resp"
	"net"
)

func handShake(link string) error {
	conn, err := net.Dial("tcp", link)
	if err != nil {
		return err
	}
	defer conn.Close()

	// send ping
	ping, _ := resp.RESP{
		Type: "array",
		Array: []resp.RESP{
			{
				Type: "bulk",
				Bulk: "PING",
			},
		},
	}.Marshal()

	conn.Write(ping)
	return nil
}
