package main

import (
	"github.com/jgrecu/redis-clone/app/resp"
	"github.com/jgrecu/redis-clone/app/structures"
)

var handlers = map[string]func([]*resp.RESP) *resp.RESP{
	"GET": structures.Get,
	"SET": structures.Set,
}
