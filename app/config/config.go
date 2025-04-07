package config

import (
	"flag"
	"fmt"
	"github.com/jgrecu/redis-clone/app/resp"
	"net"
	"strings"
	"sync"
)

type Config struct {
	Role             string
	Dir              string
	DbFileName       string
	Port             string
	MasterHost       string
	MasterPort       string
	MasterReplId     string
	MasterReplOffset string
	Replicas         []Node
	Master           Node
}

var (
	configs  *Config = &Config{}
	once     sync.Once
	fieldMap = map[string]*string{
		"dir":                &configs.Dir,
		"dbFileName":         &configs.DbFileName,
		"port":               &configs.Port,
		"master_host":        &configs.MasterHost,
		"master_port":        &configs.MasterPort,
		"master_replid":      &configs.MasterReplId,
		"master_repl_offset": &configs.MasterReplOffset,
	}
)

func Get() *Config {
	once.Do(func() {
		dir := flag.String("dir", "", "Directory for the RDB file")
		dbFileName := flag.String("dbfilename", "dump.rdb", "Filename to save the DB to")
		port := flag.String("port", "6379", "Port to listen on")
		replicaof := flag.String("replicaof", "", "Replicate to another Redis server")
		flag.Parse()

		configs.Dir = *dir
		configs.DbFileName = *dbFileName
		configs.Port = *port
		configs.Role = "master"

		if *replicaof != "" {
			configs.Role = "slave"
			configs.MasterHost = strings.Split(*replicaof, " ")[0]
			configs.MasterPort = strings.Split(*replicaof, " ")[1]
			masterConn, err := net.Dial("tcp", configs.MasterHost+":"+configs.MasterPort)
			if err != nil {
				fmt.Println("Error connecting to master: ", err.Error())
			} else {
				configs.Master = NewNode(masterConn)
			}
		} else {
			configs.MasterReplId = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
			configs.MasterReplOffset = "0"
		}

		configs.Replicas = make([]Node, 0)
	})

	return configs
}

func GetConfigHandler(params []resp.RESP) []byte {
	if len(params) > 1 && params[0].Bulk == "GET" {
		value, ok := fieldMap[params[1].Bulk]
		if !ok {
			return resp.Nil().Marshal()
		}

		return resp.Array(
			resp.Bulk(params[1].Bulk),
			resp.Bulk(*value),
		).Marshal()
	}

	return resp.Error("ERR wrong number of arguments for 'config' command").Marshal()
}

func AddReplica(conn net.Conn) {
	configs.Replicas = append(configs.Replicas, NewNode(conn))
}
