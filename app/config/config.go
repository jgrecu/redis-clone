package config

import "github.com/jgrecu/redis-clone/app/resp"

var configs = make(map[string]string)

func Set(key, value string) {
	configs[key] = value
}

func Get(key string) string {
	value := configs[key]
	return value
}

func GetConfigHandler(params []resp.RESP) resp.RESP {
	if len(params) > 1 && params[0].Bulk == "GET" {
		value, ok := configs[params[1].Bulk]
		if !ok {
			return resp.RESP{
				Type: "nil",
			}
		}

		return resp.RESP{
			Type: "array",
			Array: []resp.RESP{
				{
					Type: "bulk",
					Bulk: params[1].Bulk,
				},
				{
					Type: "bulk",
					Bulk: value,
				},
			},
		}
	}

	return resp.RESP{
		Type: "error",
		Bulk: "CONFIG GET: Invalid command",
	}
}
