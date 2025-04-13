package structures

import "strconv"

func Incr(key string) (int, error) {
	item, ok := getMapValue(key)
	if !ok {
		setMapValue(key, MapValue{
			Typ:    "string",
			String: "1",
		})

		return 1, nil
	}

	intValue, err := strconv.Atoi(item.String)
	if err != nil {
		return 0, err
	}

	intValue++

	item.String = strconv.Itoa(intValue)

	setMapValue(key, item)

	return intValue, nil
}
