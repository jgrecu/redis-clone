package rdb

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"github.com/jgrecu/redis-clone/app/structures"
	"log"
	"os"
	"strconv"
	"time"
)

type RDB struct {
	dir        string
	dbFileName string
	reader     *bufio.Reader
}

func NewRDB(dir, dbFileName string) (*RDB, error) {
	// Open the file
	file, err := os.Open(dir + "/" + dbFileName)
	if err != nil {
		return nil, err
	}

	return &RDB{
		dir:        dir,
		dbFileName: dbFileName,
		reader:     bufio.NewReader(file),
	}, nil
}

func ReadFromRDB(dir, dbFileName string) (structures.RedisDB, error) {
	rdb, err := NewRDB(dir, dbFileName)
	if err != nil {
		return nil, err
	}

	return rdb.readKeys()
}

func (r *RDB) readKeys() (structures.RedisDB, error) {
	// Read the header
	header := make([]byte, 9)
	r.reader.Read(header)
	if string(header) != "REDIS0011" {
		return nil, fmt.Errorf("invalid RDB file: invalid header: %s", string(header))
	}

	for {
		// Read the type
		typ, err := r.reader.ReadByte()
		if err != nil {
			break
		}
		switch typ {
		case 0xFE:
			log.Println("start reading database info...")
			return r.startDBRead()
		case 0xFF:
			return nil, fmt.Errorf("invalid RDB file: unexpected EOF")
		}
	}
	return nil, fmt.Errorf("invalid RDB file: unexpected EOF")
}

func (r *RDB) startDBRead() (structures.RedisDB, error) {
	// read db index
	_, err := r.readSizeEncoded()
	if err != nil {
		return nil, err
	}

	redisDB := structures.RedisDB{}
	currentExpiry := time.Time{}

	for {
		it, err := r.reader.ReadByte()
		if err != nil {
			return nil, err
		}

		switch it {
		case 0xFB: // db info
			_, err := r.readSizeEncoded()
			if err != nil {
				return nil, err
			}

			_, err = r.readSizeEncoded()
			if err != nil {
				return nil, err
			}

		case 0x00: // type string
			key, err := r.readString()
			if err != nil {
				return nil, err
			}

			value, err := r.readString()
			if err != nil {
				return nil, err
			}
			redisDB[key] = structures.MapValue{
				Value:  value,
				Expiry: currentExpiry,
			}

			currentExpiry = time.Time{}
		case 0xFC: // the current key has expiry in milliseconds (ms)
			timestampBytes := make([]byte, 8)
			r.reader.Read(timestampBytes)
			timestamp := binary.LittleEndian.Uint64(timestampBytes)

			currentExpiry = time.Unix(0, int64(timestamp)*int64(time.Millisecond))
		case 0xFD: // the current key has expiry in seconds (s)
			timestampBytes := make([]byte, 8)
			r.reader.Read(timestampBytes)
			timestamp := binary.LittleEndian.Uint64(timestampBytes)

			currentExpiry = time.Unix(0, int64(timestamp)*int64(time.Second))
		case 0xFF: // end of file?
			return redisDB, nil
		}
	}
}

func (r *RDB) readSizeEncoded() (int, error) {
	firstByte, err := r.reader.ReadByte()
	if err != nil {
		return 0, fmt.Errorf("invalid RDB file: encoded size, error reading first byte")
	}

	switch firstByte >> 6 {
	case 0b00:
		return int(firstByte), nil
	case 0b01:
		nextByte, err := r.reader.ReadByte()
		if err != nil {
			return 0, fmt.Errorf("invalid RDB file: encoded size, error reading second byte")
		}
		return int(firstByte&0b00111111)<<8 | int(nextByte), nil
	case 0b10:
		sizeBytes := make([]byte, 4)
		r.reader.Read(sizeBytes)

		return int(binary.BigEndian.Uint32(sizeBytes)), nil
	case 0b11:
		format := int(firstByte&0b00111111) + 1
		sizeBytes := make([]byte, format)
		r.reader.Read(sizeBytes)

		stringSize := string(sizeBytes)
		return strconv.Atoi(stringSize)
	}

	return 0, nil
}

func (r *RDB) readString() (string, error) {
	size, err := r.readSizeEncoded()
	if err != nil {
		return "", err
	}

	buf := make([]byte, size)
	r.reader.Read(buf)
	return string(buf), nil
}
