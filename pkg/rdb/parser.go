package rdb

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"time"

	"github.com/jgrecu/redis-clone/pkg/storage"
)

type Database struct {
	Index         uint32
	Keys          map[string]storage.Item
	KeysHTSize    *uint32
	ExpiresHTSize *uint32
}

type RDBParser struct {
	filePath string
}

func NewRDBParser(path string) *RDBParser {
	return &RDBParser{filePath: path}
}

func (r *RDBParser) ReadLength(reader *bufio.Reader) (uint32, error) {
	b, err := reader.ReadByte()
	if err != nil {
		return 0, err
	}

	switch b & 0xC0 {
	case 0x00: // 6-bit length
		return uint32(b & 0x3F), nil
	case 0x40: // 14-bit length
		b2, err := reader.ReadByte()
		if err != nil {
			return 0, err
		}
		return uint32((b&0x3F)<<8 | b2), nil
	case 0x80: // 32-bit length
		lengthBytes := make([]byte, 4)
		_, err := io.ReadFull(reader, lengthBytes)
		if err != nil {
			return 0, err
		}
		return binary.BigEndian.Uint32(lengthBytes), nil
	default:
		return 0, fmt.Errorf("invalid length encoding")
	}
}

func (r *RDBParser) ReadString(reader *bufio.Reader) (string, error) {
	length, err := r.ReadLength(reader)
	if err != nil {
		return "", err
	}

	strBytes := make([]byte, length)
	_, err = io.ReadFull(reader, strBytes)
	if err != nil {
		return "", err
	}

	return string(strBytes), nil
}

func (r *RDBParser) ParseRDB() ([]Database, error) {
	file, err := os.Open(r.filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := bufio.NewReader(file)

	// Verify header
	headerBytes := make([]byte, 9)
	_, err = io.ReadFull(reader, headerBytes)
	if err != nil || string(headerBytes) != "REDIS0011" {
		return nil, fmt.Errorf("invalid RDB file header: %s", string(headerBytes))
	}

	databases := []Database{}

	for {
		// Read database marker
		marker, err := reader.ReadByte()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		// Database subsection start
		if marker == 0xFE {
			// Read database index
			dbIndex, err := r.ReadLength(reader)
			if err != nil {
				return nil, err
			}

			currentDB := Database{
				Index: dbIndex,
				Keys:  make(map[string]storage.Item),
			}

			// Optional hash table size info
			next, err := reader.ReadByte()
			if err != nil {
				return nil, err
			}

			if next == 0xFB {
				keysHTSize, err := r.ReadLength(reader)
				if err != nil {
					return nil, err
				}
				expiresHTSize, err := r.ReadLength(reader)
				if err != nil {
					return nil, err
				}
				currentDB.KeysHTSize = &keysHTSize
				currentDB.ExpiresHTSize = &expiresHTSize
				next, err = reader.ReadByte()
				if err != nil {
					return nil, err
				}
			}

			// Process key-value pairs
			for next != 0xFE && next != 0xFF {
				// Check for expire information
				var expire *time.Time
				if next == 0xFD {
					// Expire in seconds
					expireBytes := make([]byte, 4)
					_, err := io.ReadFull(reader, expireBytes)
					if err != nil {
						return nil, err
					}
					expireTime := time.Unix(int64(binary.LittleEndian.Uint32(expireBytes)), 0)
					expire = &expireTime
					next, err = reader.ReadByte()
					if err != nil {
						return nil, err
					}
				} else if next == 0xFC {
					// Expire in milliseconds
					expireBytes := make([]byte, 8)
					_, err := io.ReadFull(reader, expireBytes)
					if err != nil {
						return nil, err
					}
					expireTime := time.Unix(0, int64(binary.LittleEndian.Uint64(expireBytes))*int64(time.Millisecond))
					expire = &expireTime
					next, err = reader.ReadByte()
					if err != nil {
						return nil, err
					}
				}

				// Value type and key-value pair
				valueType := storage.ValueType(next)
				key, err := r.ReadString(reader)
				if err != nil {
					return nil, err
				}

				var value string
				if valueType == storage.String {
					value, err = r.ReadString(reader)
					if err != nil {
						return nil, err
					}
				}

				// Store key-value pair
				currentDB.Keys[key] = storage.Item{
					Value:  value,
					Expire: expire,
					Type:   valueType,
				}

				// Read next marker
				next, err = reader.ReadByte()
				if err != nil {
					return nil, err
				}
			}

			databases = append(databases, currentDB)

			// If we've reached EOF, read checksum and break
			if next == 0xFF {
				checksum := make([]byte, 8)
				_, err := io.ReadFull(reader, checksum)
				if err != nil {
					return nil, err
				}
				break
			}
		}
	}

	return databases, nil
}

func (w *RDBParser) writeLength(writer *bufio.Writer, length uint32) error {
	if length < 64 {
		return writer.WriteByte(byte(length))
	} else if length < 16384 {
		if err := writer.WriteByte(0x40 | byte(length>>8)); err != nil {
			return err
		}
		return writer.WriteByte(byte(length & 0xFF))
	} else {
		if err := writer.WriteByte(0x80); err != nil {
			return err
		}
		lengthBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(lengthBytes, length)
		_, err := writer.Write(lengthBytes)
		return err
	}
}

func (w *RDBParser) writeString(writer *bufio.Writer, s string) error {
	// Write length
	length := uint32(len(s))
	if err := w.writeLength(writer, length); err != nil {
		return err
	}

	// Write string
	_, err := writer.WriteString(s)
	return err
}

func (w *RDBParser) calculateChecksum(filePath string) (uint64, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return 0, err
	}
	return uint64(crc32.ChecksumIEEE(data)), nil
}

func (w *RDBParser) SaveRDB(databases []Database) error {
	file, err := os.Create(w.filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	defer writer.Flush()

	// Write header
	if _, err := writer.WriteString("REDIS0011"); err != nil {
		return err
	}

	// Write databases
	for _, db := range databases {
		// Database subsection marker
		if err := writer.WriteByte(0xFE); err != nil {
			return err
		}

		// Database index
		if err := w.writeLength(writer, db.Index); err != nil {
			return err
		}

		// Optional hash table size
		if db.KeysHTSize != nil {
			if err := writer.WriteByte(0xFB); err != nil {
				return err
			}
			if err := w.writeLength(writer, *db.KeysHTSize); err != nil {
				return err
			}
			expiresHTSize := uint32(0)
			if db.ExpiresHTSize != nil {
				expiresHTSize = *db.ExpiresHTSize
			}
			if err := w.writeLength(writer, expiresHTSize); err != nil {
				return err
			}
		}

		// Write key-value pairs
		for key, entry := range db.Keys {
			// Handle expiration
			if entry.Expire != nil {
				// Write millisecond expiration
				if err := writer.WriteByte(0xFC); err != nil {
					return err
				}
				expireMs := uint64(entry.Expire.UnixNano() / int64(time.Millisecond))
				expireMsBytes := make([]byte, 8)
				binary.LittleEndian.PutUint64(expireMsBytes, expireMs)
				if _, err := writer.Write(expireMsBytes); err != nil {
					return err
				}
			}

			// Write value type (string)
			if err := writer.WriteByte(byte(storage.String)); err != nil {
				return err
			}

			// Write key
			if err := w.writeString(writer, key); err != nil {
				return err
			}

			// Write value
			if err := w.writeString(writer, entry.Value); err != nil {
				return err
			}
		}
	}

	// End of file marker
	if err := writer.WriteByte(0xFF); err != nil {
		return err
	}

	// Flush writer
	if err := writer.Flush(); err != nil {
		return err
	}

	// Calculate and write checksum
	checksum, err := w.calculateChecksum(w.filePath)
	if err != nil {
		return err
	}
	checksumBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(checksumBytes, checksum)
	if _, err := file.Write(checksumBytes); err != nil {
		return err
	}

	return nil
}
