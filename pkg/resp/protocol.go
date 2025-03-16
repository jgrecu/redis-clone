package resp

import (
	"bytes"
	"fmt"
	"strconv"
)

const (
	ArrayPrefix  = '*'
	BulkPrefix   = '$'
	SimplePrefix = '+'
	ErrorPrefix  = '-'
)

// Message represents a RESP protocol message
type Message struct {
	Type    byte
	Length  int
	Content []string
}

// Parser handles RESP protocol parsing
type Parser struct{}

// NewParser creates a new RESP parser
func NewParser() *Parser {
	return &Parser{}
}

// Parse parses a RESP message
func (p *Parser) Parse(input []byte) (*Message, error) {
	if len(input) == 0 {
		return nil, fmt.Errorf("empty input")
	}

	msg := &Message{Type: input[0]}

	switch msg.Type {
	case ArrayPrefix:
		return p.parseArray(input)
	case BulkPrefix:
		content, err := p.parseBulkString(input)
		if err != nil {
			return nil, err
		}
		msg.Content = []string{content}
		return msg, nil
	default:
		return nil, fmt.Errorf("unsupported message type: %c", msg.Type)
	}
}

func (p *Parser) parseArray(input []byte) (*Message, error) {
	// Parse length
	lengthEnd := bytes.Index(input[1:], []byte("\r\n"))
	if lengthEnd == -1 {
		return nil, fmt.Errorf("invalid array format")
	}

	length, err := strconv.Atoi(string(input[1 : lengthEnd+1]))
	if err != nil {
		return nil, fmt.Errorf("invalid array length: %w", err)
	}

	msg := &Message{
		Type:    ArrayPrefix,
		Length:  length,
		Content: make([]string, 0, length),
	}

	// Parse bulk strings
	pos := lengthEnd + 3 // Skip array header

	for i := 0; i < length; i++ {
		if pos >= len(input) {
			return nil, fmt.Errorf("incomplete array")
		}

		content, newPos, err := p.parseBulkStringWithPos(input[pos:])
		if err != nil {
			return nil, err
		}
		msg.Content = append(msg.Content, content)
		pos += newPos
	}

	return msg, nil
}

func (p *Parser) parseBulkString(input []byte) (string, error) {
	content, _, err := p.parseBulkStringWithPos(input)
	return content, err
}

func (p *Parser) parseBulkStringWithPos(input []byte) (string, int, error) {
	if input[0] != BulkPrefix {
		return "", 0, fmt.Errorf("invalid bulk string format")
	}

	// Parse length
	lengthEnd := bytes.Index(input[1:], []byte("\r\n"))
	if lengthEnd == -1 {
		return "", 0, fmt.Errorf("invalid bulk string format")
	}
	lengthEnd++ // Adjust for the slice offset

	length, err := strconv.Atoi(string(input[1:lengthEnd]))
	if err != nil {
		return "", 0, fmt.Errorf("invalid length: %w", err)
	}

	if length < 0 {
		return "", 0, fmt.Errorf("negative length not allowed")
	}

	// Calculate positions
	contentStart := lengthEnd + 2
	contentEnd := contentStart + length

	if contentEnd+2 > len(input) {
		return "", 0, fmt.Errorf("incomplete bulk string")
	}

	return string(input[contentStart:contentEnd]), contentEnd + 2, nil
}

// Writer handles RESP protocol responses
type Writer struct{}

// NewWriter creates a new RESP writer
func NewWriter() *Writer {
	return &Writer{}
}

// WriteSimpleString writes a simple string response
func (w *Writer) WriteSimpleString(s string) []byte {
	return []byte(fmt.Sprintf("%c%s\r\n", SimplePrefix, s))
}

// WriteBulkString writes a bulk string response
func (w *Writer) WriteBulkString(s string) []byte {
	return []byte(fmt.Sprintf("%c%d\r\n%s\r\n", BulkPrefix, len(s), s))
}

// WriteError writes an error response
func (w *Writer) WriteError(s string) []byte {
	return []byte(fmt.Sprintf("%c%s\r\n", ErrorPrefix, s))
}

// WriteNullBulk writes a null bulk string response
func (w *Writer) WriteNullBulk() []byte {
	return []byte(fmt.Sprintf("%c-1\r\n", BulkPrefix))
}

// WriteArray writes an array response
func (w *Writer) WriteArray(s []string) []byte {
	// Start with array length indicator
	resp := []byte(fmt.Sprintf("%c%d\r\n", ArrayPrefix, len(s)))

	// Append each string in the array
	for _, str := range s {
		resp = append(resp, []byte(fmt.Sprintf("%c%d\r\n%s\r\n", BulkPrefix, len(str), str))...)
	}
	return resp
}
