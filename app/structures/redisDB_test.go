package structures

import (
	"github.com/jgrecu/redis-clone/app/resp"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestGet(t *testing.T) {
	// Setup
	mapStore = make(RedisDB)
	mapStore["testKey"] = MapValue{
		Typ:    "string",
		String: "testValue",
		Expiry: time.Time{},
	}

	// Test cases
	tests := []struct {
		name     string
		params   []resp.RESP
		expected []byte
	}{
		{
			name: "Get existing key",
			params: []resp.RESP{
				{Type: "bulk", Bulk: "testKey"},
			},
			expected: resp.Bulk("testValue").Marshal(),
		},
		{
			name: "Get non-existing key",
			params: []resp.RESP{
				{Type: "bulk", Bulk: "nonExistingKey"},
			},
			expected: resp.Nil().Marshal(),
		},
		{
			name:     "Get with wrong number of arguments",
			params:   []resp.RESP{},
			expected: resp.Error("ERR wrong number of arguments for 'get' command").Marshal(),
		},
	}

	// Run tests
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Get(tt.params)
			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("Get() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestSet(t *testing.T) {
	// Setup
	mapStore = make(RedisDB)

	// Test cases
	tests := []struct {
		name     string
		params   []resp.RESP
		expected []byte
		check    func() bool
	}{
		{
			name: "Set simple key-value",
			params: []resp.RESP{
				{Type: "bulk", Bulk: "testKey"},
				{Type: "bulk", Bulk: "testValue"},
			},
			expected: resp.String("OK").Marshal(),
			check: func() bool {
				val, ok := mapStore["testKey"]
				return ok && val.String == "testValue" && val.Typ == "string"
			},
		},
		{
			name: "Set with expiry",
			params: []resp.RESP{
				{Type: "bulk", Bulk: "expiryKey"},
				{Type: "bulk", Bulk: "expiryValue"},
				{Type: "bulk", Bulk: "PX"},
				{Type: "bulk", Bulk: "100"},
			},
			expected: resp.String("OK").Marshal(),
			check: func() bool {
				val, ok := mapStore["expiryKey"]
				return ok && val.String == "expiryValue" && !val.Expiry.IsZero()
			},
		},
		{
			name: "Set with wrong number of arguments",
			params: []resp.RESP{
				{Type: "bulk", Bulk: "testKey"},
			},
			expected: resp.Error("ERR wrong number of arguments for 'set' command").Marshal(),
			check:    func() bool { return true },
		},
	}

	// Run tests
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Set(tt.params)
			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("Set() = %v, want %v", result, tt.expected)
			}
			if !tt.check() {
				t.Errorf("Set() did not properly update the mapStore")
			}
		})
	}
}

func TestKeys(t *testing.T) {
	// Setup
	mapStore = make(RedisDB)
	mapStore["key1"] = MapValue{Typ: "string", String: "value1"}
	mapStore["key2"] = MapValue{Typ: "string", String: "value2"}

	// Test
	params := []resp.RESP{{Type: "bulk", Bulk: "*"}}
	result := Keys(params)

	// Since we don't have an Unmarshal function, we'll check the result manually
	// The result should be a RESP array with 2 elements
	// Format: *2\r\n$4\r\nkey1\r\n$4\r\nkey2\r\n or *2\r\n$4\r\nkey2\r\n$4\r\nkey1\r\n

	// Convert result to string for easier inspection
	resultStr := string(result)

	// Check if it starts with *2 (array of 2 elements)
	if resultStr[:2] != "*2" {
		t.Errorf("Keys() should return an array of 2 elements, got: %s", resultStr)
	}

	// Check if both keys are in the result
	if !strings.Contains(resultStr, "key1") || !strings.Contains(resultStr, "key2") {
		t.Errorf("Keys() did not return all expected keys, got: %s", resultStr)
	}
}

func TestExpiry(t *testing.T) {
	// Setup
	mapStore = make(RedisDB)
	mapStore["expiryKey"] = MapValue{
		Typ:    "string",
		String: "expiryValue",
		Expiry: time.Now().Add(50 * time.Millisecond),
	}

	// Test that the key exists before expiry
	params := []resp.RESP{{Type: "bulk", Bulk: "expiryKey"}}
	result := Get(params)
	expected := resp.Bulk("expiryValue").Marshal()
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Get() before expiry = %v, want %v", result, expected)
	}

	// Wait for the key to expire
	time.Sleep(100 * time.Millisecond)

	// Test that the key is gone after expiry
	result = Get(params)
	expected = resp.Nil().Marshal()
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Get() after expiry = %v, want %v", result, expected)
	}
}
