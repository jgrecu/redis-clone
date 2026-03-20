package handlers

import (
	"github.com/jgrecu/redis-clone/app/resp"
	"github.com/jgrecu/redis-clone/app/structures"
	"reflect"
	"strings"
	"testing"
	"time"
)

func newTestRouter() *CommandRouter {
	return NewRouter(structures.NewStore())
}

func TestGetHandler(t *testing.T) {
	router := newTestRouter()

	tests := []struct {
		name          string
		command       string
		shouldBeFound bool
	}{
		{"Existing command - PING", "PING", true},
		{"Existing command - GET", "GET", true},
		{"Non-existing command", "NONEXISTENT", false},
		{"Empty command string", "", false},
		{"Lowercase command (case-sensitive)", "ping", false},
		{"Mixed case command", "Ping", false},
		{"Command with whitespace", " PING ", false},
		{"SET command exists", "SET", true},
		{"ECHO command exists", "ECHO", true},
		{"KEYS command exists", "KEYS", true},
		{"TYPE command exists", "TYPE", true},
		{"XADD command exists", "XADD", true},
		{"XRANGE command exists", "XRANGE", true},
		{"XREAD command exists", "XREAD", true},
		{"INCR command exists", "INCR", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := router.GetHandler(tt.command)

			if tt.shouldBeFound {
				if reflect.ValueOf(handler).Pointer() == reflect.ValueOf(notFound).Pointer() {
					t.Errorf("GetHandler(%q) returned notFound, expected a valid handler", tt.command)
				}
			} else {
				if reflect.ValueOf(handler).Pointer() != reflect.ValueOf(notFound).Pointer() {
					t.Errorf("GetHandler(%q) returned a handler, expected notFound", tt.command)
				}
			}
		})
	}
}

func TestPing(t *testing.T) {
	router := newTestRouter()

	tests := []struct {
		name     string
		params   []resp.RESP
		expected []byte
	}{
		{
			name:     "Simple PING",
			params:   []resp.RESP{},
			expected: resp.String("PONG").Marshal(),
		},
		{
			name:     "PING with argument (ignored)",
			params:   []resp.RESP{{Type: "bulk", Bulk: "hello"}},
			expected: resp.String("PONG").Marshal(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := router.ping(tt.params)
			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("ping() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestEcho(t *testing.T) {
	router := newTestRouter()

	tests := []struct {
		name     string
		params   []resp.RESP
		expected []byte
	}{
		{
			name:     "Echo hello",
			params:   []resp.RESP{{Type: "bulk", Bulk: "hello"}},
			expected: resp.String("hello").Marshal(),
		},
		{
			name:     "Echo empty string",
			params:   []resp.RESP{{Type: "bulk", Bulk: ""}},
			expected: resp.String("").Marshal(),
		},
		{
			name:     "Echo with spaces",
			params:   []resp.RESP{{Type: "bulk", Bulk: "hello world"}},
			expected: resp.String("hello world").Marshal(),
		},
		{
			name:     "Echo with special characters",
			params:   []resp.RESP{{Type: "bulk", Bulk: "!@#$%^&*()"}},
			expected: resp.String("!@#$%^&*()").Marshal(),
		},
		{
			name:     "Echo with unicode",
			params:   []resp.RESP{{Type: "bulk", Bulk: "héllo wörld"}},
			expected: resp.String("héllo wörld").Marshal(),
		},
		{
			name: "Echo with multiple params returns only first",
			params: []resp.RESP{
				{Type: "bulk", Bulk: "first"},
				{Type: "bulk", Bulk: "second"},
			},
			expected: resp.String("first").Marshal(),
		},
		{
			name:     "Echo with newline characters",
			params:   []resp.RESP{{Type: "bulk", Bulk: "line1\nline2"}},
			expected: resp.String("line1\nline2").Marshal(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := router.echo(tt.params)
			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("echo() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestNotFound(t *testing.T) {
	result := notFound([]resp.RESP{})
	expected := resp.Error("Command not found").Marshal()

	if !reflect.DeepEqual(result, expected) {
		t.Errorf("notFound() = %v, want %v", result, expected)
	}
}

func TestGet(t *testing.T) {
	tests := []struct {
		name     string
		setup    func(s *structures.Store)
		params   []resp.RESP
		expected []byte
	}{
		{
			name: "Get existing key",
			setup: func(s *structures.Store) {
				s.Set("testKey", "testValue", time.Time{})
			},
			params:   []resp.RESP{{Type: "bulk", Bulk: "testKey"}},
			expected: resp.Bulk("testValue").Marshal(),
		},
		{
			name:     "Get non-existing key",
			setup:    func(s *structures.Store) {},
			params:   []resp.RESP{{Type: "bulk", Bulk: "nonExistingKey"}},
			expected: resp.Nil().Marshal(),
		},
		{
			name:     "Get with wrong number of arguments",
			setup:    func(s *structures.Store) {},
			params:   []resp.RESP{},
			expected: resp.Error("ERR wrong number of arguments for 'get' command").Marshal(),
		},
		{
			name:  "Get with too many arguments",
			setup: func(s *structures.Store) {},
			params: []resp.RESP{
				{Type: "bulk", Bulk: "k1"},
				{Type: "bulk", Bulk: "k2"},
			},
			expected: resp.Error("ERR wrong number of arguments for 'get' command").Marshal(),
		},
		{
			name: "Get expired key",
			setup: func(s *structures.Store) {
				s.Set("expired", "gone", time.Now().Add(-1*time.Second))
			},
			params:   []resp.RESP{{Type: "bulk", Bulk: "expired"}},
			expected: resp.Nil().Marshal(),
		},
		{
			name: "Get key with empty value",
			setup: func(s *structures.Store) {
				s.Set("emptyVal", "", time.Time{})
			},
			params:   []resp.RESP{{Type: "bulk", Bulk: "emptyVal"}},
			expected: resp.Bulk("").Marshal(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := structures.NewStore()
			tt.setup(store)
			router := NewRouter(store)

			result := router.get(tt.params)
			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("get() = %s, want %s", string(result), string(tt.expected))
			}
		})
	}
}

func TestSet(t *testing.T) {
	tests := []struct {
		name     string
		params   []resp.RESP
		expected []byte
		check    func(s *structures.Store) bool
	}{
		{
			name: "Set simple key-value",
			params: []resp.RESP{
				{Type: "bulk", Bulk: "key"},
				{Type: "bulk", Bulk: "val"},
			},
			expected: resp.String("OK").Marshal(),
			check: func(s *structures.Store) bool {
				v, ok := s.Get("key")
				return ok && v == "val"
			},
		},
		{
			name: "Set with PX expiry",
			params: []resp.RESP{
				{Type: "bulk", Bulk: "expiryKey"},
				{Type: "bulk", Bulk: "expiryValue"},
				{Type: "bulk", Bulk: "PX"},
				{Type: "bulk", Bulk: "5000"},
			},
			expected: resp.String("OK").Marshal(),
			check: func(s *structures.Store) bool {
				v, ok := s.Get("expiryKey")
				return ok && v == "expiryValue"
			},
		},
		{
			name: "Set with too few arguments",
			params: []resp.RESP{
				{Type: "bulk", Bulk: "key"},
			},
			expected: resp.Error("ERR wrong number of arguments for 'set' command").Marshal(),
			check:    func(s *structures.Store) bool { return true },
		},
		{
			name: "Set with invalid PX duration",
			params: []resp.RESP{
				{Type: "bulk", Bulk: "k"},
				{Type: "bulk", Bulk: "v"},
				{Type: "bulk", Bulk: "PX"},
				{Type: "bulk", Bulk: "notanumber"},
			},
			expected: resp.Error("ERR invalid expire time in set command").Marshal(),
			check:    func(s *structures.Store) bool { return true },
		},
		{
			name: "Set with lowercase px",
			params: []resp.RESP{
				{Type: "bulk", Bulk: "k"},
				{Type: "bulk", Bulk: "v"},
				{Type: "bulk", Bulk: "px"},
				{Type: "bulk", Bulk: "5000"},
			},
			expected: resp.String("OK").Marshal(),
			check: func(s *structures.Store) bool {
				v, ok := s.Get("k")
				return ok && v == "v"
			},
		},
		{
			name: "Set with non-PX flag is ignored",
			params: []resp.RESP{
				{Type: "bulk", Bulk: "k"},
				{Type: "bulk", Bulk: "v"},
				{Type: "bulk", Bulk: "EX"},
				{Type: "bulk", Bulk: "100"},
			},
			expected: resp.String("OK").Marshal(),
			check: func(s *structures.Store) bool {
				_, ok := s.Get("k")
				return ok
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := structures.NewStore()
			router := NewRouter(store)

			result := router.set(tt.params)
			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("set() = %s, want %s", string(result), string(tt.expected))
			}
			if !tt.check(store) {
				t.Error("set() did not properly update the store")
			}
		})
	}
}

func TestKeys(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(s *structures.Store)
		params  []resp.RESP
		checkFn func(result []byte) bool
	}{
		{
			name:   "Empty store",
			setup:  func(s *structures.Store) {},
			params: []resp.RESP{{Type: "bulk", Bulk: "*"}},
			checkFn: func(result []byte) bool {
				return string(result) == "*0\r\n"
			},
		},
		{
			name:   "Wrong number of arguments",
			setup:  func(s *structures.Store) {},
			params: []resp.RESP{},
			checkFn: func(result []byte) bool {
				return strings.Contains(string(result), "ERR")
			},
		},
		{
			name: "Two keys",
			setup: func(s *structures.Store) {
				s.Set("k1", "v1", time.Time{})
				s.Set("k2", "v2", time.Time{})
			},
			params: []resp.RESP{{Type: "bulk", Bulk: "*"}},
			checkFn: func(result []byte) bool {
				r := string(result)
				return strings.HasPrefix(r, "*2") &&
					strings.Contains(r, "k1") &&
					strings.Contains(r, "k2")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := structures.NewStore()
			tt.setup(store)
			router := NewRouter(store)

			result := router.keys(tt.params)
			if !tt.checkFn(result) {
				t.Errorf("keys() = %s, unexpected result", string(result))
			}
		})
	}
}

func TestTyp(t *testing.T) {
	tests := []struct {
		name     string
		setup    func(s *structures.Store)
		params   []resp.RESP
		expected []byte
	}{
		{
			name: "String type",
			setup: func(s *structures.Store) {
				s.Set("str", "val", time.Time{})
			},
			params:   []resp.RESP{{Type: "bulk", Bulk: "str"}},
			expected: resp.Bulk("string").Marshal(),
		},
		{
			name: "Stream type",
			setup: func(s *structures.Store) {
				s.XAdd("mystream", "1-1", map[string]string{"a": "b"})
			},
			params:   []resp.RESP{{Type: "bulk", Bulk: "mystream"}},
			expected: resp.Bulk("stream").Marshal(),
		},
		{
			name:     "Non-existent key",
			setup:    func(s *structures.Store) {},
			params:   []resp.RESP{{Type: "bulk", Bulk: "nokey"}},
			expected: resp.String("none").Marshal(),
		},
		{
			name:     "Wrong number of arguments - zero",
			setup:    func(s *structures.Store) {},
			params:   []resp.RESP{},
			expected: resp.Error("ERR wrong number of arguments for 'type' command").Marshal(),
		},
		{
			name:  "Wrong number of arguments - two",
			setup: func(s *structures.Store) {},
			params: []resp.RESP{
				{Type: "bulk", Bulk: "k1"},
				{Type: "bulk", Bulk: "k2"},
			},
			expected: resp.Error("ERR wrong number of arguments for 'type' command").Marshal(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := structures.NewStore()
			tt.setup(store)
			router := NewRouter(store)

			result := router.typ(tt.params)
			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("typ() = %s, want %s", string(result), string(tt.expected))
			}
		})
	}
}

func TestIncr(t *testing.T) {
	tests := []struct {
		name     string
		setup    func(s *structures.Store)
		params   []resp.RESP
		expected []byte
	}{
		{
			name:     "Wrong number of arguments - zero",
			setup:    func(s *structures.Store) {},
			params:   []resp.RESP{},
			expected: resp.Error("ERR wrong number of arguments for 'incr' command").Marshal(),
		},
		{
			name:  "Wrong number of arguments - two",
			setup: func(s *structures.Store) {},
			params: []resp.RESP{
				{Type: "bulk", Bulk: "k1"},
				{Type: "bulk", Bulk: "k2"},
			},
			expected: resp.Error("ERR wrong number of arguments for 'incr' command").Marshal(),
		},
		{
			name:     "Incr new key returns 1",
			setup:    func(s *structures.Store) {},
			params:   []resp.RESP{{Type: "bulk", Bulk: "newkey"}},
			expected: resp.Integer(1).Marshal(),
		},
		{
			name: "Incr existing numeric",
			setup: func(s *structures.Store) {
				s.Set("counter", "10", time.Time{})
			},
			params:   []resp.RESP{{Type: "bulk", Bulk: "counter"}},
			expected: resp.Integer(11).Marshal(),
		},
		{
			name: "Incr non-numeric returns error",
			setup: func(s *structures.Store) {
				s.Set("str", "hello", time.Time{})
			},
			params:   []resp.RESP{{Type: "bulk", Bulk: "str"}},
			expected: resp.Error("ERR value is not an integer or out of range").Marshal(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := structures.NewStore()
			tt.setup(store)
			router := NewRouter(store)

			result := router.incr(tt.params)
			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("incr() = %s, want %s", string(result), string(tt.expected))
			}
		})
	}
}

func TestXadd(t *testing.T) {
	tests := []struct {
		name    string
		params  []resp.RESP
		checkFn func(result []byte) bool
	}{
		{
			name:   "Too few arguments",
			params: []resp.RESP{{Type: "bulk", Bulk: "stream"}},
			checkFn: func(result []byte) bool {
				return strings.Contains(string(result), "ERR")
			},
		},
		{
			name: "Add to new stream",
			params: []resp.RESP{
				{Type: "bulk", Bulk: "s"},
				{Type: "bulk", Bulk: "1-1"},
				{Type: "bulk", Bulk: "key"},
				{Type: "bulk", Bulk: "val"},
			},
			checkFn: func(result []byte) bool {
				return reflect.DeepEqual(result, resp.Bulk("1-1").Marshal())
			},
		},
		{
			name: "Invalid ID 0-0",
			params: []resp.RESP{
				{Type: "bulk", Bulk: "s"},
				{Type: "bulk", Bulk: "0-0"},
				{Type: "bulk", Bulk: "k"},
				{Type: "bulk", Bulk: "v"},
			},
			checkFn: func(result []byte) bool {
				return strings.Contains(string(result), "greater than 0-0")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			router := newTestRouter()
			result := router.xadd(tt.params)
			if !tt.checkFn(result) {
				t.Errorf("xadd() = %s, unexpected", string(result))
			}
		})
	}
}

func TestXRange(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(r *CommandRouter)
		params  []resp.RESP
		checkFn func(result []byte) bool
	}{
		{
			name:  "Too few arguments",
			setup: func(r *CommandRouter) {},
			params: []resp.RESP{
				{Type: "bulk", Bulk: "s"},
				{Type: "bulk", Bulk: "-"},
			},
			checkFn: func(result []byte) bool {
				return strings.Contains(string(result), "ERR")
			},
		},
		{
			name:  "Non-existent stream",
			setup: func(r *CommandRouter) {},
			params: []resp.RESP{
				{Type: "bulk", Bulk: "missing"},
				{Type: "bulk", Bulk: "-"},
				{Type: "bulk", Bulk: "+"},
			},
			checkFn: func(result []byte) bool {
				return reflect.DeepEqual(result, resp.Nil().Marshal())
			},
		},
		{
			name: "Non-stream type",
			setup: func(r *CommandRouter) {
				r.Store.Set("str", "val", time.Time{})
			},
			params: []resp.RESP{
				{Type: "bulk", Bulk: "str"},
				{Type: "bulk", Bulk: "-"},
				{Type: "bulk", Bulk: "+"},
			},
			checkFn: func(result []byte) bool {
				return reflect.DeepEqual(result, resp.Nil().Marshal())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			router := newTestRouter()
			tt.setup(router)
			result := router.xrange(tt.params)
			if !tt.checkFn(result) {
				t.Errorf("xrange() = %s, unexpected", string(result))
			}
		})
	}
}

func TestXRead(t *testing.T) {
	tests := []struct {
		name    string
		params  []resp.RESP
		checkFn func(result []byte) bool
	}{
		{
			name:   "No arguments",
			params: []resp.RESP{},
			checkFn: func(result []byte) bool {
				return strings.Contains(string(result), "ERR")
			},
		},
		{
			name:   "Unknown subcommand returns nil",
			params: []resp.RESP{{Type: "bulk", Bulk: "UNKNOWN"}},
			checkFn: func(result []byte) bool {
				return reflect.DeepEqual(result, resp.Nil().Marshal())
			},
		},
		{
			name: "STREAMS with odd params returns error",
			params: []resp.RESP{
				{Type: "bulk", Bulk: "STREAMS"},
				{Type: "bulk", Bulk: "mystream"},
			},
			checkFn: func(result []byte) bool {
				return strings.Contains(string(result), "ERR")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			router := newTestRouter()
			result := router.xread(tt.params)
			if !tt.checkFn(result) {
				t.Errorf("xread() = %s, unexpected", string(result))
			}
		})
	}
}

func TestFormatStreamKeys(t *testing.T) {
	tests := []struct {
		name    string
		params  []resp.RESP
		wantErr bool
	}{
		{
			name:    "Odd number of params",
			params:  []resp.RESP{{Type: "bulk", Bulk: "s1"}},
			wantErr: true,
		},
		{
			name: "Even params work",
			params: []resp.RESP{
				{Type: "bulk", Bulk: "s1"},
				{Type: "bulk", Bulk: "0-0"},
			},
			wantErr: false,
		},
		{
			name:    "Empty params",
			params:  []resp.RESP{},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			router := newTestRouter()
			_, _, err := router.formatStreamKeys(tt.params)
			if (err != nil) != tt.wantErr {
				t.Errorf("formatStreamKeys() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
