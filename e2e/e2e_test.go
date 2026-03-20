package e2e

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jgrecu/redis-clone/app/handlers"
	"github.com/jgrecu/redis-clone/app/resp"
	respConnection "github.com/jgrecu/redis-clone/app/resp-connection"
	"github.com/jgrecu/redis-clone/app/structures"
)

// ---------------------------------------------------------------------------
// Test infrastructure
// ---------------------------------------------------------------------------

// startServer creates an in-process Redis server on a random port.
// Returns the address and a cleanup function.
func startServer(t *testing.T) (string, func()) {
	t.Helper()

	store := structures.NewStore()
	router := handlers.NewRouter(store)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to start listener: %v", err)
	}

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return // listener closed
			}
			client := respConnection.NewRespConn(conn, router)
			go client.Listen()
		}
	}()

	return listener.Addr().String(), func() { listener.Close() }
}

// testClient wraps a TCP connection with RESP protocol helpers.
type testClient struct {
	conn   net.Conn
	reader *resp.RespReader
}

func dial(t *testing.T, addr string) *testClient {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("Failed to connect to %s: %v", addr, err)
	}
	return &testClient{
		conn:   conn,
		reader: resp.NewRespReader(bufio.NewReader(conn)),
	}
}

func (c *testClient) Close() { c.conn.Close() }

// Do sends a command and reads one response.
func (c *testClient) Do(t *testing.T, cmd string, args ...string) resp.RESP {
	t.Helper()
	c.conn.SetDeadline(time.Now().Add(2 * time.Second))
	defer c.conn.SetDeadline(time.Time{})

	_, err := c.conn.Write(resp.Command(cmd, args...).Marshal())
	if err != nil {
		t.Fatalf("Write %s: %v", cmd, err)
	}

	result, err := c.reader.Read()
	if err != nil {
		t.Fatalf("Read response for %s: %v", cmd, err)
	}
	return result
}

// ---------------------------------------------------------------------------
// Assertion helpers
// ---------------------------------------------------------------------------

func assertString(t *testing.T, r resp.RESP, want string) {
	t.Helper()
	if r.Type != "string" || r.Bulk != want {
		t.Errorf("got (%s %q), want string %q", r.Type, r.Bulk, want)
	}
}

func assertBulk(t *testing.T, r resp.RESP, want string) {
	t.Helper()
	if r.Type != "bulk" || r.Bulk != want {
		t.Errorf("got (%s %q), want bulk %q", r.Type, r.Bulk, want)
	}
}

func assertNil(t *testing.T, r resp.RESP) {
	t.Helper()
	if r.Type != "nil" {
		t.Errorf("got type %s (%q), want nil", r.Type, r.Bulk)
	}
}

func assertError(t *testing.T, r resp.RESP) {
	t.Helper()
	if r.Type != "error" {
		t.Errorf("got type %s (%q), want error", r.Type, r.Bulk)
	}
}

func assertErrorContains(t *testing.T, r resp.RESP, substr string) {
	t.Helper()
	if r.Type != "error" {
		t.Errorf("got type %s, want error", r.Type)
		return
	}
	if !strings.Contains(r.Bulk, substr) {
		t.Errorf("error %q does not contain %q", r.Bulk, substr)
	}
}

func assertInteger(t *testing.T, r resp.RESP, want int) {
	t.Helper()
	if r.Type != "integer" || r.Integer != want {
		t.Errorf("got (%s %d), want integer %d", r.Type, r.Integer, want)
	}
}

func assertArray(t *testing.T, r resp.RESP, wantLen int) {
	t.Helper()
	if r.Type != "array" {
		t.Errorf("got type %s, want array", r.Type)
		return
	}
	if len(r.Array) != wantLen {
		t.Errorf("array len = %d, want %d", len(r.Array), wantLen)
	}
}

// ---------------------------------------------------------------------------
// PING / ECHO
// ---------------------------------------------------------------------------

func TestE2E_Ping(t *testing.T) {
	addr, cleanup := startServer(t)
	defer cleanup()
	c := dial(t, addr)
	defer c.Close()

	assertString(t, c.Do(t, "PING"), "PONG")
}

func TestE2E_Echo(t *testing.T) {
	addr, cleanup := startServer(t)
	defer cleanup()
	c := dial(t, addr)
	defer c.Close()

	t.Run("simple", func(t *testing.T) {
		assertString(t, c.Do(t, "ECHO", "hello"), "hello")
	})
	t.Run("empty string", func(t *testing.T) {
		assertString(t, c.Do(t, "ECHO", ""), "")
	})
	t.Run("spaces", func(t *testing.T) {
		assertString(t, c.Do(t, "ECHO", "hello world"), "hello world")
	})
	t.Run("special characters", func(t *testing.T) {
		assertString(t, c.Do(t, "ECHO", "!@#$%^&*()"), "!@#$%^&*()")
	})
	t.Run("unicode", func(t *testing.T) {
		assertString(t, c.Do(t, "ECHO", "héllo wörld"), "héllo wörld")
	})
}

// ---------------------------------------------------------------------------
// SET / GET
// ---------------------------------------------------------------------------

func TestE2E_SetGet(t *testing.T) {
	addr, cleanup := startServer(t)
	defer cleanup()
	c := dial(t, addr)
	defer c.Close()

	t.Run("basic set and get", func(t *testing.T) {
		assertString(t, c.Do(t, "SET", "key", "value"), "OK")
		assertBulk(t, c.Do(t, "GET", "key"), "value")
	})

	t.Run("get non-existent key", func(t *testing.T) {
		assertNil(t, c.Do(t, "GET", "no_such_key"))
	})

	t.Run("overwrite existing key", func(t *testing.T) {
		c.Do(t, "SET", "ow", "first")
		c.Do(t, "SET", "ow", "second")
		assertBulk(t, c.Do(t, "GET", "ow"), "second")
	})

	t.Run("empty value", func(t *testing.T) {
		assertString(t, c.Do(t, "SET", "empty_val", ""), "OK")
		assertBulk(t, c.Do(t, "GET", "empty_val"), "")
	})

	t.Run("empty key name", func(t *testing.T) {
		assertString(t, c.Do(t, "SET", "", "val"), "OK")
		assertBulk(t, c.Do(t, "GET", ""), "val")
	})

	t.Run("large value", func(t *testing.T) {
		big := strings.Repeat("x", 100_000)
		assertString(t, c.Do(t, "SET", "big", big), "OK")
		assertBulk(t, c.Do(t, "GET", "big"), big)
	})

	t.Run("binary-safe value", func(t *testing.T) {
		val := "line1\r\nline2\r\n"
		assertString(t, c.Do(t, "SET", "bin", val), "OK")
		assertBulk(t, c.Do(t, "GET", "bin"), val)
	})

	t.Run("value with newlines", func(t *testing.T) {
		val := "a\nb\nc"
		assertString(t, c.Do(t, "SET", "nl", val), "OK")
		assertBulk(t, c.Do(t, "GET", "nl"), val)
	})
}

func TestE2E_SetGet_Expiry(t *testing.T) {
	addr, cleanup := startServer(t)
	defer cleanup()
	c := dial(t, addr)
	defer c.Close()

	t.Run("PX expiry", func(t *testing.T) {
		assertString(t, c.Do(t, "SET", "exp", "val", "PX", "100"), "OK")
		assertBulk(t, c.Do(t, "GET", "exp"), "val")

		time.Sleep(150 * time.Millisecond)
		assertNil(t, c.Do(t, "GET", "exp"))
	})

	t.Run("lowercase px", func(t *testing.T) {
		assertString(t, c.Do(t, "SET", "lc", "val", "px", "5000"), "OK")
		assertBulk(t, c.Do(t, "GET", "lc"), "val")
	})

	t.Run("invalid PX value", func(t *testing.T) {
		assertErrorContains(t, c.Do(t, "SET", "k", "v", "PX", "notanumber"), "invalid expire")
	})

	t.Run("no expiry persists", func(t *testing.T) {
		c.Do(t, "SET", "perm", "val")
		time.Sleep(50 * time.Millisecond)
		assertBulk(t, c.Do(t, "GET", "perm"), "val")
	})
}

func TestE2E_SetGet_WrongArgs(t *testing.T) {
	addr, cleanup := startServer(t)
	defer cleanup()
	c := dial(t, addr)
	defer c.Close()

	t.Run("GET with no args", func(t *testing.T) {
		assertErrorContains(t, c.Do(t, "GET"), "wrong number of arguments")
	})
	t.Run("SET with one arg", func(t *testing.T) {
		assertErrorContains(t, c.Do(t, "SET", "key"), "wrong number of arguments")
	})
}

// ---------------------------------------------------------------------------
// KEYS
// ---------------------------------------------------------------------------

func TestE2E_Keys(t *testing.T) {
	addr, cleanup := startServer(t)
	defer cleanup()
	c := dial(t, addr)
	defer c.Close()

	t.Run("empty store", func(t *testing.T) {
		r := c.Do(t, "KEYS", "*")
		assertArray(t, r, 0)
	})

	t.Run("multiple keys", func(t *testing.T) {
		c.Do(t, "SET", "k1", "v1")
		c.Do(t, "SET", "k2", "v2")
		c.Do(t, "SET", "k3", "v3")

		r := c.Do(t, "KEYS", "*")
		assertArray(t, r, 3)

		got := map[string]bool{}
		for _, elem := range r.Array {
			got[elem.Bulk] = true
		}
		for _, k := range []string{"k1", "k2", "k3"} {
			if !got[k] {
				t.Errorf("KEYS missing %s", k)
			}
		}
	})

	t.Run("wrong args", func(t *testing.T) {
		assertErrorContains(t, c.Do(t, "KEYS"), "wrong number of arguments")
	})
}

// ---------------------------------------------------------------------------
// TYPE
// ---------------------------------------------------------------------------

func TestE2E_Type(t *testing.T) {
	addr, cleanup := startServer(t)
	defer cleanup()
	c := dial(t, addr)
	defer c.Close()

	t.Run("non-existent", func(t *testing.T) {
		assertString(t, c.Do(t, "TYPE", "nokey"), "none")
	})

	t.Run("string type", func(t *testing.T) {
		c.Do(t, "SET", "str", "val")
		assertBulk(t, c.Do(t, "TYPE", "str"), "string")
	})

	t.Run("stream type", func(t *testing.T) {
		c.Do(t, "XADD", "mystream", "1-1", "f", "v")
		assertBulk(t, c.Do(t, "TYPE", "mystream"), "stream")
	})

	t.Run("wrong args", func(t *testing.T) {
		assertErrorContains(t, c.Do(t, "TYPE"), "wrong number of arguments")
	})
}

// ---------------------------------------------------------------------------
// INCR
// ---------------------------------------------------------------------------

func TestE2E_Incr(t *testing.T) {
	addr, cleanup := startServer(t)
	defer cleanup()
	c := dial(t, addr)
	defer c.Close()

	t.Run("new key", func(t *testing.T) {
		assertInteger(t, c.Do(t, "INCR", "counter"), 1)
	})

	t.Run("increment existing", func(t *testing.T) {
		c.Do(t, "SET", "num", "10")
		assertInteger(t, c.Do(t, "INCR", "num"), 11)
	})

	t.Run("multiple increments", func(t *testing.T) {
		c.Do(t, "SET", "seq", "0")
		assertInteger(t, c.Do(t, "INCR", "seq"), 1)
		assertInteger(t, c.Do(t, "INCR", "seq"), 2)
		assertInteger(t, c.Do(t, "INCR", "seq"), 3)
	})

	t.Run("negative value", func(t *testing.T) {
		c.Do(t, "SET", "neg", "-5")
		assertInteger(t, c.Do(t, "INCR", "neg"), -4)
	})

	t.Run("non-numeric value", func(t *testing.T) {
		c.Do(t, "SET", "str", "hello")
		assertErrorContains(t, c.Do(t, "INCR", "str"), "not an integer")
	})

	t.Run("float value", func(t *testing.T) {
		c.Do(t, "SET", "f", "3.14")
		assertErrorContains(t, c.Do(t, "INCR", "f"), "not an integer")
	})

	t.Run("empty string value", func(t *testing.T) {
		c.Do(t, "SET", "e", "")
		assertErrorContains(t, c.Do(t, "INCR", "e"), "not an integer")
	})

	t.Run("wrong args", func(t *testing.T) {
		assertErrorContains(t, c.Do(t, "INCR"), "wrong number of arguments")
	})
}

// ---------------------------------------------------------------------------
// Streams: XADD / XRANGE / XREAD
// ---------------------------------------------------------------------------

func TestE2E_Xadd(t *testing.T) {
	addr, cleanup := startServer(t)
	defer cleanup()
	c := dial(t, addr)
	defer c.Close()

	t.Run("explicit ID", func(t *testing.T) {
		assertBulk(t, c.Do(t, "XADD", "s", "1-1", "key", "val"), "1-1")
	})

	t.Run("auto-generated ID", func(t *testing.T) {
		r := c.Do(t, "XADD", "s2", "*", "key", "val")
		if r.Type != "bulk" || !strings.Contains(r.Bulk, "-") {
			t.Errorf("XADD * = (%s %q), want bulk with timestamp-seq", r.Type, r.Bulk)
		}
	})

	t.Run("auto sequence", func(t *testing.T) {
		c.Do(t, "XADD", "s3", "1-1", "a", "1")
		assertBulk(t, c.Do(t, "XADD", "s3", "1-*", "b", "2"), "1-2")
	})

	t.Run("reject 0-0", func(t *testing.T) {
		assertErrorContains(t, c.Do(t, "XADD", "s4", "0-0", "k", "v"), "greater than 0-0")
	})

	t.Run("reject smaller ID", func(t *testing.T) {
		c.Do(t, "XADD", "s5", "5-1", "a", "1")
		assertErrorContains(t, c.Do(t, "XADD", "s5", "3-1", "b", "2"), "equal or smaller")
	})

	t.Run("reject duplicate ID", func(t *testing.T) {
		c.Do(t, "XADD", "s6", "1-1", "a", "1")
		assertErrorContains(t, c.Do(t, "XADD", "s6", "1-1", "b", "2"), "equal or smaller")
	})

	t.Run("no field-value pairs", func(t *testing.T) {
		assertBulk(t, c.Do(t, "XADD", "s7", "1-1"), "1-1")
	})

	t.Run("too few args", func(t *testing.T) {
		assertErrorContains(t, c.Do(t, "XADD", "s8"), "wrong number of arguments")
	})
}

func TestE2E_Xrange(t *testing.T) {
	addr, cleanup := startServer(t)
	defer cleanup()
	c := dial(t, addr)
	defer c.Close()

	// Populate a stream
	c.Do(t, "XADD", "stream", "1-1", "a", "1")
	c.Do(t, "XADD", "stream", "2-1", "b", "2")
	c.Do(t, "XADD", "stream", "3-1", "c", "3")

	t.Run("full range with - and +", func(t *testing.T) {
		r := c.Do(t, "XRANGE", "stream", "-", "+")
		assertArray(t, r, 3)
	})

	t.Run("partial range", func(t *testing.T) {
		r := c.Do(t, "XRANGE", "stream", "2-0", "3-2")
		assertArray(t, r, 2)
	})

	t.Run("single entry range", func(t *testing.T) {
		r := c.Do(t, "XRANGE", "stream", "2-1", "2-1")
		assertArray(t, r, 1)
	})

	t.Run("non-existent stream", func(t *testing.T) {
		assertNil(t, c.Do(t, "XRANGE", "nosuch", "-", "+"))
	})

	t.Run("non-stream type", func(t *testing.T) {
		c.Do(t, "SET", "str", "val")
		assertNil(t, c.Do(t, "XRANGE", "str", "-", "+"))
	})

	t.Run("empty range", func(t *testing.T) {
		r := c.Do(t, "XRANGE", "stream", "99-0", "100-0")
		assertArray(t, r, 0)
	})

	t.Run("no duplicate entries", func(t *testing.T) {
		r := c.Do(t, "XRANGE", "stream", "-", "+")
		if r.Type != "array" {
			t.Fatal("expected array")
		}
		seen := map[string]int{}
		for _, entry := range r.Array {
			if entry.Type == "array" && len(entry.Array) >= 1 {
				seen[entry.Array[0].Bulk]++
			}
		}
		for id, count := range seen {
			if count != 1 {
				t.Errorf("entry %s appeared %d times, want 1", id, count)
			}
		}
	})

	t.Run("wrong args", func(t *testing.T) {
		assertErrorContains(t, c.Do(t, "XRANGE", "stream", "-"), "wrong number of arguments")
	})
}

func TestE2E_Xread(t *testing.T) {
	addr, cleanup := startServer(t)
	defer cleanup()
	c := dial(t, addr)
	defer c.Close()

	c.Do(t, "XADD", "s", "1-1", "a", "1")
	c.Do(t, "XADD", "s", "1-2", "b", "2")

	t.Run("STREAMS reads entries after ID", func(t *testing.T) {
		r := c.Do(t, "XREAD", "STREAMS", "s", "1-1")
		if r.Type != "array" {
			t.Fatalf("got type %s, want array", r.Type)
		}
		// Should have at least one result (entry 1-2)
		if len(r.Array) < 1 {
			t.Errorf("expected at least 1 stream result, got %d", len(r.Array))
		}
	})

	t.Run("STREAMS no new entries", func(t *testing.T) {
		r := c.Do(t, "XREAD", "STREAMS", "s", "1-2")
		// No entries after 1-2 → empty array
		assertArray(t, r, 0)
	})

	t.Run("STREAMS non-existent stream", func(t *testing.T) {
		r := c.Do(t, "XREAD", "STREAMS", "nostream", "0-0")
		assertArray(t, r, 0)
	})

	t.Run("unknown subcommand", func(t *testing.T) {
		assertNil(t, c.Do(t, "XREAD", "INVALID"))
	})

	t.Run("STREAMS odd params", func(t *testing.T) {
		assertErrorContains(t, c.Do(t, "XREAD", "STREAMS", "s"), "wrong number of arguments")
	})
}

// ---------------------------------------------------------------------------
// Transactions: MULTI / EXEC / DISCARD
// ---------------------------------------------------------------------------

func TestE2E_MultiExec(t *testing.T) {
	addr, cleanup := startServer(t)
	defer cleanup()
	c := dial(t, addr)
	defer c.Close()

	t.Run("basic transaction", func(t *testing.T) {
		assertString(t, c.Do(t, "MULTI"), "OK")
		assertString(t, c.Do(t, "SET", "tk", "tv"), "QUEUED")
		assertString(t, c.Do(t, "GET", "tk"), "QUEUED")

		r := c.Do(t, "EXEC")
		assertArray(t, r, 2)

		// SET returns +OK
		assertString(t, r.Array[0], "OK")
		// GET returns the value SET just stored
		assertBulk(t, r.Array[1], "tv")
	})

	t.Run("empty transaction", func(t *testing.T) {
		assertString(t, c.Do(t, "MULTI"), "OK")
		r := c.Do(t, "EXEC")
		assertArray(t, r, 0)
	})

	t.Run("EXEC without MULTI", func(t *testing.T) {
		assertErrorContains(t, c.Do(t, "EXEC"), "EXEC without MULTI")
	})

	t.Run("transaction with INCR", func(t *testing.T) {
		c.Do(t, "SET", "txc", "5")
		assertString(t, c.Do(t, "MULTI"), "OK")
		assertString(t, c.Do(t, "INCR", "txc"), "QUEUED")
		assertString(t, c.Do(t, "INCR", "txc"), "QUEUED")
		assertString(t, c.Do(t, "GET", "txc"), "QUEUED")

		r := c.Do(t, "EXEC")
		assertArray(t, r, 3)
		assertInteger(t, r.Array[0], 6)
		assertInteger(t, r.Array[1], 7)
		assertBulk(t, r.Array[2], "7")
	})

	t.Run("transaction with nil result", func(t *testing.T) {
		assertString(t, c.Do(t, "MULTI"), "OK")
		assertString(t, c.Do(t, "GET", "nonexistent_in_tx"), "QUEUED")

		r := c.Do(t, "EXEC")
		assertArray(t, r, 1)
		assertNil(t, r.Array[0])
	})

	t.Run("transaction with error result", func(t *testing.T) {
		c.Do(t, "SET", "txstr", "notanumber")
		assertString(t, c.Do(t, "MULTI"), "OK")
		assertString(t, c.Do(t, "INCR", "txstr"), "QUEUED")

		r := c.Do(t, "EXEC")
		assertArray(t, r, 1)
		assertError(t, r.Array[0])
	})
}

func TestE2E_Discard(t *testing.T) {
	addr, cleanup := startServer(t)
	defer cleanup()
	c := dial(t, addr)
	defer c.Close()

	t.Run("discard transaction", func(t *testing.T) {
		c.Do(t, "SET", "dk", "original")
		assertString(t, c.Do(t, "MULTI"), "OK")
		assertString(t, c.Do(t, "SET", "dk", "changed"), "QUEUED")
		assertString(t, c.Do(t, "DISCARD"), "OK")

		// Value should be unchanged
		assertBulk(t, c.Do(t, "GET", "dk"), "original")
	})

	t.Run("DISCARD without MULTI", func(t *testing.T) {
		assertErrorContains(t, c.Do(t, "DISCARD"), "Discard without MULTI")
	})

	t.Run("new transaction after discard", func(t *testing.T) {
		assertString(t, c.Do(t, "MULTI"), "OK")
		c.Do(t, "SET", "dk2", "queued")
		assertString(t, c.Do(t, "DISCARD"), "OK")

		// Start fresh transaction
		assertString(t, c.Do(t, "MULTI"), "OK")
		assertString(t, c.Do(t, "SET", "dk2", "real"), "QUEUED")
		r := c.Do(t, "EXEC")
		assertArray(t, r, 1)
		assertBulk(t, c.Do(t, "GET", "dk2"), "real")
	})
}

// ---------------------------------------------------------------------------
// Error handling & edge cases
// ---------------------------------------------------------------------------

func TestE2E_UnknownCommand(t *testing.T) {
	addr, cleanup := startServer(t)
	defer cleanup()
	c := dial(t, addr)
	defer c.Close()

	assertErrorContains(t, c.Do(t, "FOOBAR"), "Command not found")
}

func TestE2E_CaseInsensitive(t *testing.T) {
	addr, cleanup := startServer(t)
	defer cleanup()
	c := dial(t, addr)
	defer c.Close()

	t.Run("lowercase ping", func(t *testing.T) {
		assertString(t, c.Do(t, "ping"), "PONG")
	})

	t.Run("mixed case set/get", func(t *testing.T) {
		assertString(t, c.Do(t, "Set", "ci_key", "val"), "OK")
		assertBulk(t, c.Do(t, "gEt", "ci_key"), "val")
	})

	t.Run("lowercase incr", func(t *testing.T) {
		assertInteger(t, c.Do(t, "incr", "ci_counter"), 1)
	})
}

func TestE2E_PipelinedCommands(t *testing.T) {
	addr, cleanup := startServer(t)
	defer cleanup()
	c := dial(t, addr)
	defer c.Close()

	// Send multiple commands without reading responses in between
	c.conn.SetDeadline(time.Now().Add(2 * time.Second))
	c.conn.Write(resp.Command("SET", "p1", "v1").Marshal())
	c.conn.Write(resp.Command("SET", "p2", "v2").Marshal())
	c.conn.Write(resp.Command("GET", "p1").Marshal())
	c.conn.Write(resp.Command("GET", "p2").Marshal())

	// Now read all responses
	r1, _ := c.reader.Read()
	r2, _ := c.reader.Read()
	r3, _ := c.reader.Read()
	r4, _ := c.reader.Read()
	c.conn.SetDeadline(time.Time{})

	assertString(t, r1, "OK")
	assertString(t, r2, "OK")
	assertBulk(t, r3, "v1")
	assertBulk(t, r4, "v2")
}

// ---------------------------------------------------------------------------
// Multiple clients / concurrency
// ---------------------------------------------------------------------------

func TestE2E_MultipleClients(t *testing.T) {
	addr, cleanup := startServer(t)
	defer cleanup()

	c1 := dial(t, addr)
	defer c1.Close()
	c2 := dial(t, addr)
	defer c2.Close()

	// Client 1 sets a key
	c1.Do(t, "SET", "shared", "from_c1")

	// Client 2 reads it
	assertBulk(t, c2.Do(t, "GET", "shared"), "from_c1")

	// Client 2 overwrites
	c2.Do(t, "SET", "shared", "from_c2")

	// Client 1 sees the new value
	assertBulk(t, c1.Do(t, "GET", "shared"), "from_c2")
}

func TestE2E_ConcurrentSetGet(t *testing.T) {
	addr, cleanup := startServer(t)
	defer cleanup()

	const numClients = 20
	var wg sync.WaitGroup

	for i := range numClients {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			c := dial(t, addr)
			defer c.Close()

			key := fmt.Sprintf("cc_%d", id)
			val := fmt.Sprintf("val_%d", id)

			c.Do(t, "SET", key, val)
			r := c.Do(t, "GET", key)
			assertBulk(t, r, val)
		}(i)
	}

	wg.Wait()
}

func TestE2E_ConcurrentIncr(t *testing.T) {
	addr, cleanup := startServer(t)
	defer cleanup()

	// Set initial value
	c := dial(t, addr)
	c.Do(t, "SET", "shared_counter", "0")
	c.Close()

	const numClients = 50
	var wg sync.WaitGroup

	for range numClients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client := dial(t, addr)
			defer client.Close()
			client.Do(t, "INCR", "shared_counter")
		}()
	}
	wg.Wait()

	// Verify final value
	c = dial(t, addr)
	defer c.Close()
	assertBulk(t, c.Do(t, "GET", "shared_counter"), fmt.Sprintf("%d", numClients))
}

// ---------------------------------------------------------------------------
// Full integration scenarios
// ---------------------------------------------------------------------------

func TestE2E_SetGetDeleteCycle(t *testing.T) {
	addr, cleanup := startServer(t)
	defer cleanup()
	c := dial(t, addr)
	defer c.Close()

	// SET → GET → SET (overwrite) → GET → verify
	c.Do(t, "SET", "cycle", "one")
	assertBulk(t, c.Do(t, "GET", "cycle"), "one")
	c.Do(t, "SET", "cycle", "two")
	assertBulk(t, c.Do(t, "GET", "cycle"), "two")
}

func TestE2E_IncrThenGet(t *testing.T) {
	addr, cleanup := startServer(t)
	defer cleanup()
	c := dial(t, addr)
	defer c.Close()

	c.Do(t, "INCR", "ig")
	c.Do(t, "INCR", "ig")
	c.Do(t, "INCR", "ig")
	assertBulk(t, c.Do(t, "GET", "ig"), "3")
}

func TestE2E_StreamFullWorkflow(t *testing.T) {
	addr, cleanup := startServer(t)
	defer cleanup()
	c := dial(t, addr)
	defer c.Close()

	// Add entries
	c.Do(t, "XADD", "events", "1-1", "action", "click")
	c.Do(t, "XADD", "events", "2-1", "action", "scroll")
	c.Do(t, "XADD", "events", "3-1", "action", "submit")

	// Verify type
	assertBulk(t, c.Do(t, "TYPE", "events"), "stream")

	// Range query
	r := c.Do(t, "XRANGE", "events", "-", "+")
	assertArray(t, r, 3)

	// Read after first entry
	r = c.Do(t, "XREAD", "STREAMS", "events", "1-1")
	if r.Type != "array" || len(r.Array) < 1 {
		t.Errorf("XREAD should return entries after 1-1, got %v", r)
	}
}

func TestE2E_MixedTypes(t *testing.T) {
	addr, cleanup := startServer(t)
	defer cleanup()
	c := dial(t, addr)
	defer c.Close()

	// Create a string key
	c.Do(t, "SET", "mixed", "string_val")
	assertBulk(t, c.Do(t, "TYPE", "mixed"), "string")

	// Overwrite with SET (becomes string again even if we tried stream)
	c.Do(t, "SET", "mixed", "new_val")
	assertBulk(t, c.Do(t, "TYPE", "mixed"), "string")
	assertBulk(t, c.Do(t, "GET", "mixed"), "new_val")

	// Create a stream separately
	c.Do(t, "XADD", "mixed_stream", "1-1", "f", "v")
	assertBulk(t, c.Do(t, "TYPE", "mixed_stream"), "stream")

	// GET on stream returns nil (it's not a string)
	// The GET handler fetches .String which is empty for stream types
	r := c.Do(t, "GET", "mixed_stream")
	// Stream MapValue has empty String field, so GET returns bulk ""
	if r.Type == "nil" || (r.Type == "bulk" && r.Bulk == "") {
		// Either behavior is acceptable
	} else {
		t.Errorf("GET on stream type = (%s %q), want nil or empty bulk", r.Type, r.Bulk)
	}
}

func TestE2E_KeysAfterExpiry(t *testing.T) {
	addr, cleanup := startServer(t)
	defer cleanup()
	c := dial(t, addr)
	defer c.Close()

	c.Do(t, "SET", "persist", "yes")
	c.Do(t, "SET", "expire_soon", "bye", "PX", "50")

	// Both keys visible
	r := c.Do(t, "KEYS", "*")
	assertArray(t, r, 2)

	time.Sleep(100 * time.Millisecond)

	// Trigger lazy expiry via GET
	c.Do(t, "GET", "expire_soon")

	// Now only persistent key remains
	r = c.Do(t, "KEYS", "*")
	assertArray(t, r, 1)
	assertBulk(t, r.Array[0], "persist")
}
