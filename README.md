[![progress-banner](https://backend.codecrafters.io/progress/redis/9e8e4753-aeaa-4619-9386-df276b5a61f1)](https://app.codecrafters.io/users/codecrafters-bot?r=2qF)
# REDIS Clone -- Build your own Redis (CodeCrafters)

A Redis-compatible in-memory data store built from scratch in Go. Implements core Redis functionality including the RESP wire protocol, key-value storage with expiry, streams, transactions, replication, and RDB persistence.

Built as part of the [Build Your Own Redis](https://codecrafters.io/challenges/redis) challenge from [CodeCrafters](https://codecrafters.io).

## Supported Commands

| Category | Commands |
|---|---|
| **General** | `PING`, `ECHO`, `KEYS`, `TYPE`, `CONFIG GET` |
| **Strings** | `GET`, `SET` (with `PX` expiry), `INCR` |
| **Streams** | `XADD`, `XRANGE`, `XREAD` (with blocking) |
| **Transactions** | `MULTI`, `EXEC`, `DISCARD` |
| **Replication** | `INFO`, `REPLCONF`, `PSYNC` |

## Architecture

- **RESP Protocol** -- Full implementation of the Redis Serialization Protocol with binary-safe bulk strings, arrays, integers, simple strings, and error responses.
- **Command Router** -- Extensible handler-based design. Adding a new command requires registering a single handler function.
- **Store Abstraction** -- Thread-safe key-value store with internal locking, lazy expiry on access, and support for multiple data types (strings, streams).
- **RDB Persistence** -- Read and load Redis RDB files to restore state on startup.
- **Replication** -- Master-replica replication with replica handshake and command propagation.

## Getting Started

### Prerequisites

- Go 1.24+

### Run the Server

```sh
./run.sh
```

Or directly:

```sh
go run app/server.go
```

### Connect with redis-cli

```sh
redis-cli -p 6379
```

## Testing

The project includes three layers of tests:

- **Unit tests** -- RESP parsing, data structures, and individual command handlers.
- **Integration tests** -- Connection handling and command routing.
- **End-to-end tests** -- In-process tests that start a real TCP server and exercise full request/response cycles, including concurrency, pipelining, and edge cases.

Run all tests:

```sh
go test ./...
```

Run a specific test suite:

```sh
go test ./e2e/ -v          # end-to-end tests
go test ./app/resp/ -v     # RESP protocol tests
go test ./app/handlers/ -v # command handler tests
```

## Project Structure

```
app/
  server.go              # Entry point, server startup
  config/                # Configuration and CONFIG command
  handlers/              # Command handlers (CommandRouter)
  rdb/                   # RDB file parsing
  resp/                  # RESP protocol reader/writer
  resp-connection/       # TCP connection handling, transactions, replication
  structures/            # Store, data types (streams, maps)
e2e/                     # End-to-end tests
```

**Note**: If you're viewing this repo on GitHub, head over to
[codecrafters.io](https://codecrafters.io) to try the challenge.
