# Testing Guidelines for Redis Clone

This document provides guidelines for testing the Redis Clone application.

## Test Structure

The tests are organized by package, with each package having its own test file(s). The tests follow the standard Go testing conventions:

- Test files are named `*_test.go`
- Test functions are named `Test*`
- Table-driven tests are used where appropriate to test multiple cases

## Running Tests

To run all tests:

```bash
go test ./...
```

To run tests for a specific package:

```bash
go test github.com/jgrecu/redis-clone/app/structures
go test github.com/jgrecu/redis-clone/app/resp
go test github.com/jgrecu/redis-clone/app/handlers
go test github.com/jgrecu/redis-clone/app/resp-connection
```

To run a specific test:

```bash
go test github.com/jgrecu/redis-clone/app/structures -run TestGet
```

## Test Coverage

To check test coverage:

```bash
go test ./... -cover
```

For a detailed coverage report:

```bash
go test ./... -coverprofile=coverage.out
go tool cover -html=coverage.out
```

## Adding New Tests

When adding new features or modifying existing ones, follow these guidelines:

1. **Write tests first**: Consider writing tests before implementing the feature (Test-Driven Development)
2. **Test edge cases**: Make sure to test edge cases, not just the happy path
3. **Use mocks**: Use mocks for external dependencies (like network connections)
4. **Keep tests independent**: Each test should be independent of others
5. **Test public API**: Focus on testing the public API of each package

## Types of Tests

The project includes several types of tests:

1. **Unit tests**: Test individual functions and methods in isolation
2. **Integration tests**: Test interactions between components
3. **Functional tests**: Test end-to-end functionality

## Mocking

For components that have external dependencies (like network connections), use mocks to isolate the component being tested. See `app/resp-connection/resp-conn_test.go` for an example of mocking the `net.Conn` interface.

## Continuous Integration

Consider setting up a CI/CD pipeline to run tests automatically on each commit. This could be done using GitHub Actions, GitLab CI, or another CI/CD service.

## Future Improvements

Some areas for future improvement in the testing strategy:

1. **Benchmarks**: Add benchmarks for performance-critical code
2. **Property-based testing**: Consider using property-based testing for complex logic
3. **Fuzz testing**: Add fuzz testing for parsing and protocol handling
4. **End-to-end tests**: Add end-to-end tests that start a server and connect to it with a client