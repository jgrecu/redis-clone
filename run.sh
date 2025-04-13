#!/bin/sh
#
# Use this script to run your program LOCALLY.
#
set -e # Exit early if any commands fail

(
  cd "$(dirname "$0")" # Ensure compile steps are run within the repository directory
  go build -o /tmp/build-redis-go app/*.go
)

exec /tmp/build-redis-go "$@"