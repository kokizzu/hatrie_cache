#!/bin/sh
set -eu

cd "$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
gofmt -w ./hat/hatCache/command_concurrency_test.go
