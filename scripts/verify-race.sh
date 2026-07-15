#!/usr/bin/env sh
set -eu

go test -race -count=1 ./...
