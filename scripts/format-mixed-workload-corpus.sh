#!/bin/sh
set -eu

gofmt -w ./cmd/hatrie-sqlbench/main.go ./cmd/hatrie-sqlbench/main_test.go
