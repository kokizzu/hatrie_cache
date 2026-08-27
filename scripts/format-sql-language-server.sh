#!/bin/sh
set -eu

gofmt -w ./cmd/hatrie-sql-lsp/main.go ./cmd/hatrie-sql-lsp/main_test.go
