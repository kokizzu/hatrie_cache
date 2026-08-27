#!/bin/sh
set -eu

go run golang.org/x/vuln/cmd/govulncheck@v1.1.4 ./...
