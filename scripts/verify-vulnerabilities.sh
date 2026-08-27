#!/bin/sh
set -eu

GOTOOLCHAIN=go1.26.6 go run golang.org/x/vuln/cmd/govulncheck@v1.1.4 ./...
