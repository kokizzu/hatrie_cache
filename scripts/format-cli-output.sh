#!/bin/sh
set -eu

gofmt -w cmd/hatrie-cli/main.go cmd/hatrie-cli/output_format_test.go
