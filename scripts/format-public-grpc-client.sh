#!/bin/sh
set -eu

gofmt -w \
	hat/hatGrpc/public_client.go \
	hat/hatGrpc/public_client_test.go
