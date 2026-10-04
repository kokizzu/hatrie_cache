#!/bin/sh
set -eu

gofmt -w \
	hat/hatFunction/stored_function_registry.go \
	hat/hatFunction/stored_function_registry_test.go
