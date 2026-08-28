#!/usr/bin/env sh
set -eu

gofmt -w ./hat/hatStorage/namespace_lifecycle.go ./hat/hatStorage/namespace_lifecycle_test.go
