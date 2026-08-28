#!/usr/bin/env sh
set -eu

gofmt -w ./hat/hatCache/integrity.go ./hat/hatCache/integrity_test.go
