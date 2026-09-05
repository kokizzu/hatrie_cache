#!/bin/sh
set -eu

gofmt -w hat/hatCache/atomic_command.go hat/hatCache/atomic_command_test.go api.go
