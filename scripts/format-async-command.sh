#!/bin/sh
set -eu

gofmt -w hat/hatCache/async_command.go hat/hatCache/async_command_test.go hat/hatCache/journal.go
