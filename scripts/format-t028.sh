#!/bin/sh
set -eu

gofmt -w hat/hatCommand/command.go hat/hatCache/command.go hat/hatCache/main.go hat/hatCache/monitoring.go hat/hatCache/command_test.go hat/hatCache/monitoring_test.go
