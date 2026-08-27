#!/bin/sh
set -eu

cd "$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
gofmt -w ./hat/hatCommand/protocol.go ./hat/hatCommand/protocol_test.go ./hat/hatCache/command.go ./hat/hatCache/command_protocol_test.go ./hat/hatCache/monitoring.go
