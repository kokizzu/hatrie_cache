#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatCommand/command.go \
  hat/hatCommand/error_code_wire_test.go \
  hat/hatCommand/wire.go \
  hat/hatCache/command.go \
  hat/hatCache/structured_error_code_test.go
