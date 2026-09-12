#!/usr/bin/env sh
set -eu

gofmt -w \
	./hat/hatCommand/command.go \
	./hat/hatCommand/wire.go \
	./hat/hatCommand/conditional_wire_test.go \
	./hat/hatCache/command.go \
	./hat/hatCache/conditional.go \
	./hat/hatCache/conditional_command_test.go \
	./hat/hatCache/conditional_command_benchmark_test.go
