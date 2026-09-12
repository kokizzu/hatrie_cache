#!/usr/bin/env sh
set -eu

git add -- \
	BENCHMARK.md \
	CLIENT_SDK.md \
	DATA_STRUCTURE.md \
	INSPIRATION_BACKLOG.md \
	Makefile \
	README.md \
	hat/hatCache/command.go \
	hat/hatCache/conditional.go \
	hat/hatCache/conditional_command_benchmark_test.go \
	hat/hatCache/conditional_command_test.go \
	hat/hatCommand/command.go \
	hat/hatCommand/conditional_wire_test.go \
	hat/hatCommand/wire.go \
	internal/gen/hatriecache/v1/cache.pb.go \
	proto/hatriecache/v1/cache.proto \
	scripts/benchmark-conditional-command.sh \
	scripts/commit-conditional-command.sh \
	scripts/push-conditional-command.sh \
	scripts/format-conditional-command.sh \
	scripts/race-conditional-command.sh \
	scripts/review-conditional-command.sh \
	scripts/test-conditional-command.sh
git commit -m "feat: add atomic compare-and-swap command"
