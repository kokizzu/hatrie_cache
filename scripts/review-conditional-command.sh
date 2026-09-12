#!/usr/bin/env sh
set -eu

git diff --check -- \
	BENCHMARK.md \
	CLIENT_SDK.md \
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
	scripts/format-conditional-command.sh \
	scripts/race-conditional-command.sh \
	scripts/review-conditional-command.sh \
	scripts/test-conditional-command.sh

printf '%s\n' '== changed files =='
git status --short -- \
	BENCHMARK.md \
	CLIENT_SDK.md \
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
	scripts/format-conditional-command.sh \
	scripts/race-conditional-command.sh \
	scripts/review-conditional-command.sh \
	scripts/test-conditional-command.sh

git diff --stat -- \
	BENCHMARK.md \
	CLIENT_SDK.md \
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
	scripts/format-conditional-command.sh \
	scripts/race-conditional-command.sh \
	scripts/review-conditional-command.sh \
	scripts/test-conditional-command.sh
printf '%s\n' '== documentation command references =='
rg -n -C 3 'canonical command|SETSTR|CAS' DATA_STRUCTURE.md
rg -n -C 2 'canonical command groups' BENCHMARK.md
sed -n '14006,14075p' BENCHMARK.md
sed -n '1,35p' DATA_STRUCTURE.md
sed -n '75,96p' DATA_STRUCTURE.md
sed -n '170,212p' DATA_STRUCTURE.md
sed -n '335,355p' DATA_STRUCTURE.md
sed -n '412,426p' DATA_STRUCTURE.md
