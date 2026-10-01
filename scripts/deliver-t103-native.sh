#!/usr/bin/env bash
set -euo pipefail

mode="${1:?mode is required}"
branch="codex/t103-native-extension"
files=(
	Makefile
	README.md
	PLUGIN_REGISTRY.md
	CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md
	BENCHMARK.md
	T103_NATIVE_FFI_BOUNDARY.md
	hat/hatExtension/native_extension.go
	hat/hatExtension/native_extension_test.go
	hat/hatExtension/native_extension_baseline_test.go
	hat/hatExtension/native_extension_benchmark_test.go
	hat/hatExtension/native_extension_example_test.go
	scripts/deliver-t103-native.sh
	scripts/format-t103-native.sh
	scripts/test-t103-native.sh
)

case "$mode" in
status)
	git diff --check
	git status --short
	git diff --stat
	;;
review)
	git diff --check
	git diff -- Makefile README.md PLUGIN_REGISTRY.md CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md BENCHMARK.md
	;;
stage)
	git diff --check
	git add -- "${files[@]}"
	git diff --cached --check
	git diff --cached --stat
	git diff --cached --name-only
	;;
commit)
	git diff --cached --check
	git commit -m 'feat(extension): add safe native ABI boundary [skip ci]'
	;;
push)
	git push -u origin "$branch"
	;;
*)
	printf 'unknown T103 delivery mode: %s\n' "$mode" >&2
	exit 2
	;;
esac
