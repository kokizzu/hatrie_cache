#!/usr/bin/env bash
set -euo pipefail

mode=${1:?mode is required: check, review, stage, commit, or push}
files=(
	Makefile
	ENGINE_IDEAS.md
	BENCHMARK.md
	CH042_STREAMING_SAMPLE.md
	hat/hatSql/query.go
	hat/hatSql/ch042_sample_stream.go
	hat/hatSql/chg04_sample_stream_test.go
	scripts/format-chg04-sample-stream.sh
	scripts/test-chg04-sample-stream.sh
	scripts/test-chg04-sample-stream-package.sh
	scripts/benchmark-chg04-sample-stream-baseline.sh
	scripts/benchmark-chg04-sample-stream.sh
	scripts/race-chg04-sample-stream.sh
	scripts/vet-chg04-sample-stream.sh
	scripts/deliver-chg04-sample-stream.sh
)

case "$mode" in
	check)
		git diff --check -- "${files[@]}"
		git status --short -- "${files[@]}"
		;;
	review)
		git diff --cached --stat -- "${files[@]}"
		git diff --cached -- "${files[@]}"
		;;
	stage)
		git diff --check -- "${files[@]}"
		git add -- "${files[@]}"
		git diff --cached --check -- "${files[@]}"
		;;
	commit)
		git diff --check -- "${files[@]}"
		git add -- "${files[@]}"
		git diff --cached --check -- "${files[@]}"
		git commit -m "feat(sql): stream Bernoulli table samples [skip ci]"
		;;
	push)
		git push origin HEAD
		;;
	*)
		printf 'unknown delivery mode: %s\n' "$mode" >&2
		exit 2
		;;
esac
