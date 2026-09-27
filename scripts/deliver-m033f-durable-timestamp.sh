#!/usr/bin/env bash
set -euo pipefail

files=(
  M033F_DURABLE_TIMESTAMP_COORDINATOR.md
  hat/hatReplication/m033f_durable_timestamp_coordinator.go
  hat/hatReplication/m033f_durable_timestamp_coordinator_test.go
  scripts/benchmark-m033-global-timestamps.sh
  scripts/deliver-m033f-durable-timestamp.sh
)

mode="${1:-}"
case "$mode" in
plan)
	git status --short -- "${files[@]}"
	git diff --stat -- "${files[@]}"
	;;
stage)
	git add -- "${files[@]}"
	git diff --cached --check -- "${files[@]}"
	git diff --cached --stat -- "${files[@]}"
	;;
commit)
	git commit --only -m "feat(replication): add durable timestamp coordinator [skip ci]" -- "${files[@]}"
	;;
push)
	git push origin HEAD
	;;
deliver)
	bash "$0" stage
	bash "$0" commit
	bash "$0" push
	;;
*)
	printf 'usage: %s {plan|stage|commit|push|deliver}\n' "$0" >&2
	exit 2
	;;
esac
