#!/bin/sh
set -eu

mode=${1:-status}
commit_message='feat: add asynchronous journal command submission'

case "$mode" in
status)
	git status --short
	;;
apply)
	staged_files=$(git diff --cached --name-only)
	if [ -n "$staged_files" ]; then
		while IFS= read -r staged_file; do
			case "$staged_file" in
			README.md|ADOPTED_QUERY_ENGINE_IDEAS.md|ASYNC_COMMAND_SUBMISSION.md|BENCHMARK.md|hat/hatCache/async_command.go|hat/hatCache/async_command_test.go|hat/hatCache/journal.go|scripts/benchmark-async-command.sh|scripts/deliver-async-command.sh|scripts/format-async-command.sh|scripts/test-async-command.sh)
				;;
			*)
				printf 'refusing to stage async command feature with unrelated staged path: %s\n' "$staged_file" >&2
				exit 1
				;;
			esac
		done <<EOF
$staged_files
EOF
	fi

	git add -- \
		README.md \
		ADOPTED_QUERY_ENGINE_IDEAS.md \
		ASYNC_COMMAND_SUBMISSION.md \
		BENCHMARK.md \
		hat/hatCache/async_command.go \
		hat/hatCache/async_command_test.go \
		hat/hatCache/journal.go \
		scripts/benchmark-async-command.sh \
		scripts/deliver-async-command.sh \
		scripts/format-async-command.sh \
		scripts/test-async-command.sh

	head_makefile=$(mktemp)
	wanted_makefile=$(mktemp)
	trap 'rm -f "$head_makefile" "$wanted_makefile"' EXIT INT TERM
	git show HEAD:Makefile > "$head_makefile"
	cp "$head_makefile" "$wanted_makefile"
	printf '%s\n' \
		'' \
		'.PHONY: test-async-command' \
		'test-async-command:' \
		'\tsh ./scripts/test-async-command.sh' \
		'' \
		'.PHONY: format-async-command' \
		'format-async-command:' \
		'\tsh ./scripts/format-async-command.sh' \
		'' \
		'.PHONY: benchmark-async-command' \
		'benchmark-async-command:' \
		'\tsh ./scripts/benchmark-async-command.sh' \
		'' \
		'.PHONY: deliver-async-command' \
		'deliver-async-command:' \
		'\tsh ./scripts/deliver-async-command.sh apply' \
		'' \
		'.PHONY: commit-async-command' \
		'commit-async-command:' \
		'\tsh ./scripts/deliver-async-command.sh commit' \
		'' \
		'.PHONY: push-async-command' \
		'push-async-command:' \
		'\tsh ./scripts/deliver-async-command.sh push' \
		>> "$wanted_makefile"
	wanted_blob=$(git hash-object -w "$wanted_makefile")
	git update-index --add --cacheinfo "100644,$wanted_blob,Makefile"
	git diff --cached --check
	git diff --cached --stat
	;;
commit)
	git diff --cached --check
	git commit -m "$commit_message"
	;;
push)
	git push origin master
	;;
*)
	printf 'usage: %s {status|apply|commit|push}\n' "$0" >&2
	exit 2
	;;
esac
