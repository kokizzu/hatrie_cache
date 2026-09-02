#!/bin/sh
set -eu

mode=${1:-status}
commit_message='feat: add asynchronous HTTP command admission'

feature_files='ADOPTED_QUERY_ENGINE_IDEAS.md
ASYNC_HTTP_COMMANDS.md
BENCHMARK.md
README.md
api.go
cmd/hatrie-cache/async_config_test.go
cmd/hatrie-cache/main.go
hat/hatCache/async_command.go
hat/hatCache/async_command_http.go
hat/hatCache/async_command_http_benchmark_test.go
hat/hatCache/async_command_http_test.go
hat/hatCache/journal.go
hat/hatCache/monitoring.go
scripts/benchmark-async-http.sh
scripts/deliver-async-http.sh
scripts/format-async-command.sh
scripts/monitoring-server.sh
scripts/test-async-http.sh'

check_staged_scope() {
	staged_files=$(git diff --cached --name-only)
	if [ -z "$staged_files" ]; then
		return 0
	fi
	while IFS= read -r staged_file; do
		case "$staged_file" in
		ADOPTED_QUERY_ENGINE_IDEAS.md|ASYNC_HTTP_COMMANDS.md|BENCHMARK.md|Makefile|README.md|api.go|cmd/hatrie-cache/async_config_test.go|cmd/hatrie-cache/main.go|hat/hatCache/async_command.go|hat/hatCache/async_command_http.go|hat/hatCache/async_command_http_benchmark_test.go|hat/hatCache/async_command_http_test.go|hat/hatCache/journal.go|hat/hatCache/monitoring.go|scripts/benchmark-async-http.sh|scripts/deliver-async-http.sh|scripts/format-async-command.sh|scripts/monitoring-server.sh|scripts/test-async-http.sh)
			;;
		*)
			printf 'refusing async HTTP delivery with unrelated staged path: %s\n' "$staged_file" >&2
			exit 1
			;;
		esac
	done <<EOF
$staged_files
EOF
}

stage_feature() {
	check_staged_scope
	git add -- $feature_files

	head_makefile=$(mktemp)
	wanted_makefile=$(mktemp)
	trap 'rm -f "$head_makefile" "$wanted_makefile"' EXIT INT TERM
	git show HEAD:Makefile > "$head_makefile"
	awk '
	FNR == NR {
		if ($0 == "monitoring-server: export MONITORING_ASYNC_COMMANDS := $(MONITORING_ASYNC_COMMANDS)") {
			has_exports = 1
		}
		if ($0 == ".PHONY: test-async-http") {
			has_targets = 1
		}
		next
	}
	{
		print
		if (!has_exports && $0 == "monitoring-server: export DIAGNOSTICS_PROFILING := $(DIAGNOSTICS_PROFILING)") {
			print "monitoring-server: export MONITORING_ASYNC_COMMANDS := $(MONITORING_ASYNC_COMMANDS)"
			print "monitoring-server: export MONITORING_ASYNC_COMMAND_STATUS_CAPACITY := $(MONITORING_ASYNC_COMMAND_STATUS_CAPACITY)"
			print "monitoring-server: export JOURNAL_IDEMPOTENCY_CAPACITY := $(JOURNAL_IDEMPOTENCY_CAPACITY)"
		}
	}
	END {
		if (!has_targets) {
			print ""
			print ".PHONY: test-async-http"
			print "test-async-http:"
			print "\tsh ./scripts/test-async-http.sh"
			print ""
			print ".PHONY: benchmark-async-http"
			print "benchmark-async-http:"
			print "\tsh ./scripts/benchmark-async-http.sh"
			print ""
			print ".PHONY: deliver-async-http"
			print "deliver-async-http:"
			print "\tsh ./scripts/deliver-async-http.sh apply"
			print ""
			print ".PHONY: check-async-http-stage"
			print "check-async-http-stage:"
			print "\tsh ./scripts/deliver-async-http.sh check"
			print ""
			print ".PHONY: commit-async-http"
			print "commit-async-http:"
			print "\tsh ./scripts/deliver-async-http.sh commit"
			print ""
			print ".PHONY: push-async-http"
			print "push-async-http:"
			print "\tsh ./scripts/deliver-async-http.sh push"
		}
	}
' "$head_makefile" "$head_makefile" > "$wanted_makefile"
	wanted_blob=$(git hash-object -w "$wanted_makefile")
	git update-index --add --cacheinfo "100644,$wanted_blob,Makefile"
	git diff --cached --check
	git diff --cached --stat
}

case "$mode" in
status)
	git status --short
	;;
apply)
	stage_feature
	;;
check)
	check_staged_scope
	git diff --cached --check
	git diff --cached --stat
	;;
commit)
	check_staged_scope
	git diff --cached --check
	git commit -m "$commit_message"
	;;
push)
	git push origin master
	;;
*)
	printf 'usage: %s {status|apply|check|commit|push}\n' "$0" >&2
	exit 2
	;;
esac
