#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

mode="${1:-status}"

feature_paths=(
	ADOPTED_QUERY_ENGINE_IDEAS.md
	BENCHMARK.md
	Makefile
	README.md
	api.go
	cmd/hatrie-cache/main.go
	cmd/hatrie-cache/main_test.go
	hat/hatCache/command_idempotency.go
	hat/hatCache/command_idempotency_benchmark_test.go
	hat/hatCache/command_idempotency_test.go
	hat/hatCache/journal.go
	hat/hatCache/journal_format.go
	hat/hatCache/journal_test.go
	hat/hatCache/journal_wire.go
	hat/hatCache/journal_wire_test.go
	hat/hatCache/monitoring.go
	hat/hatCommand/command.go
	hat/hatCommand/idempotency_wire_test.go
	hat/hatCommand/wire.go
	hat/hatJournal/journal.go
	internal/gen/hatriecache/v1/cache.pb.go
	proto/hatriecache/v1/cache.proto
	scripts/benchmark-command-idempotency.sh
	scripts/deliver-command-idempotency.sh
	scripts/format-command-idempotency.sh
	scripts/test-command-idempotency.sh
	scripts/test-command-journal-wire.sh
	scripts/test-idempotency-wire.sh
	scripts/test-journal-idempotency-config.sh
)

print_status() {
	git status --short
	git diff --cached --name-status
	git diff --cached --stat
	git diff --cached -- Makefile
	git diff --stat
	git diff --check
	git diff -- Makefile
}

makefile_with_feature_targets() {
	local head_makefile="$1"
	local feature_makefile="$2"
	awk '
	{
		print
		if ($0 == "\tsh ./scripts/push-command-protocol.sh") {
			print ""
			print ".PHONY: format-command-idempotency"
			print "format-command-idempotency:"
			print "\tsh ./scripts/format-command-idempotency.sh"
			print ""
			print ".PHONY: test-command-journal-wire"
			print "test-command-journal-wire:"
			print "\tsh ./scripts/test-command-journal-wire.sh"
			print ""
			print ".PHONY: test-idempotency-wire"
			print "test-idempotency-wire:"
			print "\tsh ./scripts/test-idempotency-wire.sh"
			print ""
			print ".PHONY: test-journal-idempotency-config"
			print "test-journal-idempotency-config:"
			print "\tsh ./scripts/test-journal-idempotency-config.sh"
			anchor_found = 1
		}
	}
	END {
		if (!anchor_found) {
			exit 2
		}
		print ""
		print "test-command-idempotency:"
		print "\t\tbash scripts/test-command-idempotency.sh"
		print ""
		print "benchmark-command-idempotency:"
		print "\t\tbash scripts/benchmark-command-idempotency.sh"
		print ""
		print "inspect-command-idempotency:"
		print "\t\tbash scripts/deliver-command-idempotency.sh status"
		print ""
		print "deliver-command-idempotency:"
		print "\t\tbash scripts/deliver-command-idempotency.sh apply"
		print ""
		print "commit-command-idempotency:"
		print "\t\tbash scripts/deliver-command-idempotency.sh commit"
		print ""
		print "push-command-idempotency:"
		print "\t\tbash scripts/deliver-command-idempotency.sh push"
	}
	' "$head_makefile" > "$feature_makefile"
}

stage_makefile_targets() {
	local head_makefile
	local feature_makefile
	local patch_file
	local normalized_patch
	local diff_status
	head_makefile="$(mktemp)"
	feature_makefile="$(mktemp)"
	patch_file="$(mktemp)"
	normalized_patch="$(mktemp)"
	trap 'rm -f "$head_makefile" "$feature_makefile" "$patch_file" "$normalized_patch"' RETURN
	git show HEAD:Makefile > "$head_makefile"
	makefile_with_feature_targets "$head_makefile" "$feature_makefile"
	set +e
	git diff --no-index --binary "$head_makefile" "$feature_makefile" > "$patch_file"
	diff_status=$?
	set -e
	if [[ "$diff_status" -ne 1 ]]; then
		echo "unable to build the isolated Makefile patch (status $diff_status)" >&2
		return 1
	fi
	sed -e "s|${head_makefile#/}|Makefile|g" -e "s|${feature_makefile#/}|Makefile|g" "$patch_file" > "$normalized_patch"
	git apply --cached "$normalized_patch"
	trap - RETURN
	rm -f "$head_makefile" "$feature_makefile" "$patch_file" "$normalized_patch"
}

verify_staged_paths() {
	local path
	local staged
	staged="$(git diff --cached --name-only)"
	while IFS= read -r path; do
		[[ -z "$path" ]] && continue
		case "$path" in
		ADOPTED_QUERY_ENGINE_IDEAS.md|BENCHMARK.md|Makefile|README.md|api.go|cmd/hatrie-cache/main.go|cmd/hatrie-cache/main_test.go|hat/hatCache/command_idempotency.go|hat/hatCache/command_idempotency_benchmark_test.go|hat/hatCache/command_idempotency_test.go|hat/hatCache/journal.go|hat/hatCache/journal_format.go|hat/hatCache/journal_test.go|hat/hatCache/journal_wire.go|hat/hatCache/journal_wire_test.go|hat/hatCache/monitoring.go|hat/hatCommand/command.go|hat/hatCommand/idempotency_wire_test.go|hat/hatCommand/wire.go|hat/hatJournal/journal.go|internal/gen/hatriecache/v1/cache.pb.go|proto/hatriecache/v1/cache.proto|scripts/benchmark-command-idempotency.sh|scripts/deliver-command-idempotency.sh|scripts/format-command-idempotency.sh|scripts/test-command-idempotency.sh|scripts/test-command-journal-wire.sh|scripts/test-idempotency-wire.sh|scripts/test-journal-idempotency-config.sh)
			;;
		*)
			echo "unexpected staged path: $path" >&2
			return 1
			;;
		esac
	done <<< "$staged"
	for path in "${feature_paths[@]}"; do
		if git diff --cached --quiet -- "$path"; then
			echo "feature path is not staged: $path" >&2
			return 1
		fi
	done
	git diff --cached --check
}

stage_feature() {
	local path
	for path in "${feature_paths[@]}"; do
		if [[ "$path" != "Makefile" ]]; then
			git add -- "$path"
		fi
	done
	git restore --staged -- Makefile
	stage_makefile_targets
	verify_staged_paths
}

case "$mode" in
status)
	print_status
	;;
apply)
	stage_feature
	;;
commit)
	verify_staged_paths
	git commit -m "feat: add retry-safe command idempotency"
	;;
push)
	git push origin master
	;;
*)
	echo "usage: $0 {status|apply|commit|push}" >&2
	exit 2
	;;
esac
