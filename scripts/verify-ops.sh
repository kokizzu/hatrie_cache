#!/usr/bin/env sh
set -eu

tmp_dir=
created_tmp=0
pids=
started_pid=

fail() {
	echo "verify-ops: $*" >&2
	exit 1
}

cleanup() {
	for pid in $pids; do
		if kill -0 "$pid" 2>/dev/null; then
			kill "$pid" 2>/dev/null || true
			wait "$pid" 2>/dev/null || true
		fi
	done
	if [ "$created_tmp" -eq 1 ] && [ -n "$tmp_dir" ]; then
		rm -rf "$tmp_dir"
	fi
}

trap cleanup EXIT HUP INT TERM

if [ -n "${OPS_SMOKE_DIR:-}" ]; then
	tmp_dir=$OPS_SMOKE_DIR
	mkdir -p "$tmp_dir"
else
	tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie_cache_ops_verify.XXXXXX")
	created_tmp=1
fi

bin_dir="$tmp_dir/bin"
mkdir -p "$bin_dir"
server_bin="$bin_dir/hatrie-cache"
cli_bin="$bin_dir/hatrie-cli"

go build -o "$server_bin" ./cmd/hatrie-cache
go build -o "$cli_bin" ./cmd/hatrie-cli

base_port=${OPS_SMOKE_BASE_PORT:-$((18080 + ($$ % 10000)))}
restore_addr="127.0.0.1:$base_port"
leader_addr="127.0.0.1:$((base_port + 1))"
follower_addr="127.0.0.1:$((base_port + 2))"

dump_log() {
	name=$1
	log_file="$tmp_dir/$name.log"
	if [ -f "$log_file" ]; then
		echo "--- $name log ---" >&2
		sed -n '1,160p' "$log_file" >&2
	fi
}

wait_health() {
	name=$1
	addr=$2
	pid=$3
	i=0
	while [ "$i" -lt 30 ]; do
		if ! kill -0 "$pid" 2>/dev/null; then
			dump_log "$name"
			fail "$name exited before health check succeeded"
		fi
		if "$cli_bin" -addr "http://$addr" -timeout 2s health >/dev/null 2>&1; then
			return 0
		fi
		i=$((i + 1))
		sleep 1
	done
	dump_log "$name"
	fail "$name did not become healthy at $addr"
}

start_server() {
	name=$1
	addr=$2
	shift 2
	log_file="$tmp_dir/$name.log"
	"$server_bin" \
		-monitoring-server \
		-monitoring-addr "$addr" \
		-monitoring-web-dir "" \
		"$@" >"$log_file" 2>&1 &
	pid=$!
	pids="$pids $pid"
	wait_health "$name" "$addr" "$pid"
	started_pid=$pid
}

stop_server() {
	pid=$1
	if kill -0 "$pid" 2>/dev/null; then
		kill "$pid"
		wait "$pid" || true
	fi
}

cli() {
	addr=$1
	shift
	"$cli_bin" -addr "http://$addr" -timeout 5s "$@"
}

command() {
	addr=$1
	cmd=$2
	key=$3
	value=$4
	cli "$addr" command -cmd "$cmd" -key "$key" -value "$value" >/dev/null
}

expect_value() {
	addr=$1
	key=$2
	want=$3
	output=$(cli "$addr" command -cmd GET -key "$key")
	printf '%s' "$output" | grep -F '"ok":true' >/dev/null || fail "GET $key was not ok: $output"
	printf '%s' "$output" | grep -F '"value":"'"$want"'"' >/dev/null || fail "GET $key = $output, want value $want"
}

run_snapshot_journal_restore_smoke() {
	data_dir="$tmp_dir/restore"
	mkdir -p "$data_dir"
	snapshot_path="$data_dir/snapshot.hc"
	journal_path="$data_dir/commands.journal"

	start_server restore-source "$restore_addr" \
		-snapshot-path "$snapshot_path" \
		-journal-path "$journal_path"
	pid=$started_pid
	command "$restore_addr" SET ops:restore:before before
	cli "$restore_addr" snapshot >/dev/null
	command "$restore_addr" SET ops:restore:after after
	stop_server "$pid"

	start_server restore-replay "$restore_addr" \
		-snapshot-path "$snapshot_path" \
		-journal-path "$journal_path"
	pid=$started_pid
	expect_value "$restore_addr" ops:restore:before before
	expect_value "$restore_addr" ops:restore:after after
	stop_server "$pid"
}

run_journal_pull_smoke() {
	leader_dir="$tmp_dir/leader"
	follower_dir="$tmp_dir/follower"
	mkdir -p "$leader_dir" "$follower_dir"

	start_server journal-leader "$leader_addr" \
		-journal-path "$leader_dir/commands.journal"
	leader_pid=$started_pid
	command "$leader_addr" SET ops:journal:first first
	command "$leader_addr" SET ops:journal:second second

	start_server journal-follower "$follower_addr" \
		-journal-path "$follower_dir/commands.journal" \
		-journal-pull-source "http://$leader_addr" \
		-journal-pull-state-path "$follower_dir/pull_state.json" \
		-journal-pull-timeout 5s
	follower_pid=$started_pid

	expect_value "$follower_addr" ops:journal:first first
	expect_value "$follower_addr" ops:journal:second second

	command "$leader_addr" SET ops:journal:third third
	cli "$follower_addr" journal -pull-from "http://$leader_addr" -after-sequence 2 -until-current >/dev/null
	expect_value "$follower_addr" ops:journal:third third

	stop_server "$follower_pid"
	stop_server "$leader_pid"
}

run_snapshot_journal_restore_smoke
run_journal_pull_smoke

echo "verify-ops: ok"
