#!/usr/bin/env sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
. "$ROOT/scripts/c-sanitizer-policy.sh"

tmp_dir=${TMPDIR:-/tmp}/hatrie_cache_c_policy_test.$$
trap 'rm -rf "$tmp_dir"' EXIT HUP INT TERM
mkdir -p "$tmp_dir"

overcommit_file=$tmp_dir/overcommit_memory
meminfo_file=$tmp_dir/meminfo
SANITIZE_C_OVERCOMMIT_MEMORY_PATH=$overcommit_file
SANITIZE_C_MEMINFO_PATH=$meminfo_file
SANITIZE_C_ASAN_MIN_COMMIT_HEADROOM_KB=1024
export SANITIZE_C_OVERCOMMIT_MEMORY_PATH SANITIZE_C_MEMINFO_PATH SANITIZE_C_ASAN_MIN_COMMIT_HEADROOM_KB

fail() {
	echo "$1" >&2
	exit 1
}

expect_strict_enabled() {
	printf '%s\n' "$1" > "$overcommit_file"
	SANITIZE_C_ALLOW_STRICT_OVERCOMMIT=0
	export SANITIZE_C_ALLOW_STRICT_OVERCOMMIT
	if ! strict_overcommit_enabled; then
		fail "expected overcommit mode $1 to enable strict sanitizer guard"
	fi
}

expect_strict_disabled() {
	printf '%s\n' "$1" > "$overcommit_file"
	SANITIZE_C_ALLOW_STRICT_OVERCOMMIT=0
	export SANITIZE_C_ALLOW_STRICT_OVERCOMMIT
	if strict_overcommit_enabled; then
		fail "expected overcommit mode $1 to disable strict sanitizer guard"
	fi
}

expect_blocks() {
	printf '%s\n' "$1" > "$overcommit_file"
	SANITIZE_C_ALLOW_STRICT_OVERCOMMIT=0
	export SANITIZE_C_ALLOW_STRICT_OVERCOMMIT
	if ! strict_overcommit_blocks_sanitizers; then
		fail "expected overcommit mode $1 to block C sanitizers"
	fi
}

expect_allows() {
	printf '%s\n' "$1" > "$overcommit_file"
	SANITIZE_C_ALLOW_STRICT_OVERCOMMIT=0
	export SANITIZE_C_ALLOW_STRICT_OVERCOMMIT
	if strict_overcommit_blocks_sanitizers; then
		fail "expected overcommit mode $1 to allow C sanitizers"
	fi
}

write_meminfo() {
	cat > "$meminfo_file" <<EOF_MEMINFO
CommitLimit:    $1 kB
Committed_AS:   $2 kB
EOF_MEMINFO
}

expect_low_commit_blocks() {
	write_meminfo "$1" "$2"
	SANITIZE_C_ALLOW_LOW_COMMIT_HEADROOM=0
	export SANITIZE_C_ALLOW_LOW_COMMIT_HEADROOM
	if ! low_commit_headroom_blocks_sanitizers; then
		fail "expected low commit headroom to block C sanitizers"
	fi
}

expect_low_commit_allows() {
	write_meminfo "$1" "$2"
	SANITIZE_C_ALLOW_LOW_COMMIT_HEADROOM=0
	export SANITIZE_C_ALLOW_LOW_COMMIT_HEADROOM
	if low_commit_headroom_blocks_sanitizers; then
		fail "expected sufficient commit headroom to allow C sanitizers"
	fi
}

expect_asan_reservation_bytes() {
	SANITIZE_C_ASAN_MIN_COMMIT_HEADROOM_KB=$1
	export SANITIZE_C_ASAN_MIN_COMMIT_HEADROOM_KB
	got=$(asan_min_commit_headroom_bytes)
	if [ "$got" != "$2" ]; then
		fail "expected ASan reservation $2 bytes, got $got"
	fi
}

expect_default_asan_reservation_bytes() {
	unset SANITIZE_C_ASAN_MIN_COMMIT_HEADROOM_KB
	got=$(asan_min_commit_headroom_bytes)
	if [ "$got" != "15392894357504" ]; then
		fail "expected default ASan reservation 15392894357504 bytes, got $got"
	fi
	SANITIZE_C_ASAN_MIN_COMMIT_HEADROOM_KB=1024
	export SANITIZE_C_ASAN_MIN_COMMIT_HEADROOM_KB
}

expect_strict_enabled 2
expect_strict_disabled 0
expect_strict_disabled 1

expect_blocks 2
expect_allows 0
expect_allows 1

expect_asan_reservation_bytes 1024 1048576
expect_default_asan_reservation_bytes

expect_low_commit_blocks 2048 1536
expect_low_commit_allows 4096 1024

printf '%s\n' 2 > "$overcommit_file"
for allow_value in 1 true yes; do
	SANITIZE_C_ALLOW_STRICT_OVERCOMMIT=$allow_value
	export SANITIZE_C_ALLOW_STRICT_OVERCOMMIT
	if ! strict_overcommit_enabled; then
		fail "expected strict-overcommit override $allow_value to keep auto-mode guard enabled"
	fi
	if strict_overcommit_blocks_sanitizers; then
		fail "expected strict-overcommit override $allow_value to allow C sanitizers"
	fi
done

write_meminfo 2048 1536
for allow_value in 1 true yes; do
	SANITIZE_C_ALLOW_LOW_COMMIT_HEADROOM=$allow_value
	export SANITIZE_C_ALLOW_LOW_COMMIT_HEADROOM
	if low_commit_headroom_blocks_sanitizers; then
		fail "expected low-commit-headroom override $allow_value to allow C sanitizers"
	fi
done

SANITIZE_C_ALLOW_STRICT_OVERCOMMIT=0
SANITIZE_C_OVERCOMMIT_MEMORY_PATH=$tmp_dir/missing
SANITIZE_C_ALLOW_LOW_COMMIT_HEADROOM=0
SANITIZE_C_MEMINFO_PATH=$tmp_dir/missing-meminfo
export SANITIZE_C_ALLOW_STRICT_OVERCOMMIT SANITIZE_C_OVERCOMMIT_MEMORY_PATH SANITIZE_C_ALLOW_LOW_COMMIT_HEADROOM SANITIZE_C_MEMINFO_PATH
if strict_overcommit_enabled; then
	fail "expected missing overcommit setting to disable strict sanitizer guard"
fi
if strict_overcommit_blocks_sanitizers; then
	fail "expected missing overcommit setting to allow C sanitizers"
fi
if low_commit_headroom_blocks_sanitizers; then
	fail "expected missing meminfo to allow C sanitizers"
fi
