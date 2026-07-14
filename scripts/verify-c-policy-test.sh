#!/usr/bin/env sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
. "$ROOT/scripts/c-sanitizer-policy.sh"

tmp_dir=${TMPDIR:-/tmp}/hatrie_cache_c_policy_test.$$
trap 'rm -rf "$tmp_dir"' EXIT HUP INT TERM
mkdir -p "$tmp_dir"

overcommit_file=$tmp_dir/overcommit_memory
SANITIZE_C_OVERCOMMIT_MEMORY_PATH=$overcommit_file
export SANITIZE_C_OVERCOMMIT_MEMORY_PATH

fail() {
	echo "$1" >&2
	exit 1
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

expect_blocks 2
expect_allows 0
expect_allows 1

printf '%s\n' 2 > "$overcommit_file"
for allow_value in 1 true yes; do
	SANITIZE_C_ALLOW_STRICT_OVERCOMMIT=$allow_value
	export SANITIZE_C_ALLOW_STRICT_OVERCOMMIT
	if strict_overcommit_blocks_sanitizers; then
		fail "expected strict-overcommit override $allow_value to allow C sanitizers"
	fi
done

SANITIZE_C_ALLOW_STRICT_OVERCOMMIT=0
SANITIZE_C_OVERCOMMIT_MEMORY_PATH=$tmp_dir/missing
export SANITIZE_C_ALLOW_STRICT_OVERCOMMIT SANITIZE_C_OVERCOMMIT_MEMORY_PATH
if strict_overcommit_blocks_sanitizers; then
	fail "expected missing overcommit setting to allow C sanitizers"
fi
