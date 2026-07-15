#!/usr/bin/env sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
SANITIZE_C=${SANITIZE_C:-auto}

. "$ROOT/scripts/c-sanitizer-policy.sh"

tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie_cache_c_verify.XXXXXX")
trap 'rm -rf "$tmp_dir"' EXIT HUP INT TERM

case "$SANITIZE_C" in
	auto|1|true|yes|0|false|no)
		;;
	*)
		echo "SANITIZE_C must be auto, 1, or 0" >&2
		exit 2
		;;
esac

case "$SANITIZE_C" in
	1|true|yes)
		if strict_overcommit_blocks_sanitizers; then
			echo "SANITIZE_C=1 requested but vm.overcommit_memory=2 uses strict overcommit and can reject AddressSanitizer shadow memory reservations around $(asan_min_commit_headroom_label); set SANITIZE_C=0 or SANITIZE_C_ALLOW_STRICT_OVERCOMMIT=1 to override" >&2
			exit 2
		fi
		if low_commit_headroom_blocks_sanitizers; then
			echo "SANITIZE_C=1 requested but available commit headroom $(commit_headroom_kb) KiB is below AddressSanitizer's expected shadow-memory reservation $(asan_min_commit_headroom_label); set SANITIZE_C=0 or SANITIZE_C_ALLOW_LOW_COMMIT_HEADROOM=1 to override" >&2
			exit 2
		fi
		;;
esac

build_c_check() {
	output=$1
	test_file=$2
	shift 2
	gcc -std=c99 -Wall -Wextra "$@" \
		-I"$ROOT/luikore__hat-trie/src" \
		-o "$output" \
		"$test_file" \
		"$ROOT/luikore__hat-trie/test/str_map.c" \
		"$ROOT/luikore__hat-trie/src/hat-trie.c" \
		"$ROOT/luikore__hat-trie/src/ahtable.c" \
		"$ROOT/luikore__hat-trie/src/misc.c" \
		"$ROOT/luikore__hat-trie/src/murmurhash3.c"
}

build_c_ahtable_check() {
	output=$1
	shift
	gcc -std=c99 -Wall -Wextra "$@" \
		-I"$ROOT/luikore__hat-trie/src" \
		-o "$output" \
		"$ROOT/luikore__hat-trie/test/check_ahtable.c" \
		"$ROOT/luikore__hat-trie/test/str_map.c" \
		"$ROOT/luikore__hat-trie/src/ahtable.c" \
		"$ROOT/luikore__hat-trie/src/misc.c" \
		"$ROOT/luikore__hat-trie/src/murmurhash3.c"
}

build_c_check \
	"$tmp_dir/check_hattrie" \
	"$ROOT/luikore__hat-trie/test/check_hattrie.c"

build_c_ahtable_check "$tmp_dir/check_ahtable"

"$tmp_dir/check_hattrie"
"$tmp_dir/check_ahtable"

compiler_supports_sanitizers() {
	tmp_bin=$tmp_dir/c_asan_probe
	tmp_c=$tmp_bin.c
	printf 'int main(void) { return 0; }\n' > "$tmp_c"
	if gcc -std=c99 -fsanitize=address,undefined -fno-omit-frame-pointer "$tmp_c" -o "$tmp_bin" >/dev/null 2>&1 &&
		ASAN_OPTIONS=${ASAN_OPTIONS:-detect_leaks=1:halt_on_error=1:abort_on_error=1} \
			UBSAN_OPTIONS=${UBSAN_OPTIONS:-halt_on_error=1:abort_on_error=1} \
			"$tmp_bin" >/dev/null 2>&1; then
		rm -f "$tmp_c" "$tmp_bin"
		return 0
	fi
	rm -f "$tmp_c" "$tmp_bin"
	return 1
}

compiler_supports_leak_sanitizer() {
	tmp_bin=$tmp_dir/c_lsan_probe
	tmp_c=$tmp_bin.c
	printf 'int main(void) { return 0; }\n' > "$tmp_c"
	if gcc -std=c99 -fsanitize=leak -fno-omit-frame-pointer "$tmp_c" -o "$tmp_bin" >/dev/null 2>&1 &&
		LSAN_OPTIONS=${LSAN_OPTIONS:-detect_leaks=1:halt_on_error=1:exitcode=23} \
			"$tmp_bin" >/dev/null 2>&1; then
		rm -f "$tmp_c" "$tmp_bin"
		return 0
	fi
	rm -f "$tmp_c" "$tmp_bin"
	return 1
}

run_leak_sanitizer=0
if [ "$SANITIZE_C" = "auto" ]; then
	if strict_overcommit_enabled; then
		echo "skipping C AddressSanitizer/UBSan pass: vm.overcommit_memory=2 uses strict overcommit and can reject AddressSanitizer shadow memory reservations around $(asan_min_commit_headroom_label)" >&2
		if compiler_supports_leak_sanitizer; then
			echo "running C leak sanitizer fallback" >&2
			run_leak_sanitizer=1
		fi
		SANITIZE_C=0
	elif low_commit_headroom_blocks_sanitizers; then
		echo "skipping C AddressSanitizer/UBSan pass: available commit headroom $(commit_headroom_kb) KiB is below AddressSanitizer's expected shadow-memory reservation $(asan_min_commit_headroom_label)" >&2
		if compiler_supports_leak_sanitizer; then
			echo "running C leak sanitizer fallback" >&2
			run_leak_sanitizer=1
		fi
		SANITIZE_C=0
	elif compiler_supports_sanitizers; then
		SANITIZE_C=1
	elif compiler_supports_leak_sanitizer; then
		echo "skipping C AddressSanitizer/UBSan pass: compiler/runtime does not support AddressSanitizer and UBSan" >&2
		echo "running C leak sanitizer fallback" >&2
		run_leak_sanitizer=1
		SANITIZE_C=0
	else
		echo "skipping C sanitizer pass: compiler/runtime does not support AddressSanitizer, UBSan, or LeakSanitizer" >&2
		SANITIZE_C=0
	fi
fi

case "$SANITIZE_C" in
	1|true|yes)
		SAN_FLAGS="-fsanitize=address,undefined -fno-omit-frame-pointer"
		build_c_check \
			"$tmp_dir/check_hattrie_sanitize" \
			"$ROOT/luikore__hat-trie/test/check_hattrie.c" \
			$SAN_FLAGS
		build_c_ahtable_check \
			"$tmp_dir/check_ahtable_sanitize" \
			$SAN_FLAGS
		ASAN_OPTIONS=${ASAN_OPTIONS:-detect_leaks=1:halt_on_error=1:abort_on_error=1} \
			UBSAN_OPTIONS=${UBSAN_OPTIONS:-halt_on_error=1:abort_on_error=1} \
			"$tmp_dir/check_hattrie_sanitize"
		ASAN_OPTIONS=${ASAN_OPTIONS:-detect_leaks=1:halt_on_error=1:abort_on_error=1} \
			UBSAN_OPTIONS=${UBSAN_OPTIONS:-halt_on_error=1:abort_on_error=1} \
			"$tmp_dir/check_ahtable_sanitize"
		;;
	0|false|no)
		;;
	*)
		echo "SANITIZE_C must be auto, 1, or 0" >&2
		exit 2
		;;
esac

if [ "$run_leak_sanitizer" = "1" ]; then
	LEAK_FLAGS="-fsanitize=leak -fno-omit-frame-pointer"
	build_c_check \
		"$tmp_dir/check_hattrie_leak" \
		"$ROOT/luikore__hat-trie/test/check_hattrie.c" \
		$LEAK_FLAGS
	build_c_ahtable_check \
		"$tmp_dir/check_ahtable_leak" \
		$LEAK_FLAGS
	LSAN_OPTIONS=${LSAN_OPTIONS:-detect_leaks=1:halt_on_error=1:exitcode=23} \
		"$tmp_dir/check_hattrie_leak"
	LSAN_OPTIONS=${LSAN_OPTIONS:-detect_leaks=1:halt_on_error=1:exitcode=23} \
		"$tmp_dir/check_ahtable_leak"
fi
