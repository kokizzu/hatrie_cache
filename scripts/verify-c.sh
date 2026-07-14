#!/usr/bin/env sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
SANITIZE_C=${SANITIZE_C:-auto}

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
	/tmp/hatrie_cache_check_hattrie \
	"$ROOT/luikore__hat-trie/test/check_hattrie.c"

build_c_ahtable_check /tmp/hatrie_cache_check_ahtable

/tmp/hatrie_cache_check_hattrie
/tmp/hatrie_cache_check_ahtable

compiler_supports_sanitizers() {
	tmp_c=${TMPDIR:-/tmp}/hatrie_cache_sanitize_probe.c
	tmp_bin=${TMPDIR:-/tmp}/hatrie_cache_sanitize_probe
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

if [ "$SANITIZE_C" = "auto" ]; then
	if compiler_supports_sanitizers; then
		SANITIZE_C=1
	else
		echo "skipping C sanitizer pass: compiler/runtime does not support AddressSanitizer and UBSan" >&2
		SANITIZE_C=0
	fi
fi

case "$SANITIZE_C" in
	1|true|yes)
		SAN_FLAGS="-fsanitize=address,undefined -fno-omit-frame-pointer"
		build_c_check \
			/tmp/hatrie_cache_check_hattrie_sanitize \
			"$ROOT/luikore__hat-trie/test/check_hattrie.c" \
			$SAN_FLAGS
		build_c_ahtable_check \
			/tmp/hatrie_cache_check_ahtable_sanitize \
			$SAN_FLAGS
		ASAN_OPTIONS=${ASAN_OPTIONS:-detect_leaks=1:halt_on_error=1:abort_on_error=1} \
			UBSAN_OPTIONS=${UBSAN_OPTIONS:-halt_on_error=1:abort_on_error=1} \
			/tmp/hatrie_cache_check_hattrie_sanitize
		ASAN_OPTIONS=${ASAN_OPTIONS:-detect_leaks=1:halt_on_error=1:abort_on_error=1} \
			UBSAN_OPTIONS=${UBSAN_OPTIONS:-halt_on_error=1:abort_on_error=1} \
			/tmp/hatrie_cache_check_ahtable_sanitize
		;;
	0|false|no)
		;;
	*)
		echo "SANITIZE_C must be auto, 1, or 0" >&2
		exit 2
		;;
esac
