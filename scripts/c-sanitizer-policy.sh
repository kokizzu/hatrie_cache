#!/usr/bin/env sh

strict_overcommit_enabled() {
	overcommit_path=${SANITIZE_C_OVERCOMMIT_MEMORY_PATH:-/proc/sys/vm/overcommit_memory}
	if [ ! -r "$overcommit_path" ]; then
		return 1
	fi

	overcommit_mode=$(cat "$overcommit_path" 2>/dev/null) || return 1
	[ "$overcommit_mode" = "2" ]
}

strict_overcommit_blocks_sanitizers() {
	case "${SANITIZE_C_ALLOW_STRICT_OVERCOMMIT:-0}" in
		1|true|yes)
			return 1
			;;
	esac

	strict_overcommit_enabled
}

asan_min_commit_headroom_kb() {
	printf '%s\n' "${SANITIZE_C_ASAN_MIN_COMMIT_HEADROOM_KB:-15032123396}"
}

commit_headroom_kb() {
	meminfo_path=${SANITIZE_C_MEMINFO_PATH:-/proc/meminfo}
	if [ ! -r "$meminfo_path" ]; then
		return 1
	fi

	awk '
		/^CommitLimit:/ { limit = $2 }
		/^Committed_AS:/ { committed = $2 }
		END {
			if (limit == "" || committed == "") {
				exit 1
			}
			printf "%.0f\n", limit - committed
		}
	' "$meminfo_path"
}

low_commit_headroom_blocks_sanitizers() {
	case "${SANITIZE_C_ALLOW_LOW_COMMIT_HEADROOM:-0}" in
		1|true|yes)
			return 1
			;;
	esac

	required=$(asan_min_commit_headroom_kb) || return 1
	case "$required" in
		''|*[!0-9]*)
			return 1
			;;
	esac
	if [ "$required" -eq 0 ]; then
		return 1
	fi

	available=$(commit_headroom_kb) || return 1
	case "$available" in
		''|*[!0-9-]*)
			return 1
			;;
	esac

	[ "$available" -lt "$required" ]
}
