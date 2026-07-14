#!/usr/bin/env sh

strict_overcommit_blocks_sanitizers() {
	case "${SANITIZE_C_ALLOW_STRICT_OVERCOMMIT:-0}" in
		1|true|yes)
			return 1
			;;
	esac

	overcommit_path=${SANITIZE_C_OVERCOMMIT_MEMORY_PATH:-/proc/sys/vm/overcommit_memory}
	if [ ! -r "$overcommit_path" ]; then
		return 1
	fi

	overcommit_mode=$(cat "$overcommit_path" 2>/dev/null) || return 1
	[ "$overcommit_mode" = "2" ]
}
