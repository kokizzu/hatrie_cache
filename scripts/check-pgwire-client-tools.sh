#!/usr/bin/env sh
set -eu

for tool in psql java javac; do
	if command -v "$tool" >/dev/null 2>&1; then
		printf '%s: ' "$tool"
		"$tool" --version
	else
		printf '%s: unavailable\n' "$tool"
	fi
done
