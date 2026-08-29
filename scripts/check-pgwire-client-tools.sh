#!/usr/bin/env sh
set -eu

for tool in psql java javac isql odbcinst; do
	if command -v "$tool" >/dev/null 2>&1; then
		printf '%s: ' "$tool"
		"$tool" --version
	else
		printf '%s: unavailable\n' "$tool"
	fi
done

found_jar=false
for jar in /usr/share/java/postgresql*.jar; do
	if [ -f "$jar" ]; then
		printf 'postgresql-jdbc: %s\n' "$jar"
		found_jar=true
	fi
done
if [ "$found_jar" = false ]; then
	printf '%s\n' 'postgresql-jdbc: unavailable'
fi
