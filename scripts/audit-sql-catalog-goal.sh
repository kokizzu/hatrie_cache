#!/bin/sh
set -eu

case "${1:-overview}" in
overview)
	printf '%s\n' '=== SQL capability files ==='
	rg -l -i 'information_schema|create view|describe|show |temporary|session|generated|identity|default|coalesce|nullif|case|queryhint|explain.*json|slow.query|queryobserver' ./hat/hatSql ./hat/hatCache --glob '*.go'
	;;
contracts)
	printf '%s\n' '=== observer and explain contracts ==='
	rg -n -C 4 'type (SQLQueryObserver|QueryObserver|SQLQueryEvent|QueryEvent)|EXPLAIN|Explain' ./hat/hatSql/contracts.go ./hat/hatSql/query.go
	printf '%s\n' '=== parser and session keywords ==='
	rg -n -C 4 'keyword\("(CREATE|VIEW|SHOW|DESCRIBE|TEMP|DEFAULT|GENERATED|IDENTITY)' ./hat/hatSql/query.go || true
	;;
slow-query)
	printf '%s\n' '=== query event and execution observation ==='
	sed -n '1,75p' ./hat/hatSql/contracts.go
	sed -n '470,530p' ./hat/hatSql/query.go
	;;
packages)
	printf '%s\n' '=== package directories ==='
	find . -type f -name '*.go' -not -path './vendor/*' -exec dirname {} \; | sort -u
	;;
*)
	printf '%s\n' "unknown SQL catalog audit mode: ${1:-}" >&2
	exit 2
	;;
esac
