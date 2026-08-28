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
*)
	printf '%s\n' "unknown SQL catalog audit mode: ${1:-}" >&2
	exit 2
	;;
esac
