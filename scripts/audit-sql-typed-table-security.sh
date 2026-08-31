#!/bin/sh
set -eu

go vet ./hat/hatSql
if rg -n '(^|[^[:alnum:]_])(os|filepath|net/http|os/exec)\.' hat/hatSql/typed_table.go; then
	echo 'typed table unexpectedly added filesystem, network, or process access' >&2
	exit 1
fi
