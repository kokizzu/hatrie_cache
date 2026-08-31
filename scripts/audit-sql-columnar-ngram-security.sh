#!/bin/sh
set -eu

go vet ./hat/hatSql ./hat/hatCache
if git diff -- hat/hatSql/contracts.go hat/hatSql/query.go hat/hatCache/sql_columnar_layout_cache.go | rg -n '^\+.*(^|[^[:alnum:]_])(os|filepath|net/http|os/exec)\.'; then
	echo 'n-gram sidecars unexpectedly added filesystem, network, or process access' >&2
	exit 1
fi
