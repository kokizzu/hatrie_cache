#!/bin/sh
set -eu

go vet ./hat/hatSql
if git diff -- hat/hatSql/projection_advisor.go hat/hatSql/query.go | rg -n '^\+.*(^|[^[:alnum:]_])(os|filepath|net/http|os/exec)\.'; then
	echo 'projection advisor unexpectedly added filesystem, network, or process access' >&2
	exit 1
fi
