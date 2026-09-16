#!/bin/sh
set -eu

gofmt -w \
	hat/hatCache/ch_u49_skip_index_explain_test.go \
	hat/hatCache/sql_index_diagnostics.go \
	hat/hatCache/sql_json_path_skip.go \
	hat/hatCache/sql_query.go \
	hat/hatCache/monitoring.go \
	hat/hatSql/contracts.go \
	hat/hatSql/index_diagnostics.go \
	hat/hatSql/model.go \
	hat/hatSql/catalog.go \
	hat/hatSql/query.go
