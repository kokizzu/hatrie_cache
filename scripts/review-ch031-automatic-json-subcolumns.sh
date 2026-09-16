#!/usr/bin/env bash
set -euo pipefail

git status --short
git diff --check -- \
	README.md \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	INSPIRATION_BACKLOG.md \
	BENCHMARK.md \
	CH031_AUTOMATIC_TYPED_JSON_SUBCOLUMNS.md \
	hat/hatSql/ch031_automatic_json_subcolumn.go \
	hat/hatSql/ch031_automatic_json_subcolumn_test.go \
	hat/hatSql/ch031_automatic_json_subcolumn_benchmark_test.go \
	Makefile \
	scripts/format-ch031-automatic-json-subcolumns.sh \
	scripts/run-ch031-automatic-json-subcolumns.sh
git diff --stat -- \
	README.md \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	INSPIRATION_BACKLOG.md \
	BENCHMARK.md \
	CH031_AUTOMATIC_TYPED_JSON_SUBCOLUMNS.md \
	hat/hatSql/ch031_automatic_json_subcolumn.go \
	hat/hatSql/ch031_automatic_json_subcolumn_test.go \
	hat/hatSql/ch031_automatic_json_subcolumn_benchmark_test.go \
	Makefile \
	scripts/format-ch031-automatic-json-subcolumns.sh \
	scripts/run-ch031-automatic-json-subcolumns.sh
git diff -- \
	README.md \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	INSPIRATION_BACKLOG.md \
	BENCHMARK.md
