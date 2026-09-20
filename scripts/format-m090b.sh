#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatSql/contracts.go \
	hat/hatSql/query.go \
	hat/hatSql/session.go \
	hat/hatSql/catalog.go \
	hat/hatSql/m052p_auto_native_dataflow.go \
	hat/hatSql/whatif.go \
	hat/hatSql/m090b_context_source_resolver_test.go
