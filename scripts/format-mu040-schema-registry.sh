#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatSql/mu040_schema_registry.go \
	hat/hatSql/mu040_schema_registry_test.go \
	hat/hatSql/sql_source_ingestion.go \
	hat/hatSql/mu019_source_transaction_envelope.go
