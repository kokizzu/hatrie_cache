#!/usr/bin/env bash
set -euo pipefail

git diff --cached --check
git status --short --branch
git diff --cached --stat
git diff --cached -- \
	hat/hatSql/mu040_schema_registry.go \
	hat/hatSql/mu040_schema_registry_test.go \
	hat/hatSql/sql_source_ingestion.go \
	hat/hatSql/mu019_source_transaction_envelope.go \
	MU040_SCHEMA_REGISTRY.md \
	PRODUCT_IDEA_GAPS.md \
	BENCHMARK.md
