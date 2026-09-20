#!/usr/bin/env bash
set -euo pipefail

git add \
	BENCHMARK.md \
	Makefile \
	PRODUCT_IDEA_GAPS.md \
	MU040_SCHEMA_REGISTRY.md \
	hat/hatSql/mu019_source_transaction_envelope.go \
	hat/hatSql/sql_source_ingestion.go \
	hat/hatSql/mu040_schema_registry.go \
	hat/hatSql/mu040_schema_registry_test.go \
	scripts/benchmark-mu040-schema-registry.sh \
	scripts/commit-mu040-schema-registry.sh \
	scripts/format-mu040-schema-registry.sh \
	scripts/race-mu040-schema-registry.sh \
	scripts/review-mu040-schema-registry.sh \
	scripts/stage-mu040-schema-registry.sh \
	scripts/push-mu040-schema-registry.sh \
	scripts/amend-mu040-schema-registry.sh \
	scripts/test-mu040-schema-registry.sh \
	scripts/vet-mu040-schema-registry.sh
git diff --cached --check
git status --short
