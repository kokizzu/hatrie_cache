#!/usr/bin/env bash
set -euo pipefail

git diff --check
git status --short
git diff --stat -- hat/hatSql/m_u35_snapshot_blocking.go hat/hatSql/m_u35_snapshot_blocking_test.go hat/hatSql/m_u35_snapshot_blocking_benchmark_test.go hat/hatSql/sql_source_frontier.go MU035_SNAPSHOT_BLOCKING.md PRODUCT_IDEA_GAPS.md scripts Makefile
git diff -- Makefile PRODUCT_IDEA_GAPS.md MU035_SNAPSHOT_BLOCKING.md hat/hatSql/m_u35_snapshot_blocking.go hat/hatSql/m_u35_snapshot_blocking_test.go hat/hatSql/m_u35_snapshot_blocking_benchmark_test.go hat/hatSql/sql_source_frontier.go
