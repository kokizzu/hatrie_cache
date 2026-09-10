#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/compiled.go hat/hatSql/query.go hat/hatSql/compiled_template_reuse_test.go
