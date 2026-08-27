#!/bin/sh
set -eu

git diff --check -- Makefile SQL.md ./cmd/hatrie-sql-lsp ./scripts/test-sql-language-server.sh ./scripts/format-sql-language-server.sh ./scripts/sql-language-server.sh
git status --short
