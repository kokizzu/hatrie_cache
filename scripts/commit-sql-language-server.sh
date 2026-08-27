#!/bin/sh
set -eu

git add -- Makefile SQL.md ./cmd/hatrie-sql-lsp ./scripts/test-sql-language-server.sh ./scripts/format-sql-language-server.sh ./scripts/sql-language-server.sh ./scripts/review-sql-language-server.sh ./scripts/commit-sql-language-server.sh
git commit -m "feat: add SQL language server transport"
git push
