#!/bin/sh
set -eu

git diff --check -- Makefile README.md NOTEBOOK.md notebook_integration_test.go notebooks/hatrie_sql_analysis.ipynb scripts/audit-notebook-integration.sh scripts/test-notebook-integration.sh scripts/deliver-notebook-integration.sh
git add -- Makefile README.md NOTEBOOK.md notebook_integration_test.go notebooks/hatrie_sql_analysis.ipynb scripts/audit-notebook-integration.sh scripts/test-notebook-integration.sh scripts/deliver-notebook-integration.sh
git commit -m "feat(sql): add reproducible notebook integration"
git push
