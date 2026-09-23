#!/usr/bin/env bash
set -euo pipefail

bash scripts/test-sql-index-hints.sh
bash scripts/test-tu26.sh
bash scripts/test-sql-index-advisor.sh
bash scripts/test-sql-projection-advisor.sh
