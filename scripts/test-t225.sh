#!/usr/bin/env bash
set -euo pipefail

make test-tr024-covering-index
make test-sql-covering-indexes
make verify-tr024-covering-index
