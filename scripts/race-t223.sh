#!/usr/bin/env bash
set -euo pipefail

make race-tr023-functional-index
make test-race-sql-expression-index
