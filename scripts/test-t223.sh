#!/usr/bin/env bash
set -euo pipefail

make test-tr023-functional-index
make test-sql-expression-index
