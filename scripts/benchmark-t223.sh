#!/usr/bin/env bash
set -euo pipefail

make benchmark-tr023-functional-index
make benchmark-sql-expression-index
