#!/usr/bin/env bash
set -euo pipefail

make format-c212-typed-table-order-cache
make test-c212-typed-table-order-cache
make test-sql-typed-table
make race-c212-typed-table-order-cache
make vet-c212-typed-table-order-cache
make review-c212-typed-table-order-cache
