#!/usr/bin/env bash
set -euo pipefail

make format-tt050-typed-table-cache
make test-tt050-typed-table-cache
make race-tt050-typed-table-cache
make vet-tt050-typed-table-cache
