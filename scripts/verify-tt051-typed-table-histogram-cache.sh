#!/usr/bin/env bash
set -euo pipefail

make format-tt051-typed-table-histogram-cache
make test-tt051-typed-table-histogram-cache
make race-tt051-typed-table-histogram-cache
make vet-tt051-typed-table-histogram-cache
