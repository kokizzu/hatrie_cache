#!/bin/sh
set -eu

bash scripts/format-typed-table-aggregate-key.sh
bash scripts/test-typed-table-aggregate-key.sh
