#!/usr/bin/env bash
set -euo pipefail

bash ./scripts/benchmark-ch011-projection-query.sh
bash ./scripts/benchmark-ch011-projection-store.sh
