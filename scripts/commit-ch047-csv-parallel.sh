#!/usr/bin/env bash
set -euo pipefail

bash scripts/stage-ch047-csv-parallel.sh
git commit -m 'build: track CH-047 workflow scripts'
