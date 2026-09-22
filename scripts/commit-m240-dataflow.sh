#!/usr/bin/env bash
set -euo pipefail

git diff --cached --check
git commit -m "M240 explain dataflow exchange topology"
