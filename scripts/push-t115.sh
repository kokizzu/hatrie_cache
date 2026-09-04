#!/usr/bin/env bash
set -euo pipefail

bash ./scripts/commit-t115.sh
git push origin HEAD
