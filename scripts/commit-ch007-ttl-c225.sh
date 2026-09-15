#!/usr/bin/env bash
set -euo pipefail

bash ./scripts/stage-ch007-ttl-c225.sh
git commit -m "optimize typed table TTL expiry purges"
