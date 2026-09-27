#!/usr/bin/env bash
set -euo pipefail
bash scripts/format-ch011-source-notifications.sh
bash scripts/test-ch011-source-notifications.sh
bash scripts/race-ch011-source-notifications.sh
bash scripts/vet-ch011-source-notifications.sh
