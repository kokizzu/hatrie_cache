#!/usr/bin/env bash
set -euo pipefail

bash ./scripts/test-t243-package.sh
bash ./scripts/race-t243.sh
bash ./scripts/vet-t243.sh
test -s T243_MTLS_CERTIFICATE_ROTATION.md
rg -q 'T243_MTLS_CERTIFICATE_ROTATION.md' README.md
rg -q '^\- \[x\] T243 ' INSPIRATION_ROUND2.md
