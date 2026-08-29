#!/bin/sh
set -eu

git diff --check -- Makefile README.md OPENAPI.md hat/hatCache/monitoring.go hat/hatCache/monitoring_test.go scripts/audit-openapi-management.sh scripts/test-openapi-management.sh scripts/deliver-openapi-management.sh
git add -- Makefile README.md OPENAPI.md hat/hatCache/monitoring.go hat/hatCache/monitoring_test.go scripts/audit-openapi-management.sh scripts/test-openapi-management.sh scripts/deliver-openapi-management.sh
git commit -m "feat(monitoring): serve OpenAPI management contract"
git push
