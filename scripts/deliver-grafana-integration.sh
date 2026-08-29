#!/bin/sh
set -eu

git diff --check -- Makefile README.md GRAFANA.md hat/hatCache/monitoring.go hat/hatCache/monitoring_test.go scripts/audit-grafana-integration.sh scripts/test-grafana-integration.sh scripts/deliver-grafana-integration.sh
git add -- Makefile README.md GRAFANA.md hat/hatCache/monitoring.go hat/hatCache/monitoring_test.go scripts/audit-grafana-integration.sh scripts/test-grafana-integration.sh scripts/deliver-grafana-integration.sh
git commit -m "feat(monitoring): add Grafana SQL datasource API"
git push
