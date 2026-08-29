#!/bin/sh
set -eu

git diff --check -- README.md Makefile hat/hatAuth/identity.go hat/hatAuth/identity_test.go hat/hatCache/monitoring.go hat/hatCache/monitoring_test.go scripts/test-auth-identity.sh scripts/test-monitoring-identity.sh scripts/deliver-pluggable-monitoring-identity.sh
git add -- README.md Makefile hat/hatAuth/identity.go hat/hatAuth/identity_test.go hat/hatCache/monitoring.go hat/hatCache/monitoring_test.go scripts/test-auth-identity.sh scripts/test-monitoring-identity.sh scripts/deliver-pluggable-monitoring-identity.sh
git commit -m "feat(auth): add pluggable monitoring identity"
git push
