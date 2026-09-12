#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatReplication -run '^TestConflictPolicyRegistry' -count=1
