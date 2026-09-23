#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^TestT242AuditAllOperations' -count=1
