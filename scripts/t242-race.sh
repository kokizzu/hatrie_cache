#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatCache -run '^TestT242AuditAllOperations' -count=1
