#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSchema -run '^TestSpaceMigrationManager' -count=1
