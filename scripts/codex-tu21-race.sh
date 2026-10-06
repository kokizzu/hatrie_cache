#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSchema -run '^TestSpaceMigrationManager' -count=1
