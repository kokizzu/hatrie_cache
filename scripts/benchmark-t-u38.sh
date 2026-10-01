#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatReplication -run '^$' -bench 'TU38|ConflictPolicyResolution' -benchmem -count=5
