#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSchema -run '^TestSpaceCatalog' -count=1
