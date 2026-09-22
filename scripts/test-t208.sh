#!/usr/bin/env bash
set -euo pipefail

go test -tags t208 ./hat/hatReplication -run '^TestT208' -count=1
