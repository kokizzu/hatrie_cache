#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSchema -run '^TestT227' -count=1
