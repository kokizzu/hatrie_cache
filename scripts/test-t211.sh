#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^TestT211' -count=1
