#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatStorage -run '^TestCHU33' -count=1
