#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSchema -run 'TestTT027' -count=1
