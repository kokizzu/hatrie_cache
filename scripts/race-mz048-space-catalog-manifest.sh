#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSchema -run 'TestMZ048' -count=1
