#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatFiber -run 'TestTT033' -count=1
