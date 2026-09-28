#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPgWire -count=1
