#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatStorage -count=1
