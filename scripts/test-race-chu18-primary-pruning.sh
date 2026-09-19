#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatPrimaryPruning -count=1
