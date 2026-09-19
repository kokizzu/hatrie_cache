#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatMembership -count=1
