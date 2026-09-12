#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatPeer -count=1
