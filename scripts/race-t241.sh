#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatPeer -run 'TestT241' -count=1
