#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatPeer -run 'TestT237ConnectionPool' -count=1
