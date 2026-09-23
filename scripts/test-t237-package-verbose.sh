#!/usr/bin/env bash
set -euo pipefail

go test -v ./hat/hatPeer -count=1 -timeout=2m
