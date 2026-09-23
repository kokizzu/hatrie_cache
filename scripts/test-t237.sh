#!/usr/bin/env bash
set -euo pipefail

go test -tags t237_impl ./hat/hatPeer -run 'TestT237ConnectionPool' -count=1
