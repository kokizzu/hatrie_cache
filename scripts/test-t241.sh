#!/usr/bin/env bash
set -euo pipefail

go test -tags t241_impl ./hat/hatPeer -run 'TestT241' -count=1
