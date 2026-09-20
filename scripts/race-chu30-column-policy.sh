#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatStorage -run 'TestRemotePartCache' -count=1
