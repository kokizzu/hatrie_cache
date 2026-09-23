#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatReplication ./hat/hatCache -run 'TestT205' -count=1
