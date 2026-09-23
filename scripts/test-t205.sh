#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatReplication ./hat/hatCache -run 'TestT205' -count=1
