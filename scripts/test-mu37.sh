#!/usr/bin/env bash
set -euo pipefail

go test -tags mu37 ./hat/hatStorage -run 'TestMU37CompactionDiagnostics' -count=1
