#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run 'TestSealedUpsertRun' -count=1
