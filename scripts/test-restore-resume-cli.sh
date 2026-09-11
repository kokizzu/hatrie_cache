#!/usr/bin/env bash
set -euo pipefail

go test ./cmd/hatrie-cli -run '^TestRunRestoreBundleVerifiesAndRestores$' -count=1
