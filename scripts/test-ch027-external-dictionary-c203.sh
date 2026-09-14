#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDictionary -run '^TestCH027' -count=1
