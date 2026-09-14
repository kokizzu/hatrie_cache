#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDictionary -run '^TestCH028' -count=1
