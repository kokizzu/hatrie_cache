#!/usr/bin/env bash
set -euo pipefail

GOTOOLCHAIN=auto go test ./hat/hatCache -run 'Columnar|CH046|SQLColumnar'
