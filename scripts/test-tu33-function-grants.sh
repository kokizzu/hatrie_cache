#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatAuth -run 'TestRoleCatalogFunctionGrant' -count=1
