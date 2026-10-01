#!/usr/bin/env bash
set -euo pipefail
go test ./hat/hatAuth -run 'TestPolicyAuthorizesFunction|TestRoleCatalogAuthorizesFunction' -count=1
