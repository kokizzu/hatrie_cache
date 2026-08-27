#!/bin/sh
set -eu

go test ./hat/hatSql -run '^TestNamespaceResourceLimits|^TestNewNamespaceQueryGovernor' -count=1
