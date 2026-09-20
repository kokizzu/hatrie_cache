#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatStorage -count=1
go test -race ./hat/hatStorage -run 'TestSQL(ResolverAdapter|NamespaceAdapter|AdapterRegistry)' -count=1
go vet ./hat/hatStorage
