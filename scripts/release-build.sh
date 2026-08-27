#!/bin/sh
set -eu

mkdir -p ./dist
go build -trimpath -buildvcs=false -ldflags=-buildid= -o ./dist/hatrie-cache ./cmd/hatrie-cache
go build -trimpath -buildvcs=false -ldflags=-buildid= -o ./dist/hatrie-cli ./cmd/hatrie-cli
go build -trimpath -buildvcs=false -ldflags=-buildid= -o ./dist/hatrie-sql-lsp ./cmd/hatrie-sql-lsp
sha256sum ./dist/hatrie-cache ./dist/hatrie-cli ./dist/hatrie-sql-lsp
