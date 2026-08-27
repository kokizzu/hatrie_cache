#!/bin/sh
set -eu

mkdir -p ./dist
go run ./cmd/hatrie-sbom -output ./dist/sbom.spdx.json
