#!/bin/sh
set -eu

go test ./hat/hatSql ./hat/hatSchema ./hat/hatCache ./cmd/hatrie-cli
