#!/bin/sh
set -eu

go test -race ./hat/hatSql ./hat/hatSchema ./hat/hatCache ./cmd/hatrie-cli
