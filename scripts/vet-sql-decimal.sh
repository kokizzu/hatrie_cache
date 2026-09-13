#!/bin/sh
set -eu

go vet ./hat/hatSql ./hat/hatSchema ./hat/hatCache ./cmd/hatrie-cli
