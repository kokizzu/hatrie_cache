#!/bin/sh
set -eu

go test ./hat/hatCommand ./hat/hatCache
go test -race ./hat/hatCommand ./hat/hatCache
go vet ./hat/hatCommand ./hat/hatCache
git diff --check
