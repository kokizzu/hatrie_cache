#!/bin/sh
set -eu

go test -race ./hat/hatCache ./hat/hatSql
