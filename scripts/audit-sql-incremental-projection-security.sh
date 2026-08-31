#!/bin/sh
set -eu

go vet ./hat/hatSql ./hat/hatCache
