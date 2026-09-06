#!/bin/sh
set -eu

go test -race ./hat/hatPredicate -run '^TestMatch'
