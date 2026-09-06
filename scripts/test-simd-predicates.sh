#!/bin/sh
set -eu

go test ./hat/hatPredicate -run '^TestMatch'
