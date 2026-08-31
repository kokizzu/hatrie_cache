#!/bin/sh
set -eu

printf '%s\n' '== commits from the last two weeks =='
git log --since='2 weeks ago' --format='%h %ad %s' --date=short
printf '%s\n' '== deleted Go files from the last two weeks =='
git log --since='2 weeks ago' --diff-filter=D --format='' --name-only -- '*.go'
printf '%s\n' '== verification =='
go test ./...
go vet ./...
