#!/bin/sh
set -eu

go test ./hat/hatBackup -run '^TestObjectStoreTarget'
