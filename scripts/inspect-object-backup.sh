#!/bin/sh
set -eu

awk '{print NR ":" $0}' hat/hatBackup/object_store.go
rg -n -C 8 'type .*ObjectStore|func TestObjectStore|NewObjectStoreTarget' hat/hatBackup/object_store_test.go
