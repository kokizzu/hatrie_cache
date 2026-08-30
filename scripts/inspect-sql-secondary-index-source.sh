#!/usr/bin/env sh
set -eu

rg -n -A28 -B4 'CreateSQLJSONBitmapIndex|CreateSQLJSONTextIndex|CreateSQLJSONCompositeIndex' hat/hatCache
rg -n -A24 -B4 'ResolveSQLTextSource|ResolveSQLSecondaryIndexedSource|ResolveSQLCompositeIndexedSource' hat/hatCache
