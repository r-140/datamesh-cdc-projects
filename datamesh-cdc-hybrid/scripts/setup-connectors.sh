#!/bin/sh
set -eu
for file in /connectors/*.json; do
  name=$(sed -n 's/.*"name"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' "$file" | head -1)
  code=$(curl -s -o /dev/null -w '%{http_code}' "http://kafka-connect:8083/connectors/$name")
  [ "$code" = 200 ] || curl -fsS -X POST -H 'Content-Type: application/json' --data-binary "@$file" http://kafka-connect:8083/connectors
done
