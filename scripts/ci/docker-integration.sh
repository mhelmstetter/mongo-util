#!/usr/bin/env bash

set -euo pipefail

readonly TESTPATTERN="${TEST_PATTERN:-*IntegrationTest,!CollModTtlIntegrationTest}"

readonly SRC_CONTAINER="mongo-util-integration-src"
readonly DST_CONTAINER="mongo-util-integration-dst"
readonly PROVISION_IMAGE="felipegasper298/mongo-provision"
readonly SRC_VERSION="4.4"
readonly DST_VERSION="8.0"

readonly SRC_PORT=27017
readonly SRC_PORTS=27017-27099
readonly DST_PORT=28017
readonly DST_PORTS=28017-28099

readonly READY_FILE_NAME="ready"
readonly TEST_PROPERTIES_FILE="src/test/resources/shard-sync.properties"

cleanup() {
  docker rm -f "$SRC_CONTAINER" "$DST_CONTAINER" >/dev/null 2>&1 || true
}

trap cleanup EXIT

start_cluster() {
  local name="$1"
  local version="$2"
  local port="$3"
  local ports="$4"

  docker rm -f "$name" >/dev/null 2>&1 || true
  docker run --detach \
    -p "$ports:$ports" \
    --name "$name" \
    "$PROVISION_IMAGE" "$version" --sharded 2 --replicaset --port "$port"
}

find_ready_file() {
  local container="$1"

  docker exec "$container" sh -lc "find / -type f -name '$READY_FILE_NAME' -print -quit 2>/dev/null"
}

await_connection_string() {
  local container="$1"
  local label="$2"
  local ready_path=""
  local connection_string=""

  for _ in $(seq 1 180); do
    ready_path="$(find_ready_file "$container" | tr -d '\r')"
    if [[ -n "$ready_path" ]]; then
      connection_string="$(docker exec "$container" sh -lc "cat '$ready_path'" | jq -r '.connection_string // empty')" || true

      if [[ -n "$connection_string" ]]; then
        echo "$connection_string"
        return 0
      fi
    fi

    sleep 2
  done

  echo "Timed out waiting for $label cluster readiness" >&2
  return 1
}

write_test_properties() {
  local src_uri="$1"
  local dst_uri="$2"

  cat > "$TEST_PROPERTIES_FILE" <<EOF
source=$src_uri
dest=$dst_uri
EOF
}

#----------------------------------------------------------
echo "Using test pattern: $TESTPATTERN"

echo "Starting source and destination clusters"
start_cluster "$SRC_CONTAINER" "$SRC_VERSION" "$SRC_PORT" "$SRC_PORTS"
start_cluster "$DST_CONTAINER" "$DST_VERSION" "$DST_PORT" "$DST_PORTS"

echo "Awaiting cluster readiness"
src_uri="$(await_connection_string "$SRC_CONTAINER" "source")"
echo "Source cluster connection string: $src_uri"

dst_uri="$(await_connection_string "$DST_CONTAINER" "destination")"
echo "Destination cluster connection string: $dst_uri"

echo "Writing integration test configuration"
write_test_properties "$src_uri" "$dst_uri"

echo "Running Java integration suite"
mvn -q test -Dtest="$TESTPATTERN"

echo "Integration suite passed"