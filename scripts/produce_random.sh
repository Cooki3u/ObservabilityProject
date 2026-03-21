#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-docker/docker-compose.yml}"
TOPIC="${TOPIC:-input-topic}"
BROKER="${BROKER:-kafka:9092}"
MIN_DELAY_SEC="${MIN_DELAY_SEC:-1}"
MAX_DELAY_SEC="${MAX_DELAY_SEC:-15}"

if [[ "$MAX_DELAY_SEC" -lt "$MIN_DELAY_SEC" ]]; then
  echo "MAX_DELAY_SEC must be >= MIN_DELAY_SEC" >&2
  exit 1
fi

NAMES=(
  Ari Noam Maya Lior Tal Dana Ronen Shahar Yarden Amit Erez Shaked Ori Nadav
  Meital Avital Yonatan Elad Gideon Boaz Almog Netanel Moran Hadar Shlomi Itamar
  Tomer Yosef Eitan Omer Gal
)

producer_cmd=(
  docker compose -f "$COMPOSE_FILE" exec -T kafka \
    kafka-console-producer --bootstrap-server "$BROKER" --topic "$TOPIC"
)

while true; do
  name="${NAMES[$((RANDOM % ${#NAMES[@]}))]}"
  printf '{"name":"%s"}\n' "$name"
  sleep $((MIN_DELAY_SEC + RANDOM % (MAX_DELAY_SEC - MIN_DELAY_SEC + 1)))
done | "${producer_cmd[@]}"
