#!/usr/bin/env bash
set -e

cd "$RABBITMQ_PIKA_ADAPTER_ROOT" || {
  echo "ERROR: RABBITMQ_PIKA_ADAPTER_ROOT is not set or invalid."
  exit 1
}

while IFS='=' read -r key value; do
  [[ -z "$key" || "$key" =~ ^\; ]] && continue

  if [[ -z "$value" ]]; then
    echo "ERROR: Variable $key has no value."
    exit 1
  fi

  export "$key=$value"
done < .env

python -m pytest -q
