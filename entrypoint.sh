#!/bin/bash
set -e

if [[ -f "/etc/default/kafka-kit/env" ]]; then
  source /etc/default/kafka-kit/env
fi

exec "$@"
