#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ -f deploy/.env.production ]]; then
  echo 'Keeping existing deployment settings.'
  exit 0
fi
umask 077
settings=$(mktemp deploy/.env.production.XXXXXX)
trap 'rm -f "$settings"' EXIT
{
  printf 'APP_IMAGE=ghcr.io/parkryan0128/distributed-job-runner:sha-%s\n' "$(git rev-parse HEAD)"
  printf 'POSTGRES_PASSWORD=%s\n' "$(openssl rand -hex 32)"
} > "$settings"
ln "$settings" deploy/.env.production
echo 'Created private deployment settings.'
