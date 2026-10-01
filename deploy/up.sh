#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
[[ -f deploy/.env.production ]] || { echo 'Run bash deploy/init-env.sh first.' >&2; exit 1; }
export APP_IMAGE=${1:-ghcr.io/parkryan0128/distributed-job-runner:sha-$(git rev-parse HEAD)}
[[ "$APP_IMAGE" =~ ^ghcr.io/parkryan0128/distributed-job-runner(:sha-[a-f0-9]{40}|@sha256:[a-f0-9]{64})$ ]] || { echo 'Invalid image reference.' >&2; exit 1; }
if [[ "${PORTFOLIO_DEPLOY_LOCKED:-0}" != 1 ]]; then
  mkdir -p "$HOME/.cache"
  exec 9>"$HOME/.cache/portfolio-deploy.lock"
  flock -w 600 9
fi
docker_command=(docker)
if ! docker info >/dev/null 2>&1; then
  docker_command=(sudo env "APP_IMAGE=$APP_IMAGE" docker)
fi
app=("${docker_command[@]}" compose --env-file deploy/.env.production -f deploy/compose.yml)
inventory_dir="${INVENTORY_DIR:-$HOME/apps/inventory-reservation-service}"
[[ -f "$inventory_dir/deploy/.env.production" ]] || { echo 'The shared Inventory proxy must be installed first.' >&2; exit 1; }
proxy=("${docker_command[@]}" compose --env-file "$inventory_dir/deploy/.env.production" -f "$inventory_dir/deploy/proxy/compose.yml")
"${docker_command[@]}" network inspect portfolio-edge >/dev/null
"${app[@]}" config --quiet
"${app[@]}" pull
"${app[@]}" up -d --wait --wait-timeout 240
site="$inventory_dir/deploy/proxy/sites/distributed-job-runner.local.caddy"
backup=$(mktemp)
had_site=0
if [[ -f "$site" ]]; then cp "$site" "$backup"; had_site=1; fi
trap 'rm -f "$backup"' EXIT
cp deploy/distributed-job-runner.caddy "$site"
if ! "${proxy[@]}" exec -T caddy caddy validate --config /etc/caddy/Caddyfile; then
  if [[ "$had_site" == 1 ]]; then cp "$backup" "$site"; else rm -f "$site"; fi
  exit 1
fi
"${proxy[@]}" exec -T caddy caddy reload --config /etc/caddy/Caddyfile
curl --fail --silent --show-error --retry 12 --retry-delay 5 --retry-all-errors \
  --connect-timeout 5 --max-time 10 https://distributed-job-runner.ryanparkdev.com/readyz
curl --fail --silent --show-error --connect-timeout 5 --max-time 10 \
  https://distributed-job-runner.ryanparkdev.com/api/workers | \
  python3 -c 'import json,sys; workers=json.load(sys.stdin); assert len([w for w in workers if w["online"] and w["concurrency"]==2])==2, "Both workers must be online"'
umask 077
if [[ -f deploy/.deployed-image ]] && [[ "$(cat deploy/.deployed-image)" != "$APP_IMAGE" ]]; then
  cp deploy/.deployed-image deploy/.previous-image
fi
printf '%s\n' "$APP_IMAGE" > deploy/.deployed-image
env_temp=$(mktemp deploy/.env.production.XXXXXX)
trap 'rm -f "$backup" "$env_temp"' EXIT
awk -v image="$APP_IMAGE" '/^APP_IMAGE=/ {$0="APP_IMAGE=" image} {print}' deploy/.env.production > "$env_temp"
mv "$env_temp" deploy/.env.production
printf '\nDeployed %s\nOpen https://distributed-job-runner.ryanparkdev.com/\n' "$APP_IMAGE"
