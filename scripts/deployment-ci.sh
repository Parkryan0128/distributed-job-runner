#!/usr/bin/env bash
# Isolated production configuration smoke test, only on disposable GitHub runners.
set -euo pipefail
cd "$(dirname "$0")/.."
: "${RUNNER_TEMP:?GitHub Actions only}"
[[ "${GITHUB_ACTIONS:-}" == true ]] || exit 64
bash deploy/init-env.sh
export APP_IMAGE=distributed-job-runner:local
app=(docker compose --env-file deploy/.env.production -f deploy/compose.yml)
cleanup() {
  if [[ $? != 0 ]]; then "${app[@]}" logs --no-color --tail 100 || true; docker logs job-runner-proxy-ci || true; fi
  docker rm -fv job-runner-proxy-ci >/dev/null 2>&1 || true
  "${app[@]}" down -v
}
trap cleanup EXIT
"${app[@]}" config --quiet
docker network create portfolio-edge
"${app[@]}" up -d --wait --wait-timeout 240
printf '{\n local_certs\n}\nimport /etc/caddy/sites/*.caddy\n' > "$RUNNER_TEMP/Caddyfile.job-runner-ci"
docker run -d --name job-runner-proxy-ci --network portfolio-edge -p 127.0.0.1:8443:443 \
  -v "$RUNNER_TEMP/Caddyfile.job-runner-ci:/etc/caddy/Caddyfile:ro" \
  -v "$PWD/deploy/distributed-job-runner.caddy:/etc/caddy/sites/job-runner.caddy:ro" caddy:2-alpine
for attempt in $(seq 1 30); do
  if docker cp job-runner-proxy-ci:/data/caddy/pki/authorities/local/root.crt "$RUNNER_TEMP/job-runner-ca.crt" 2>/dev/null; then break; fi
  sleep 1
done
python3 scripts/public-smoke.py "$RUNNER_TEMP/job-runner-ca.crt"
