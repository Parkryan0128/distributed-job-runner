#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
[[ "$PWD" == "$HOME/apps/distributed-job-runner" ]] || { echo 'Use ~/apps/distributed-job-runner.' >&2; exit 1; }
[[ -d "$HOME/apps/inventory-reservation-service/deploy/proxy/sites" ]] || { echo 'Existing portfolio proxy is required.' >&2; exit 1; }
bash deploy/init-env.sh
bash deploy/up.sh
bash deploy/install-cd-key.sh deploy/cd/distributed-job-runner.pub
printf 'Initial deployment and repository-specific CD key installation complete.\n'
