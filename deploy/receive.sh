#!/usr/bin/env bash
# Forced SSH command. Each deployment key is restricted to one repository.
set -euo pipefail
repo=${1:-}
[[ "$repo" == distributed-job-runner ]] || exit 64
if [[ ! "${SSH_ORIGINAL_COMMAND:-}" =~ ^deploy\ ([a-f0-9]{40})$ ]]; then
  echo 'Only deploy <commit SHA> is allowed.' >&2
  exit 64
fi
revision=${BASH_REMATCH[1]}
mkdir -p "$HOME/.cache"
exec 9>"$HOME/.cache/portfolio-deploy.lock"
flock -w 600 9
export PORTFOLIO_DEPLOY_LOCKED=1
cd "$HOME/apps/$repo"
[[ "$(git branch --show-current)" == main ]] || { echo 'Deployment checkout must be on main.' >&2; exit 1; }
git diff --quiet && git diff --cached --quiet || { echo 'Tracked server changes must be resolved before deployment.' >&2; exit 1; }
git fetch origin main
if [[ "$(git rev-parse FETCH_HEAD)" != "$revision" ]]; then
  echo 'Skipped: a newer main commit has superseded this release.'
  exit 0
fi
git merge --ff-only "$revision"
bash deploy/up.sh "ghcr.io/parkryan0128/$repo:sha-$revision"
