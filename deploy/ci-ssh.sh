#!/usr/bin/env bash
set -euo pipefail
: "${DEPLOY_SSH_KEY:?Configure DEPLOY_SSH_KEY}"
: "${DEPLOY_KNOWN_HOSTS:?Configure the verified server host key}"
[[ "${DEPLOY_HOST:-}" =~ ^[A-Za-z0-9.-]+$ ]] || exit 64
[[ "${DEPLOY_USER:-}" =~ ^[a-z_][a-z0-9_-]*$ ]] || exit 64
[[ "${RELEASE_SHA:-}" =~ ^[a-f0-9]{40}$ ]] || exit 64
umask 077
ssh_dir=$(mktemp -d)
trap 'rm -rf -- "$ssh_dir"' EXIT
printf '%s\n' "$DEPLOY_SSH_KEY" > "$ssh_dir/key"
printf '%s\n' "$DEPLOY_KNOWN_HOSTS" > "$ssh_dir/known_hosts"
ssh -T -i "$ssh_dir/key" -o IdentitiesOnly=yes -o BatchMode=yes \
  -o StrictHostKeyChecking=yes -o UserKnownHostsFile="$ssh_dir/known_hosts" \
  -o HostKeyAlgorithms=ssh-ed25519 -o ConnectTimeout=15 \
  -o ServerAliveInterval=15 -o ServerAliveCountMax=4 \
  "$DEPLOY_USER@$DEPLOY_HOST" "deploy $RELEASE_SHA"
