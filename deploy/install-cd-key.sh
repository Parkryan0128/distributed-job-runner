#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
[[ "$PWD" == "$HOME/apps/distributed-job-runner" ]] || { echo 'Use ~/apps/distributed-job-runner.' >&2; exit 1; }
[[ $# == 1 ]] || { echo 'Usage: bash deploy/install-cd-key.sh public-key-file' >&2; exit 1; }
read -r key_type key_data _ < "$1"
[[ "$key_type" == ssh-ed25519 && "$key_data" =~ ^[A-Za-z0-9+/=]+$ ]] || exit 1
mkdir -p "$HOME/.ssh"
chmod 700 "$HOME/.ssh"
touch "$HOME/.ssh/authorized_keys"
chmod 600 "$HOME/.ssh/authorized_keys"
entry="restrict,command=\"/bin/bash $PWD/deploy/receive.sh distributed-job-runner\" $key_type $key_data github-cd-distributed-job-runner"
if ! grep -Fqx "$entry" "$HOME/.ssh/authorized_keys"; then
  printf '%s\n' "$entry" >> "$HOME/.ssh/authorized_keys"
fi
echo 'Installed repository-specific deployment key.'
