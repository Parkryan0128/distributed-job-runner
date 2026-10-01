# Portfolio VM deployment

This application follows Inventory and OptiRoute's release process: CI tests the image, publishes `ghcr.io/parkryan0128/distributed-job-runner:sha-<commit>`, then the Deploy workflow uses a repository-specific forced SSH command. Only successful main pushes can deploy. The VM checks that the requested revision is still the latest main, fast-forwards a clean checkout and pulls the tested image. It does not compile Go on the VM.

## Runtime

The production stack uses one API (256 MiB), two workers (128 MiB each, two slots each), and private PostgreSQL (512 MiB). The short-lived migration container has a separate 128 MiB ceiling. Logs rotate at 10 MB × 3 files. The database has its own persistent volume and no host port. Only the API joins the existing `portfolio-edge` network, with the `job-runner-api` alias. No new public ports or proxy are created.

Caddy serves `https://distributed-job-runner.ryanparkdev.com`, preserves the public Host and streams SSE immediately without response compression. Production cookies explicitly use Secure, HttpOnly and SameSite=Strict even though Caddy terminates TLS. API scaling above one instance is unsupported because the controller and workload generator live in that process.

Manual submissions use a global 20-request burst, replenished at five per second, and a transaction-protected ceiling of 100 queued/running jobs. Idempotent replays remain available when the queue is full (subject to rate limits). The generator retains its lower 40-queued-job threshold. Sessions arbitrate 30-second turns; they are not user accounts.

Production deletes terminal jobs and their attempts after 24 hours, in batches of up to 500 each minute. Active jobs are never pruned. Job transition events expire after ten minutes. History cleanup reconnects observers to a fresh snapshot so counts reflect the retained data. Idempotency keys expire with their jobs. Local history retention stays disabled unless `HISTORY_RETENTION_HOURS` is set. Keep schema changes compatible with rollback images.

## One-time setup

Create the Route 53 A record `distributed-job-runner.ryanparkdev.com` pointing to `15.235.24.199`. Wait for a successful main CI run and make the GHCR container package public, as for the other portfolio applications. Do not enter passwords or SSH private keys into chat.

In an authenticated VM terminal:

```bash
mkdir -p ~/apps
cd ~/apps
git clone https://github.com/Parkryan0128/distributed-job-runner.git
cd distributed-job-runner
bash deploy/bootstrap.sh
```

The bootstrap generates an owner-only, Git-ignored database password, pulls the tested image, starts and health-checks the stack, installs only this application's `.local.caddy` site into Inventory's existing proxy, validates/reloads Caddy, verifies public HTTPS readiness, and installs the public CD key with a forced command restricted to this repository. Existing app data and proxy sites are preserved. Deployments share `~/.cache/portfolio-deploy.lock` to serialize changes.

After initial setup passes, set repository variable `DEPLOY_ENABLED=true` and dispatch Deploy once to verify the restricted-key path. Secrets are `DEPLOY_SSH_KEY` and the independently verified `DEPLOY_KNOWN_HOSTS`; variables are `DEPLOY_HOST`, `DEPLOY_USER`, and `DEPLOY_ENABLED`. Rotate the CD key by installing its new public key and updating the GitHub secret, then removing the old authorized entry.

## Updates and rollback

```bash
git pull --ff-only origin main
bash deploy/up.sh
# Previous successful application image, retaining database data:
bash deploy/up.sh "$(cat deploy/.previous-image)"
```

Successful image references are saved only after public health checks pass. A failed deployment reports an error and does not delete data; inspect service logs and roll back explicitly. Do not run `down -v` on the VM. A process restart releases demo control and stops generation, but persisted jobs continue on workers.

The CI production smoke test uses a disposable database and Caddy-issued local TLS CA. It verifies HTTPS assets, secure cookies, spectator exclusion, two live workers, and ordered SSE events for a real retrying task.
