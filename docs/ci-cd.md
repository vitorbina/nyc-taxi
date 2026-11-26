# CI/CD and deployment

Every push to `main` is checked by GitHub Actions and, only if every check passes, deployed to a server running [Dokploy](https://dokploy.com). Pull requests get the same checks without the deploy.

```
push to main
  └─ GitHub Actions (.github/workflows/ci.yml)
       ├─ lint   ruff check + ruff format --check
       ├─ stack  compose config → build every image → DAG import + dbt parse inside the built Airflow image
       └─ deploy (needs lint + stack, push to main only)
            └─ POST {DOKPLOY_URL}/api/compose.deploy
                 └─ Dokploy: re-clone the repo, docker compose -f docker-compose.yml up -d --build
                      └─ compose recreates only the services whose image or config changed
```

| Trigger | lint | stack | deploy |
|---|---|---|---|
| push to `main` | ✓ | ✓ | ✓ (if both pass) |
| pull request | ✓ | ✓ | skipped |
| manual run (Actions → CI → Run workflow) | ✓ | ✓ | skipped |

A newer push to the same branch cancels the run still in progress (`concurrency`). The workflow token is read-only (`permissions: contents: read`).

## Configuration

| Where | Name | What |
|---|---|---|
| GitHub → Settings → Secrets and variables → Actions → **Secrets** | `DOKPLOY_API_KEY` | Dokploy API key (Profile → API/CLI Keys), scoped to the organization that owns the project. Nobody can read it back, and Actions masks it in logs. |
| same page → **Variables** | `DOKPLOY_URL` | Dokploy panel base URL |
| same page → **Variables** | `DOKPLOY_COMPOSE_ID` | ID of the compose service (the last part of its URL in the panel) |
| Dokploy → project → compose service → **Environment** | the app's `.env` | Every variable in `.env.example`, with production values. Dokploy writes it next to `docker-compose.yml` at deploy time. It is never committed. |

On the Dokploy service: provider GitHub, branch `main`, compose path `./docker-compose.yml`, **Autodeploy OFF**.

Without `DOKPLOY_API_KEY` (a fork, for example) the deploy job prints a notice and exits successfully.

### Why the API and not the webhook URL

Dokploy's webhook URL is meant to be called by GitHub itself. It reads the branch from GitHub's push payload, and it only works with Autodeploy ON. With Autodeploy ON, every push deploys immediately, without waiting for CI. The API call works with Autodeploy OFF, so only a green CI deploys. The call uses `curl --fail-with-body`, so any error response fails the job and prints Dokploy's message. An early version used the webhook with plain `--fail`: Dokploy answered `301 {"message":"Branch Not Match"}`, and the job went green without deploying anything.

### Rotating the API key

1. Dokploy → Profile → API/CLI Keys → generate a new key (organization: the project's, with an expiration date).
2. `gh secret set DOKPLOY_API_KEY` and paste it.
3. Delete the old key in Dokploy.

When a key expires, the next deploy job fails with Dokploy's error, so it shows up right away.

## Images are the deploy artifact

Code and configuration are built into the images:

- **Airflow:** `airflow/dags`, `airflow/utils`, `airflow/staging`, `dbt`
- **Trino:** `trino/etc`
- **Superset:** `superset_config.py`, plus the dashboards zip, built from the YAML in `dashboards/`

Only data and state live in volumes.

**Why not bind mounts:** Dokploy deletes and re-clones the repo folder on every deploy. A running container that compose doesn't recreate keeps its bind mount pointing at the deleted folder. On the server, the Airflow containers saw empty `dags/` and `dbt/` folders after the first redeploy. With the code in the image, a code change produces a new image, and compose recreates exactly the containers that use it.

**Local development** still binds the Airflow code, so edits apply without a rebuild. That happens in `docker-compose.override.yml`, which `docker compose` loads automatically. The server and CI pass `-f docker-compose.yml`, so they never load it. That's why CI also checks the DAGs and dbt against the code **inside the built image**: it is the exact artifact the server runs.

**Airflow state** lives in the `airflow_state` volume, shared by every Airflow container. It holds one `airflow.cfg` and the generated passwords file. Airflow writes `airflow.cfg` on first start (`airflow-init` runs first) with random secrets: the JWT secret that signs task tokens, and the Fernet key that encrypts Variables. When each container generated its own, the API rejected the scheduler's tokens, and tasks failed before writing a single log line.

## Public URLs

Only **Superset** (container port 8088) and **Airflow** (8080) are published, through Dokploy → Domains (Traefik). Trino has no authentication and MinIO holds the lake, so both stay on the internal Docker network. All published host ports are bound to `127.0.0.1`.

The current domains are Dokploy's generated test hosts (`*.sslip.io`), which are HTTP only. For anything beyond testing, point a real subdomain at the server, set it as the host in Domains and turn on HTTPS: Dokploy issues the Let's Encrypt certificate. Domain changes apply on the next deploy.

## Operating the server (no SSH)

Dokploy → compose service → **General → Open Terminal** opens a shell in any container.

- **Airflow admin password:** in the `nyc_airflow_webserver` terminal, run `cat /opt/airflow/state/simple_auth_manager_passwords.json.generated`.
- **Seed data:** in the `nyc_airflow_scheduler` terminal, run the commands below. `scripts/backfill.sh` detects that it runs inside the Airflow container and calls the `airflow` CLI directly. `setsid nohup` keeps it running after the terminal closes. The log is in the logs volume.

  ```bash
  curl -fsSL -o /tmp/backfill.sh https://raw.githubusercontent.com/<owner>/nyc-taxi/main/scripts/backfill.sh
  setsid nohup bash /tmp/backfill.sh 2025-01-01 2025-03-01 > /opt/airflow/logs/seed.log 2>&1 < /dev/null &
  tail -f /opt/airflow/logs/seed.log
  ```

- **Logs and deploy history:** use the service's Logs and Deployments tabs.
- Avoid pushing to `main` while a long backfill runs. A deploy that changes the Airflow image recreates the scheduler, and that kills the running tasks.

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `Build the images` fails with `digest mismatch` on every Hive jar, all with the same digest | The download returned an error page instead of the jar. Maven Central intermittently served one to GitHub runners. | The jars come from Google's Maven Central mirror. `ADD --checksum` still pins the exact files, so a wrong download can never get into an image. |
| Deploy job green, but no new deployment in Dokploy | The call wasn't rejected with a 4xx/5xx (see the webhook note above). | The job prints Dokploy's response. A real deploy answers `{"success":true,"message":"Deployment queued",...}`. |
| DAGs disappear (`is_stale: True`) after a deploy | A bind mount pointed at the re-cloned folder. | Code is in the images now (see above). |
| Tasks fail with empty log files | Airflow containers with different JWT secrets. | One shared `airflow.cfg` (`AIRFLOW_CONFIG` on the state volume). |
