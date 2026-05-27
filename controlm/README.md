# Control-M / Helix Pipeline — B3 POC

This directory contains the Control-M pipeline definition for the B3 Data Observability POC, packaged so it can run on any plausible B3 environment (Linux or Windows agents; Python global / venv / py launcher; or no Python at all).

## How it works

A single **manifest** ([jobs/manifest.yaml](jobs/manifest.yaml)) describes the pipeline. A generator ([generate.py](generate.py)) reads the manifest and emits **four Control-M JSON variants** plus a Python module consumed by the wrapper.

```
jobs/manifest.yaml           ← edit this
        │
        ▼ make ctm-generate
        │
        ├─► jobs/market_d1_script.json       (Job:Script — file on disk)
        ├─► jobs/market_d1_command.json      (Job:Command — inline command)
        ├─► jobs/market_d1_embedded.json     (Job:EmbeddedScript — inline script)
        ├─► jobs/market_d1_database.json     (Job:Database — pure SQL)
        └─► scripts/_job_io_generated.py     (JOB_IO table for run_job.py)
```

The four JSONs are equivalent in business behavior — they differ only in **how Control-M invokes the work** on the agent. B3 picks the one that matches their Helix agents.

## Compatibility matrix

Use this to pick the variant + variable overrides for B3's actual environment.

| B3 environment | Variant | Variable overrides (Helix Web UI) |
|---|---|---|
| Linux + Python global on PATH | `script` | (defaults work) `PYTHON_BIN=python3`, `SCRIPT_DIR=/scripts` |
| Linux + Python venv at `/opt/b3-venv` | `script` (call `run_job.sh`) | `PYTHON_BIN=/opt/b3-venv/bin/python`, `SCRIPT_DIR=/scripts` |
| Linux + no file deploy permission | `command` or `embedded` | same Python vars as above |
| Windows + Python via `py` launcher | `script` (call `run_job.bat`) | `PYTHON_BIN=py -3`, `SCRIPT_DIR=C:\controlm\scripts`, `PATH_SEP=\` |
| Windows + Python at `C:\Python311` | `script` | `PYTHON_BIN=C:\Python311\python.exe`, `SCRIPT_DIR=C:\controlm\scripts`, `PATH_SEP=\` |
| **No Python on agents** | `database` | `DB_CONN_ORACLE`, `DB_CONN_MSSQL` (Helix Connection Profiles) |

All variants accept the same set of Control-M variables (see [jobs/manifest.yaml](jobs/manifest.yaml) `defaults:` block):

| Variable | Purpose | Default |
|---|---|---|
| `%%CTM_AGENT_HOST%%` | Agent name in Helix | `workbench` |
| `%%RUN_AS%%` | User the job runs as | `workbench` |
| `%%PYTHON_BIN%%` | Python interpreter | `python3` |
| `%%SCRIPT_DIR%%` | Where `run_job.py` lives on the agent | `/scripts` |
| `%%PATH_SEP%%` | `/` on Linux, `\` on Windows | `/` |
| `%%DB_CONN_ORACLE%%` | Helix Connection Profile name (database variant) | — |
| `%%DB_CONN_MSSQL%%` | Helix Connection Profile name (database variant) | — |

## Files in this directory

| Path | Generated? | Purpose |
|---|---|---|
| [jobs/manifest.yaml](jobs/manifest.yaml) | no — **edit this** | Source of truth: pipeline name, datasets, jobs |
| [generate.py](generate.py) | no | Reads the manifest, writes the four JSONs + `_job_io_generated.py` |
| [setup.sh](setup.sh) | no | Deploys a variant to Workbench/Helix via Automation API |
| `jobs/market_d1_<variant>.json` | yes | Control-M folder definition per variant |
| `scripts/run_job.py` | no | Python wrapper: OpenLineage events + APM traces + structured logs to Datadog |
| `scripts/_job_io_generated.py` | yes | Imported by `run_job.py` — table of OpenLineage inputs/outputs per job |
| `scripts/run_job.sh` | no | POSIX wrapper that activates a venv (if `VENV_PATH` is set) then calls `run_job.py` |
| `scripts/run_job.bat` | no | Windows wrapper that resolves `python.exe` / `py -3` / venv |
| `scripts/run_job.sql` | no | Reference DQ SQL — copy/adapt for B3's Job:Database connection |

## Adding a new job

1. Edit [jobs/manifest.yaml](jobs/manifest.yaml) — append an entry under `jobs:`:
   ```yaml
   - name: my_new_job
     sla: "00:10"
     depends_on: publish_d1_reports   # or null for the first job
     description: "What this job does"
     inputs:
       - {ns: sqlserver, name: "dbo.SOME_TABLE"}
     outputs:
       - {ns: minio, name: "exports/some_output"}
     # Optional — only if you want the database variant to include this job:
     sql:
       type: mssql
       query: |
         SELECT COUNT(*) FROM dbo.SOME_TABLE WHERE business_date = ?
   ```

2. Regenerate:
   ```bash
   make ctm-generate
   ```

3. Deploy:
   ```bash
   make ctm-deploy-all       # or ctm-deploy-script for just one variant
   ```

That's it. No edits to `run_job.py`, JSONs, or `_job_io_generated.py` — the generator owns those.

## Local validation (Workbench)

The Control-M Workbench image is included in [docker-compose.yml](../docker-compose.yml) behind the `workbench` profile. It exposes the same Automation API as Helix on `https://localhost:8443` with `workbench / workbench` credentials.

```bash
make up-workbench                                 # boot stack + Workbench
make ctm-generate                                 # generate the 4 variants
make ctm-deploy-all                               # deploy each variant
make ctm-test-all-variants BUSINESS_DATE=$(date +%F)
```

Validate in Datadog:

- **DJM**: four pipeline runs (one per variant), four jobs each
- **Lineage**: `ASTA*` → `ADWPM*` for the three Python variants (script / command / embedded)
- **APM**: `trace.controlm.job.*` for the Python variants
- **DBM**: SQL Server / Oracle queries appear for the database variant

## Open questions for B3

Still need confirmation from Karol & team before going live in Helix:

- [ ] Linux or Windows on the agent hosts?
- [ ] Which `Job:Type` does their team standardize on today? (Script / Command / EmbeddedScript / Database)
- [ ] Python: installed? Which version? Global vs venv path?
- [ ] If venv: which path? (Required for `VENV_PATH` in `run_job.sh`.)
- [ ] If they prefer the `database` variant: which Helix Connection Profile names should we use for Oracle and SQL Server?
