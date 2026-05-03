"""
fire_import.py

Trigger an Aura Bulk Import job and poll until it reaches a terminal state.

Workflow this script automates:
  1. Authenticate to the Aura public API at api.neo4j.io.
  2. POST /v2beta1/organizations/{org}/projects/{proj}/import/jobs with
     a model id (created via the Aura Console UI) and a target instance id.
  3. Poll GET /v2beta1/.../import/jobs/{job_id} every --poll-seconds until
     the job reaches a terminal state.
  4. Print elapsed wall time and the full final job payload.

What this script does NOT do (by design):
  - It does not create the import model. Model creation is currently a
    one-time UI step in the Aura Console (the v2beta1 API does not expose
    model CRUD to ordinary OAuth keys). Once a model is saved, the same
    importModelId can be reused for unlimited triggered runs.
  - It does not stage source files. Run a separate `gsutil cp` to upload
    parquet/CSV to the bucket the model points at, before triggering.

Usage (Business Critical instance — no DB credentials needed in body):
    python fire_import.py \\
        --api-credentials /path/to/Neo4j-credentials-Agent_Key.txt \\
        --organization-id <UUID> \\
        --project-id <UUID> \\
        --model-id <model UUID from console> \\
        --instance-id e3290355

  Use `--list-orgs` to discover the org/project IDs your key has access to:
    python fire_import.py \\
        --api-credentials .../api-creds.txt \\
        --list-orgs

Free / Virtual Dedicated Cloud tiers also need DB user/password in the body:
    python fire_import.py ... --db-username neo4j --db-password '<pw>'
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

from aura_lifecycle import AuraApiCredentials, AuraApiError, AuraClient


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--api-credentials", required=True, type=Path,
                   help="File with CLIENT_ID and CLIENT_SECRET for the Aura public API.")
    p.add_argument("--list-orgs", action="store_true",
                   help="Discovery mode: print organizations/projects this key can see, exit.")
    p.add_argument("--organization-id",
                   help="Aura organization (UUID). Same as project-id for personal tenants.")
    p.add_argument("--project-id",
                   help="Aura project (UUID). Same as organization-id for personal tenants.")
    p.add_argument("--model-id",
                   help="Import model UUID (from the Aura Console after saving the model).")
    p.add_argument("--instance-id",
                   help="Target Aura instance id (e.g. e3290355). DESTRUCTIVE: existing data is overwritten.")
    p.add_argument("--db-username", default=None,
                   help="Required only for Free and VDC tiers. Omit for Business Critical.")
    p.add_argument("--db-password", default=None,
                   help="Required only for Free and VDC tiers.")
    p.add_argument("--poll-seconds", type=float, default=10.0,
                   help="Status poll interval. Default 10s.")
    p.add_argument("--max-wait-seconds", type=float, default=3600.0,
                   help="Hard cap on the total polling time. Default 3600s (1 hour).")
    p.add_argument("--no-poll", action="store_true",
                   help="Submit the job and exit immediately. Caller polls separately.")
    return p.parse_args()


def list_orgs_and_exit(client: AuraClient) -> int:
    orgs = client.list_organizations_v2()
    if not orgs:
        print("No organizations visible to this API key.")
        return 1
    print(f"{'organization id':<40} {'name'}")
    print("-" * 90)
    for org in orgs:
        org_id = org["id"]
        name = org.get("name", "(no name)")
        print(f"{org_id:<40} {name}")
        try:
            projs = client.list_projects_v2(org_id)
        except AuraApiError as e:
            print(f"  (could not list projects: {e})")
            continue
        for proj in projs:
            print(f"  └── project {proj['id']:<35} {proj.get('name', '(no name)')}")
    return 0


def main() -> int:
    args = parse_args()
    creds = AuraApiCredentials.from_file(args.api_credentials)
    client = AuraClient(creds)

    if args.list_orgs:
        return list_orgs_and_exit(client)

    for required, value in (
        ("--organization-id", args.organization_id),
        ("--project-id", args.project_id),
        ("--model-id", args.model_id),
        ("--instance-id", args.instance_id),
    ):
        if not value:
            print(f"Error: {required} is required (use --list-orgs to discover org/project ids).",
                  file=sys.stderr)
            return 2

    print("=" * 60)
    print("Aura Bulk Import job")
    print("=" * 60)
    print(f"  organization id : {args.organization_id}")
    print(f"  project id      : {args.project_id}")
    print(f"  model id        : {args.model_id}")
    print(f"  target db       : {args.instance_id}  (DESTRUCTIVE — existing data will be overwritten)")
    print(f"  poll interval   : {args.poll_seconds:.1f}s")
    print(f"  wait ceiling    : {args.max_wait_seconds:.0f}s")
    print()

    t0 = time.time()
    try:
        submit_resp = client.submit_import_job(
            organization_id=args.organization_id,
            project_id=args.project_id,
            import_model_id=args.model_id,
            db_id=args.instance_id,
            db_username=args.db_username,
            db_password=args.db_password,
        )
    except AuraApiError as e:
        print(f"Submit failed: {e}", file=sys.stderr)
        return 3

    job_id = submit_resp.get("id") or submit_resp.get("jobId") or submit_resp.get("job_id")
    if not job_id:
        print("Submit response did not include a job id. Full payload:")
        print(json.dumps(submit_resp, indent=2, default=str))
        return 3

    print(f"submitted at t+{time.time() - t0:.1f}s, job id = {job_id}")
    print(f"initial submit response: {json.dumps(submit_resp, default=str)[:300]}")
    print()

    if args.no_poll:
        print("--no-poll set; exiting after submit. Use get_import_job to check status later.")
        return 0

    print(f"polling every {args.poll_seconds:.0f}s ...")
    print()

    def on_status(job: dict) -> None:
        elapsed = time.time() - t0
        status = job.get("status", "?")
        # Aura may surface progress under various keys; we show whichever is set.
        extra_keys = ("progress", "phase", "step", "stepDescription", "details")
        extras = " ".join(f"{k}={job[k]}" for k in extra_keys if k in job and job[k])
        suffix = f"  {extras}" if extras else ""
        print(f"  [{elapsed:7.1f}s] status={status}{suffix}")

    try:
        final = client.wait_for_import_job(
            organization_id=args.organization_id,
            project_id=args.project_id,
            job_id=job_id,
            poll_seconds=args.poll_seconds,
            max_seconds=args.max_wait_seconds,
            on_status=on_status,
        )
        terminal_ok = True
    except RuntimeError as e:
        # Terminal error status. Capture and report; do NOT raise to caller.
        print(f"\nJob terminated with error: {e}")
        terminal_ok = False
        try:
            final = client.get_import_job(
                organization_id=args.organization_id,
                project_id=args.project_id,
                job_id=job_id,
            )
        except AuraApiError:
            final = {}
    except TimeoutError as e:
        print(f"\nTimeout: {e}")
        return 4
    except AuraApiError as e:
        print(f"\nAPI error during polling: {e}")
        return 4

    elapsed = time.time() - t0
    print()
    print("=" * 60)
    print(f"FINAL STATUS : {final.get('status', '?')}")
    print(f"WALL TIME    : {elapsed:.1f}s ({elapsed/60:.1f} min)")
    print("=" * 60)
    print()
    print("Full final job payload:")
    print(json.dumps(final, indent=2, default=str))
    return 0 if terminal_ok else 5


if __name__ == "__main__":
    sys.exit(main())
