"""
recreate_instance.py

Destructively recreate an Aura instance for benchmarking.

Why this exists: a fresh Aura instance is the cleanest possible benchmark
substrate (no warm page cache, no leftover schema, no WAL replay history).
Resetting an existing instance with reset_to_blank_neo4j_db.sh keeps the
JVM and OS state warm; for repeatable benchmark numbers, fresh is better.
For everyday operations, reset is fine and cheaper.

Workflow:
  1. Authenticate to the Aura API.
  2. Fetch the target instance's full config so the new one is byte-identical.
  3. Confirm with the operator (unless --yes).
  4. DELETE the existing instance and wait for it to disappear.
  5. POST a new instance with the captured config (or --rename).
  6. Poll until status == "running".
  7. Write the new connection_url + one-time password into a credentials
     file the rest of this repo's scripts can consume.
  8. Print the custom-endpoint rebind reminder.

Custom endpoints are not rebound automatically: the public Aura API does
not expose them to ordinary OAuth keys (403 forbidden in our testing).
Rebinding is one click in the Aura console once the new instance is up.

Safety guardrails:
  - --instance-id is required; no fuzzy name matching.
  - Confirmation prompt requires typing the instance name unless --yes.
  - --dry-run shows the full plan without making any mutating call.
  - The new credentials file is written with mode 0600.

Usage:
  python recreate_instance.py \\
      --api-credentials /path/to/Neo4j-credentials-Agent_Key.txt \\
      --instance-id 27ad415a \\
      --output-credentials ~/Neo4j-fresh-credentials.txt

  # Unattended (CI / scripts):
  python recreate_instance.py \\
      --api-credentials .../api-creds.txt \\
      --instance-id 27ad415a \\
      --output-credentials ~/Neo4j-fresh-credentials.txt \\
      --yes

  # Plan only:
  python recreate_instance.py \\
      --api-credentials .../api-creds.txt \\
      --instance-id 27ad415a \\
      --output-credentials ~/Neo4j-fresh-credentials.txt \\
      --dry-run
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

from aura_lifecycle import (
    AuraApiCredentials,
    AuraApiError,
    AuraClient,
    InstanceSpec,
    write_credentials_file,
)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--api-credentials", required=True, type=Path,
                   help="Path to a file containing CLIENT_ID and CLIENT_SECRET "
                        "for the Aura public API.")
    p.add_argument("--instance-id", required=True,
                   help="Aura instance ID to delete and recreate (e.g. 27ad415a). "
                        "Required; no fuzzy name matching to avoid accidents.")
    p.add_argument("--output-credentials", required=True, type=Path,
                   help="Where to write the new NEO4J_URI / NEO4J_USERNAME / "
                        "NEO4J_PASSWORD file. Existing file at this path will "
                        "be overwritten with mode 0600.")
    p.add_argument("--rename", default=None,
                   help="Optional new name for the recreated instance. "
                        "Defaults to the original name.")
    p.add_argument("--version", default="5",
                   help="Neo4j major version for the new instance. Default 5.")
    p.add_argument("--yes", action="store_true",
                   help="Skip the interactive confirmation prompt. Required "
                        "for unattended use.")
    p.add_argument("--dry-run", action="store_true",
                   help="Print the plan and exit without mutating anything.")
    p.add_argument("--custom-endpoint",
                   help="If provided, the URL is printed in the final summary "
                        "as a reminder for manual rebinding in the Aura "
                        "console. Has no programmatic effect.")
    p.add_argument("--max-create-seconds", type=float, default=900.0,
                   help="Ceiling for waiting on the new instance to reach "
                        "running. Default 900s (15 min).")
    p.add_argument("--max-delete-seconds", type=float, default=600.0,
                   help="Ceiling for waiting on the old instance to be "
                        "deleted. Default 600s (10 min).")
    return p.parse_args()


def confirm_destructive(instance: dict, args: argparse.Namespace) -> bool:
    print()
    print("=" * 60)
    print("DESTRUCTIVE OPERATION REVIEW")
    print("=" * 60)
    print(f"  Instance ID:      {instance['id']}")
    print(f"  Instance name:    {instance['name']}")
    print(f"  Tenant:           {instance['tenant_id']}")
    print(f"  Cloud / region:   {instance['cloud_provider']} / {instance['region']}")
    print(f"  Type / memory:    {instance['type']} / {instance['memory']}")
    print(f"  Current status:   {instance['status']}")
    print(f"  Connection URL:   {instance.get('connection_url', '(unknown)')}")
    print()
    print("This will DELETE the instance above (data is unrecoverable),")
    print("then create a NEW instance with the same config. Total time: 5-15 minutes.")
    print()

    if args.dry_run:
        print("[--dry-run] No mutating calls will be made. Plan above shown for review.")
        return False

    if args.yes:
        print("[--yes] Confirmation skipped. Proceeding.")
        return True

    expected = instance["name"]
    typed = input(f"To proceed, type the instance name exactly ({expected!r}): ").strip()
    if typed != expected:
        print(f"Confirmation mismatch (got {typed!r}). Aborting.")
        return False
    return True


def status_logger(prefix: str):
    def log(status: str) -> None:
        print(f"  [{prefix}] status -> {status}  (t={time.strftime('%H:%M:%S')})")
    return log


def main() -> int:
    args = parse_args()

    if args.output_credentials.exists() and not args.dry_run:
        # Refuse to overwrite without warning. We do not want to silently
        # destroy a known-good credentials file the user might need to
        # roll back to. A single backup copy is enough; we don't keep history.
        backup = args.output_credentials.with_suffix(args.output_credentials.suffix + ".bak")
        print(f"Existing credentials file at {args.output_credentials}.")
        print(f"Backing up to {backup} before overwrite.")
        if backup.exists():
            backup.unlink()
        args.output_credentials.rename(backup)

    creds = AuraApiCredentials.from_file(args.api_credentials)
    client = AuraClient(creds)

    print(f"Fetching current state of instance {args.instance_id}...")
    try:
        current = client.get_instance(args.instance_id)
    except AuraApiError as e:
        print(f"Could not fetch instance {args.instance_id}: {e}")
        return 2
    if not current:
        print(f"Instance {args.instance_id} not found.")
        return 2

    spec = InstanceSpec.from_existing(current, name=args.rename, version=args.version)
    print()
    print("New instance spec (captured from current):")
    print(json.dumps(spec.to_create_body(), indent=2))

    if not confirm_destructive(current, args):
        # Dry-run is a successful "show me the plan"; explicit user decline
        # or any other non-confirmation is a failure (exit 1).
        return 0 if args.dry_run else 1

    # ---- DELETE ----
    print()
    print(f"Deleting instance {args.instance_id}...")
    try:
        client.delete_instance(args.instance_id)
    except AuraApiError as e:
        print(f"Delete request failed: {e}")
        return 3
    try:
        client.wait_until_deleted(
            args.instance_id,
            max_seconds=args.max_delete_seconds,
            on_status=status_logger("delete"),
        )
    except (TimeoutError, AuraApiError) as e:
        print(f"Delete did not complete cleanly: {e}")
        return 3
    print(f"Instance {args.instance_id} deleted.")

    # ---- CREATE ----
    print()
    print(f"Creating new instance with name {spec.name!r}...")
    try:
        created = client.create_instance(spec)
    except AuraApiError as e:
        print(f"Create request failed: {e}")
        return 4

    new_id = created["id"]
    # The password is shown ONLY in the create response. If we lose it
    # before persisting, the only recovery is to rotate via the console.
    one_time_password = created.get("password")
    new_uri = created.get("connection_url")
    new_user = created.get("username", "neo4j")
    if not (one_time_password and new_uri):
        print("Create response did not include password or connection_url:")
        print(json.dumps(created, indent=2))
        return 4

    print(f"Created instance {new_id} (status: {created.get('status', 'unknown')}).")
    print("Waiting for instance to reach status=running...")
    try:
        client.wait_until_running(
            new_id,
            max_seconds=args.max_create_seconds,
            on_status=status_logger("create"),
        )
    except (TimeoutError, RuntimeError, AuraApiError) as e:
        # The instance was created and we have its credentials; even if
        # waiting timed out, we should still write the creds file so the
        # operator can recover.
        print(f"Wait failed but credentials are available: {e}")
        write_credentials_file(
            args.output_credentials, uri=new_uri, username=new_user,
            password=one_time_password,
        )
        print(f"Credentials written to {args.output_credentials} despite the wait error.")
        return 5

    write_credentials_file(
        args.output_credentials, uri=new_uri, username=new_user,
        password=one_time_password,
    )

    # ---- SUMMARY ----
    print()
    print("=" * 60)
    print("RECREATE COMPLETE")
    print("=" * 60)
    print(f"  Old instance:    {args.instance_id} (deleted)")
    print(f"  New instance:    {new_id}")
    print(f"  Connection URL:  {new_uri}")
    print(f"  Credentials at:  {args.output_credentials}  (mode 0600)")
    if args.custom_endpoint:
        print()
        print("  REMINDER: rebind your custom endpoint to the new instance.")
        print(f"    Custom endpoint: {args.custom_endpoint}")
        print(f"    Rebind to:       {new_id}")
        print( "    Where:           Aura Console -> Custom endpoints -> Configure")
        print( "    The Aura public API does not expose this operation to "
               "ordinary OAuth keys, so this step is manual.")
    print()
    print("Next: run the loader against the new credentials file.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
