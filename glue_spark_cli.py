#!/usr/bin/env python3
"""
glue_spark_cli.py — AWS Glue Spark Connect CLI

CLI tool for AWS Glue Spark Connect interactive sessions (June 2026 API).

Commands:
  create-session  — Create (start) a new SPARK_CONNECT session
  list-sessions  — List all sessions (with optional status filter)
  stop-session   — Stop a session by ID
  get-endpoint   — Get Spark Connect endpoint URL + auth token for a session

Dependencies: boto3>=1.43.25 (pip install -r requirements.txt)

Examples:
  python3 glue_spark_cli.py --region us-east-1 create-session \\
      --name "Analytics" --worker-type G.1X \\
      --number-of-workers 3 --role-arn arn:aws:iam::123456789012:role/GlueSessionRole

  python3 glue_spark_cli.py --region us-east-1 list-sessions --max-results 20

  python3 glue_spark_cli.py --region us-east-1 stop-session \\
      --id 4d5a8b2c-1234-5678-9abc-def012345678

  python3 glue_spark_cli.py --region us-east-1 get-endpoint \\
      --id 4d5a8b2c-1234-5678-9abc-def012345678
"""

import argparse
import json
import logging
import sys
import textwrap
import traceback
import uuid
from typing import Any, Dict, List, Optional

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import (
    BotoCoreError,
    ClientError,
    NoCredentialsError,
    NoRegionError,
    ParamValidationError,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SERVICE_NAME = "glue"
EXIT_SUCCESS = 0
EXIT_FAILURE = 1

VALID_WORKER_TYPES = ["Standard", "G.1X", "G.2X", "G.025X", "G.4X", "G.8X", "Z.2X"]
VALID_SESSION_STATUSES = ["PROVISIONING", "READY", "FAILED", "TIMEOUT", "STOPPING", "STOPPED"]

BOTO_TIMEOUT_CONFIG = BotoConfig(
    connect_timeout=10,
    read_timeout=30,
    retries={"max_attempts": 3},
)

# ---------------------------------------------------------------------------
# Error formatting
# ---------------------------------------------------------------------------


def _friendly_error(msg: str, detail: str = "") -> str:
    parts = [f"[ERROR] {msg}"]
    if detail:
        parts.append(detail)
    return "\n".join(parts)


def _handle_boto3_error(e: Exception) -> str:
    if isinstance(e, NoCredentialsError):
        return _friendly_error(
            "AWS credentials not found.",
            "Check ~/.aws/credentials or AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY.",
        )
    if isinstance(e, NoRegionError):
        return _friendly_error(
            "AWS region not specified.",
            "Pass --region or set AWS_DEFAULT_REGION.",
        )
    if isinstance(e, ParamValidationError):
        return _friendly_error("Parameter validation failed.", str(e))
    if isinstance(e, ClientError):
        code = e.response["Error"]["Code"]
        message = e.response["Error"]["Message"]
        status = e.response["ResponseMetadata"]["HTTPStatusCode"]
        hints = {
            "AccessDeniedException": "Permission denied. Check Glue IAM policy.",
            "EntityNotFoundException": "Session not found. Check the ID.",
            "InvalidInputException": "Invalid input. Check parameter format.",
            "InternalServiceException": "AWS Glue internal error. Retry later.",
            "OperationTimeoutException": "Operation timed out. Retry.",
            "IllegalSessionStateException": "Session in invalid state for this action.",
            "ConflictException": "A session with this ID already exists.",
            "ConcurrentModificationException": "Concurrent modification. Retry.",
            "ThrottlingException": "Rate limit exceeded. Wait and retry.",
        }
        hint = hints.get(code, f"AWS error code: {code}")
        return _friendly_error(f"AWS Glue returned an error (HTTP {status})", f"{hint}\nDetails: {message}")
    if isinstance(e, BotoCoreError):
        return _friendly_error("Internal boto3 error.", str(e))
    return _friendly_error(
        f"Unknown error ({type(e).__name__})",
        str(e) if str(e) else traceback.format_exc(),
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _auto_id() -> str:
    """Generate a UUID for Session ID if the user didn't provide one."""
    return str(uuid.uuid4())


def _is_empty_listing(result: Any, action: str) -> bool:
    """Check if API returned an empty list for listing operations."""
    if action not in ("list-sessions",):
        return False
    if not isinstance(result, dict):
        return False
    for key in ("Sessions",):
        if key in result and isinstance(result[key], list) and len(result[key]) == 0:
            return True
    return False


def _parse_tags(tags: List[str]) -> Dict[str, str]:
    """Parse a list of 'key=value' strings into a dictionary.

    Raises ValueError with a clear message on invalid format.
    """
    result: Dict[str, str] = {}
    for item in tags:
        if "=" not in item:
            raise ValueError(
                _friendly_error(
                    f"Invalid tag format: '{item}'",
                    "Each tag must be in key=value format (e.g. Environment=production).",
                )
            )
        key, value = item.split("=", 1)
        if not key:
            raise ValueError(
                _friendly_error(
                    f"Empty key in tag: '{item}'",
                    "Tag key cannot be empty. Use: key=value.",
                )
            )
        result[key] = value
    return result


def _setup_debug_logging() -> None:
    """Enable boto3 debug logging via modern API.

    WARNING: Debug mode may print AWS credentials to stdout/stderr.
    Only use in isolated environments.
    """
    print("[WARNING] Debug mode enabled. AWS credentials may appear in logs.", file=sys.stderr)
    logging.basicConfig(level=logging.DEBUG)
    for logger_name in ("boto3", "botocore", "s3transfer", "urllib3"):
        logging.getLogger(logger_name).setLevel(logging.DEBUG)
        logging.getLogger(logger_name).propagate = True


# ---------------------------------------------------------------------------
# Command functions
# ---------------------------------------------------------------------------


def cmd_start_session(client: boto3.client, args: argparse.Namespace) -> Dict[str, Any]:
    """Start a Spark Connect session in Glue."""
    session_id = args.id if args.id else _auto_id()

    kwargs: Dict[str, Any] = {
        "Id": session_id,
        "Role": args.role_arn,
        "SessionType": "SPARK_CONNECT",
        "Command": {
            "Name": "spark",
        },
    }

    if args.name:
        kwargs["Description"] = args.name

    if args.worker_type:
        if args.worker_type not in VALID_WORKER_TYPES:
            raise ValueError(
                _friendly_error(
                    f"Invalid worker type: {args.worker_type}",
                    f"Valid values: {', '.join(VALID_WORKER_TYPES)}",
                )
            )
        kwargs["WorkerType"] = args.worker_type

    if args.number_of_workers is not None:
        if args.number_of_workers < 1:
            raise ValueError(
                _friendly_error(
                    "Invalid number of workers.",
                    "--number-of-workers must be >= 1. Got: {}.".format(args.number_of_workers),
                )
            )
        kwargs["NumberOfWorkers"] = args.number_of_workers

    if args.glue_version:
        kwargs["GlueVersion"] = args.glue_version

    if args.timeout is not None:
        kwargs["Timeout"] = args.timeout

    if args.idle_timeout is not None:
        kwargs["IdleTimeout"] = args.idle_timeout

    if args.tags:
        tags_dict = _parse_tags(args.tags)
        kwargs["Tags"] = tags_dict

    result = client.create_session(**kwargs)

    if isinstance(result.get("Session"), dict):
        result["Session"]["SessionId"] = session_id

    return result


def cmd_list_sessions(client: boto3.client, args: argparse.Namespace) -> Dict[str, Any]:
    """List Glue sessions with optional client-side status filtering."""
    kwargs: Dict[str, Any] = {}

    if args.max_results is not None:
        kwargs["MaxResults"] = args.max_results

    if args.next_token:
        kwargs["NextToken"] = args.next_token

    result = client.list_sessions(**kwargs)

    if args.status and isinstance(result.get("Sessions"), list):
        print(
            "[INFO] Status filtering is applied client-side. "
            "--max-results limits the page fetched from AWS, not the filtered results. "
            "Use --max-results with a larger value or omit it when filtering by status.",
            file=sys.stderr,
        )
        filtered = [
            s for s in result["Sessions"]
            if isinstance(s, dict) and s.get("Status") == args.status
        ]
        result["Sessions"] = filtered
        result["_filtered_by_status"] = args.status
        result["_total_matching"] = len(filtered)

    return result


def cmd_stop_session(client: boto3.client, args: argparse.Namespace) -> Dict[str, Any]:
    """Stop a Glue session by ID."""
    kwargs: Dict[str, Any] = {
        "Id": args.id,
    }
    return client.stop_session(**kwargs)


def cmd_get_endpoint(client: boto3.client, args: argparse.Namespace) -> Dict[str, Any]:
    """Get Spark Connect endpoint URL and auth token for a session.

    This is the killer feature: it returns the gRPC endpoint and
    authentication token needed to connect to the session via Spark Connect.
    """
    return client.get_session_endpoint(SessionId=args.id)


# ---------------------------------------------------------------------------
# Argument parser
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="glue-spark-cli",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=textwrap.dedent("""\
            CLI tool for AWS Glue Spark Connect sessions (June 2026).

            Requires Python >= 3.10 and boto3 >= 1.43.25.
            Install: pip install -r requirements.txt

            AWS credentials: ~/.aws/credentials or AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY.

            Commands:
              create-session  Launch a SPARK_CONNECT session (maps to boto3 create_session)
              list-sessions  List sessions (optional status filter)
              stop-session   Stop a session by ID
              get-endpoint   Get Spark Connect gRPC endpoint + token
        """),
        epilog=textwrap.dedent("""\
            Examples:
              # Create a session (maps to boto3 create_session)
              python3 glue_spark_cli.py --region us-east-1 create-session \\
                  --name "Analytics" --worker-type G.1X \\
                  --number-of-workers 3 --role-arn arn:aws:iam::1:role/GlueSparkRole

              # List sessions
              python3 glue_spark_cli.py --region us-east-1 list-sessions --max-results 20

              # Stop a session
              python3 glue_spark_cli.py --region us-east-1 stop-session \\
                  --id 4d5a8b2c-1234-5678-9abc-def012345678

              # Get Spark Connect endpoint
              python3 glue_spark_cli.py --region us-east-1 get-endpoint \\
                  --id 4d5a8b2c-1234-5678-9abc-def012345678
        """),
    )

    parser.add_argument("--region", default=None, help="AWS region (e.g. us-east-1)")
    parser.add_argument("--profile", default=None, help="Profile from ~/.aws/credentials")
    parser.add_argument("--endpoint-url", default=None, help="Custom endpoint URL (debugging)")
    parser.add_argument("--debug", action="store_true", help="Enable boto3 debug logging")

    subparsers = parser.add_subparsers(dest="action", required=True, help="Available actions")

    # --- create-session (maps to boto3 create_session) ---
    start_parser = subparsers.add_parser("create-session", help="Create (start) a new Spark Connect session")
    start_parser.add_argument("--id", default=None, help="Session ID (UUID). Auto-generated if omitted.")
    start_parser.add_argument("--name", default=None, help="Session description / name")
    start_parser.add_argument(
        "--worker-type", default=None,
        choices=VALID_WORKER_TYPES,
        help=f"Worker type. Default: Standard. Values: {', '.join(VALID_WORKER_TYPES)}",
    )
    start_parser.add_argument(
        "--number-of-workers", type=int, default=None,
        help="Number of workers of the specified type",
    )
    start_parser.add_argument(
        "--role-arn", required=True,
        help="IAM role ARN for the session (arn:aws:iam::...:role/...)",
    )
    start_parser.add_argument(
        "--glue-version", default=None,
        help="Glue version (e.g. 4.0, 5.0). Must be > 2.0.",
    )
    start_parser.add_argument(
        "--timeout", type=int, default=None,
        help="Session timeout in minutes (default: 2880 = 48h)",
    )
    start_parser.add_argument(
        "--idle-timeout", type=int, default=None,
        help="Idle timeout in minutes",
    )
    start_parser.add_argument(
        "--tags", nargs="+", default=None,
        help="Tags: key=value key=value ... (required if --tags is used)",
    )

    # --- list-sessions ---
    list_parser = subparsers.add_parser("list-sessions", help="List Glue sessions")
    list_parser.add_argument(
        "--max-results", type=int, default=None,
        help="Maximum number of results",
    )
    list_parser.add_argument(
        "--next-token", default=None,
        help="Pagination token (NextToken from previous response)",
    )
    list_parser.add_argument(
        "--status", default=None,
        choices=VALID_SESSION_STATUSES,
        help="Filter by session status (client-side filtering)",
    )

    # --- stop-session ---
    stop_parser = subparsers.add_parser("stop-session", help="Stop a session by ID")
    stop_parser.add_argument("--id", required=True, help="Session ID to stop")

    # --- get-endpoint ---
    endpoint_parser = subparsers.add_parser("get-endpoint", help="Get Spark Connect endpoint + token")
    endpoint_parser.add_argument("--id", required=True, help="Session ID")

    return parser


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    session_kwargs: Dict[str, Any] = {}
    if args.profile:
        session_kwargs["profile_name"] = args.profile
    if args.region:
        session_kwargs["region_name"] = args.region

    try:
        session = boto3.Session(**session_kwargs)
    except Exception as e:
        print(_friendly_error("Failed to create AWS session.", str(e)), file=sys.stderr)
        return EXIT_FAILURE

    client_kwargs: Dict[str, Any] = {
        "service_name": SERVICE_NAME,
        "config": BOTO_TIMEOUT_CONFIG,
    }
    if args.endpoint_url:
        client_kwargs["endpoint_url"] = args.endpoint_url
    if args.debug:
        _setup_debug_logging()

    try:
        client = session.client(**client_kwargs)
    except Exception as e:
        print(_friendly_error("Failed to create boto3 client.", str(e)), file=sys.stderr)
        return EXIT_FAILURE

    action_map: Dict[str, Any] = {
        "create-session": cmd_start_session,
        "list-sessions": cmd_list_sessions,
        "stop-session": cmd_stop_session,
        "get-endpoint": cmd_get_endpoint,
    }

    handler = action_map.get(args.action)
    if handler is None:
        print(_friendly_error(f"Unknown action: {args.action}"), file=sys.stderr)
        return EXIT_FAILURE

    try:
        result = handler(client, args)
    except ValueError as e:
        print(str(e), file=sys.stderr)
        return EXIT_FAILURE
    except Exception as e:
        print(_handle_boto3_error(e), file=sys.stderr)
        return EXIT_FAILURE

    preview = json.dumps(result, indent=2, ensure_ascii=False, default=str)
    if _is_empty_listing(result, args.action):
        print("[INFO] No resources found. API returned an empty list.", file=sys.stderr)
    print(preview)
    return EXIT_SUCCESS


if __name__ == "__main__":
    sys.exit(main())