# Glue Spark Connect CLI

**The first CLI for AWS Glue Spark Connect interactive sessions — available in June 2026.**

> ⚡ **June 2026**: AWS Glue added `SPARK_CONNECT` as a new `SessionType`, enabling remote gRPC-based Spark execution. The `get_session_endpoint` API returns a Spark Connect URL + auth token. This is the first CLI wrapper for these brand-new capabilities.

## Description

`glue-spark-cli` is a production-ready command-line tool for managing **AWS Glue Spark Connect sessions** — the new interactive session type that enables remote Spark execution over gRPC.

**Why this exists:** In June 2026, AWS Glue added `SPARK_CONNECT` to the `SessionType` enum and shipped `get_session_endpoint` for retrieving Spark Connect gRPC endpoints. Community tooling doesn't exist yet. This CLI gives data engineers day-zero access to launch, list, and stop Spark Connect sessions without writing boto3 boilerplate.

### Features

- **start-session** — Launch a Spark Connect session with configurable worker type, count, timeout, and tags
- **list-sessions** — List all sessions with optional status post-filtering (PROVISIONING, READY, FAILED, etc.)
- **stop-session** — Stop a running session by ID
- **Zero-Traceback** — Every AWS error is parsed into clean, human-readable output
- **Network-safe** — 10s connect timeout, 30s read timeout, 3 retries on all API calls
- **Automatic ID generation** — Session UUID generated automatically if `--id` is not provided
- **Client-side status filtering** — `--status` works correctly even though the API doesn't support server-side filtering

## Installation

```bash
pip install "boto3>=1.43.25"
```

Python 3.10+ required.

## Authentication

Credentials are resolved via the standard AWS credential chain:

```bash
# Environment variables
export AWS_ACCESS_KEY_ID=AKIA...
export AWS_SECRET_ACCESS_KEY=*** AWS_DEFAULT_REGION=us-east-1

# Or: ~/.aws/credentials
# Or: IAM role on EC2/ECS/Lambda
```

The IAM role used must have permission for `glue:CreateSession`, `glue:ListSessions`, and `glue:StopSession`.

## Usage

### Start a Spark Connect session

```bash
python3 glue-spark-cli.py \
    --region us-east-1 \
    start-session \
    --name "Analytics Pipeline" \
    --worker-type G.1X \
    --number-of-workers 3 \
    --role-arn arn:aws:iam::123456789012:role/GlueSparkRole \
    --glue-version 5.0 \
    --timeout 120 \
    --idle-timeout 30 \
    --tags Environment=production CostCenter=analytics
```

Supported `--worker-type` values:

| Type | DPU | vCPU | Memory | Best for |
|------|-----|------|--------|----------|
| `Standard` | 1 | 4 | 16 GB | General purpose |
| `G.1X` | 1 | 4 | 16 GB | Data transforms, joins |
| `G.2X` | 2 | 8 | 32 GB | Heavy transforms |
| `G.4X` | 4 | 16 | 64 GB | Demanding workloads |
| `G.8X` | 8 | 32 | 128 GB | Maximum throughput |
| `G.025X` | 0.25 | 1 | 4 GB | Lightweight dev/testing |
| `Z.2X` | 2 M-DPU | 8 | 64 GB | Ray notebooks |

### List sessions

```bash
python3 glue-spark-cli.py \
    --region us-east-1 \
    list-sessions \
    --max-results 20
```

Filter by status (client-side):

```bash
python3 glue-spark-cli.py \
    --region us-east-1 \
    list-sessions \
    --status READY
```

Status values: `PROVISIONING`, `READY`, `FAILED`, `TIMEOUT`, `STOPPING`, `STOPPED`.

### Stop a session

```bash
python3 glue-spark-cli.py \
    --region us-east-1 \
    stop-session \
    --id 4d5a8b2c-1234-5678-9abc-def012345678
```

### Additional options

| Flag | Description |
|------|-------------|
| `--profile` | AWS credential profile name |
| `--endpoint-url` | Custom API endpoint (debugging) |
| `--debug` | Enable boto3 debug logging |

## Error Handling

This tool never prints Python tracebacks. All errors are caught and displayed in structured format:

```
[ОШИБКА] Session with specified ID not found.
Details: Session 4d5a8b2c-1234-5678-9abc-def012345678 does not exist.
```

Error types handled explicitly:

| AWS Error | User-friendly message |
|-----------|---------------------|
| `AccessDeniedException` | Permission denied — check Glue IAM policy |
| `EntityNotFoundException` | Session not found — check the ID |
| `InvalidInputException` | Invalid input — check parameters |
| `InternalServiceException` | AWS Glue internal error — retry later |
| `OperationTimeoutException` | Operation timed out — retry |
| `IllegalSessionStateException` | Session in invalid state for this action |
| `ConcurrentModificationException` | Concurrent modification — retry |
| `ConflictException` | Session with this ID already exists |
| `ThrottlingException` | Rate limit exceeded — retry with backoff |

Input validation is also handled before any API call:

- `--number-of-workers` must be ≥ 1 (validated before API call)
- `--tags` must be in `key=value` format
- `--worker-type` must be one of the valid enum values (validated by argparse)
- Non-integer values for `--number-of-workers` are rejected by argparse before the Python code runs

## Commercial Support

Need a custom Spark Connect pipeline, multi-region deployment, or integration with your data engineering workflow?

📧 **Email**: [alex.o.europe@gmail.com]  
🔧 **One-time setup**: $200–$500 per script  
📋 **Enterprise consulting**: Auto-scaling clusters, monitoring, CI/CD integration, custom Spark configurations

This tool is part of the **AWS New-API Gap Filler** collection — bridging the gap between AWS API releases and community tooling since June 2026.

---

*Made for the AWS Glue Spark Connect API (June 2026 release)*
