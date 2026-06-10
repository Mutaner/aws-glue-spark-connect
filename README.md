# Glue Spark Connect CLI

![Python](https://img.shields.io/badge/python-3.10+-blue.svg)
![License](https://img.shields.io/badge/license-MIT-green.svg)
![AWS](https://img.shields.io/badge/aws-glue-orange.svg)

**The first CLI for AWS Glue Spark Connect interactive sessions — June 2026.**

> ⚡ **June 2026**: AWS Glue added `SPARK_CONNECT` as a new `SessionType`,
> enabling remote gRPC-based Spark execution. `get_session_endpoint` returns
> a Spark Connect URL + auth token. This is the first CLI wrapper for these
> brand-new capabilities.

## Description

`glue-spark-cli` is a production-ready command-line tool for managing **AWS Glue
Spark Connect sessions** — the new interactive session type for remote Spark
execution over gRPC.

### Features

- **start-session** — Launch a Spark Connect session with configurable workers, timeouts, and tags
- **list-sessions** — List all sessions with optional status post-filtering
- **stop-session** — Stop a running session by ID
- **Zero-Traceback** — Every AWS error is parsed into clean output
- **Network-safe** — 10s connect timeout, 30s read timeout, 3 retries
- **Auto-UUID** — Session ID generated automatically if `--id` omitted

## Installation

```bash
pip install -r requirements.txt
```

Python 3.10+ required.

## Quick Start

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

### List sessions

```bash
python3 glue-spark-cli.py --region us-east-1 list-sessions --max-results 20
```

Filter by status:

```bash
python3 glue-spark-cli.py --region us-east-1 list-sessions --status READY
```

Status values: `PROVISIONING`, `READY`, `FAILED`, `TIMEOUT`, `STOPPING`, `STOPPED`.

### Stop a session

```bash
python3 glue-spark-cli.py --region us-east-1 stop-session --id 4d5a8b2c-1234-5678-9abc-def012345678
```

### Pipe through `jq`

```bash
python3 glue-spark-cli.py --region us-east-1 list-sessions \
    | jq '.Sessions[] | {id: .Id, status: .Status, type: .SessionType, workers: .NumberOfWorkers}'
```

### Example JSON output

```json
{
  "Sessions": [
    {
      "Id": "4d5a8b2c-1234-5678-9abc-def012345678",
      "Status": "READY",
      "SessionType": "SPARK_CONNECT",
      "WorkerType": "G.1X",
      "NumberOfWorkers": 3,
      "Role": "arn:aws:iam::123456789012:role/GlueSparkRole",
      "CreatedOn": "2026-06-10T12:00:00Z",
      "GlueVersion": "5.0"
    }
  ]
}
```

### Worker types

| Type | DPU | vCPU | Memory | Best for |
|------|-----|------|--------|----------|
| `Standard` | 1 | 4 | 16 GB | General purpose |
| `G.1X` | 1 | 4 | 16 GB | Data transforms, joins |
| `G.2X` | 2 | 8 | 32 GB | Heavy transforms |
| `G.4X` | 4 | 16 | 64 GB | Demanding workloads |
| `G.8X` | 8 | 32 | 128 GB | Maximum throughput |
| `G.025X` | 0.25 | 1 | 4 GB | Lightweight dev/testing |
| `Z.2X` | 2 M-DPU | 8 | 64 GB | Ray notebooks |

## Authentication

Standard AWS credential chain:

```bash
export AWS_ACCESS_KEY_ID=AKIA...
export AWS_SECRET_ACCESS_KEY=***
export AWS_DEFAULT_REGION=us-east-1
```

IAM role needs: `glue:CreateSession`, `glue:ListSessions`, `glue:StopSession`.

## CLI Options

| Flag | Description |
|------|-------------|
| `--region` | AWS region |
| `--profile` | AWS credential profile |
| `--endpoint-url` | Custom API endpoint (debugging) |
| `--debug` | Enable debug logging |

## Error Handling

No Python tracebacks. All errors are parsed to clean output:

```text
[ОШИБКА] Session with specified ID not found.
Details: Session 4d5a... does not exist.
```

| AWS Error | User-friendly message |
|-----------|----------------------|
| `AccessDeniedException` | Permission denied — check Glue IAM policy |
| `EntityNotFoundException` | Session not found — check ID |
| `InvalidInputException` | Invalid input — check parameters |
| `InternalServiceException` | AWS Glue internal error — retry |
| `OperationTimeoutException` | Operation timed out — retry |
| `IllegalSessionStateException` | Session in invalid state for action |
| `ConcurrentModificationException` | Concurrent modification — retry |
| `ThrottlingException` | Rate limit exceeded — retry with backoff |

Input validation is handled before API calls: `--number-of-workers` ≥ 1,
`--tags` in `key=value` format, `--worker-type` validated against enum.

## Contact & Support

Questions, feature requests, or enterprise integrations?

📧 **alex.o.europe@gmail.com**

---

*Part of the AWS New-API Gap Filler collection — June 2026.*