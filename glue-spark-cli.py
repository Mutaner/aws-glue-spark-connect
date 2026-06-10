#!/usr/bin/env python3
"""
glue-spark-cli.py — CLI для AWS Glue Spark Connect (июнь 2026)

Управление интерактивными Spark Connect сессиями в AWS Glue:
  start-session  — Запустить новую Spark Connect сессию
  list-sessions  — Список всех сессий (с пагинацией и фильтрацией)
  stop-session   — Остановить сессию по ID

Зависимости: boto3>=1.43.25 (pip install boto3>=1.43.25)

Примеры:
  python3 glue-spark-cli.py --region us-east-1 start-session \\
      --name "Моя Spark сессия" --worker-type G.1X \\
      --number-of-workers 3 --role-arn arn:aws:iam::123456789012:role/GlueSessionRole

  python3 glue-spark-cli.py --region us-east-1 list-sessions --max-results 20

  python3 glue-spark-cli.py --region us-east-1 stop-session --id 4d5a8b2c-1234-5678-9abc-def012345678
"""

import argparse
import json
import sys
import textwrap
import traceback
import uuid

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import (
    BotoCoreError,
    ClientError,
    NoCredentialsError,
    NoRegionError,
    ParamValidationError,
)

# Типы для type hints (Python 3.10+)
from typing import Any, Dict, List, Optional

# ---------------------------------------------------------------------------
# Константы
# ---------------------------------------------------------------------------
SERVICE_NAME = "glue"
EXIT_SUCCESS = 0
EXIT_FAILURE = 1

VALID_WORKER_TYPES = ["Standard", "G.1X", "G.2X", "G.025X", "G.4X", "G.8X", "Z.2X"]
VALID_SESSION_STATUSES = ["PROVISIONING", "READY", "FAILED", "TIMEOUT", "STOPPING", "STOPPED"]

# Таймауты для boto3-клиента (сек)
BOTO_TIMEOUT_CONFIG = BotoConfig(
    connect_timeout=10,
    read_timeout=30,
    retries={"max_attempts": 3},
)

# ---------------------------------------------------------------------------
# Форматирование ошибок
# ---------------------------------------------------------------------------

def _friendly_error(msg: str, detail: str = "") -> str:
    parts = [f"[ОШИБКА] {msg}"]
    if detail:
        parts.append(detail)
    return "\n".join(parts)


def _handle_boto3_error(e: Exception) -> str:
    if isinstance(e, NoCredentialsError):
        return _friendly_error(
            "Учётные данные AWS не найдены.",
            "Проверьте ~/.aws/credentials или переменные AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY.",
        )
    if isinstance(e, NoRegionError):
        return _friendly_error(
            "Регион AWS не указан.",
            "Передайте --region или установите AWS_DEFAULT_REGION.",
        )
    if isinstance(e, ParamValidationError):
        return _friendly_error("Ошибка валидации параметров.", str(e))
    if isinstance(e, ClientError):
        code = e.response["Error"]["Code"]
        message = e.response["Error"]["Message"]
        status = e.response["ResponseMetadata"]["HTTPStatusCode"]
        hints = {
            "AccessDeniedException": "Доступ запрещён. Проверьте IAM-права для Glue.",
            "EntityNotFoundException": "Сессия с указанным ID не найдена.",
            "InvalidInputException": "Некорректные входные данные. Проверьте параметры запроса.",
            "InternalServiceException": "Внутренняя ошибка AWS Glue. Повторите позже.",
            "OperationTimeoutException": "Тайм-аут операции. Повторите запрос.",
            "IllegalSessionStateException": "Сессия в недопустимом состоянии для этого действия.",
            "ConflictException": "Конфликт: сессия с таким ID уже существует.",
            "ConcurrentModificationException": "Конкурирующее изменение. Повторите запрос.",
            "ThrottlingException": "Превышен лимит запросов. Подождите и повторите.",
        }
        hint = hints.get(code, f"Код ошибки AWS: {code}")
        return _friendly_error(f"AWS Glue вернул ошибку (HTTP {status})", f"{hint}\nДетали: {message}")
    if isinstance(e, BotoCoreError):
        return _friendly_error("Внутренняя ошибка boto3.", str(e))
    return _friendly_error(f"Неизвестная ошибка ({type(e).__name__})", str(e) if str(e) else traceback.format_exc())


# ---------------------------------------------------------------------------
# Функции-действия
# ---------------------------------------------------------------------------

def _auto_id() -> str:
    """Генерирует UUID для Session ID, если пользователь не указал свой."""
    return str(uuid.uuid4())


def _is_empty_listing(result: Any, action: str) -> bool:
    """Проверяет, вернул ли API пустой список для операций листинга.

    Проверяет наличие ключей: Sessions, KnowledgeBaseSummaries, Flows, Ids.
    Возвращает True, если ключ существует и значение — пустой список.
    """
    if action not in ("list-sessions", "list-kbs", "list-flows"):
        return False
    if not isinstance(result, dict):
        return False
    for key in ("Sessions", "KnowledgeBaseSummaries", "Flows", "Ids"):
        if key in result and isinstance(result[key], list) and len(result[key]) == 0:
            return True
    return False


def action_start_session(client: boto3.client, args: argparse.Namespace) -> Dict[str, Any]:
    """Запустить Spark Connect сессию в Glue."""
    session_id = args.id if args.id else _auto_id()

    kwargs: Dict[str, Any] = {
        "Id": session_id,
        "Role": args.role_arn,
        "SessionType": "SPARK_CONNECT",
        "Command": {
            "Name": "glueetl",
            "PythonVersion": "3",
        },
    }

    if args.name:
        kwargs["Description"] = args.name

    if args.worker_type:
        if args.worker_type not in VALID_WORKER_TYPES:
            raise ValueError(
                _friendly_error(
                    f"Недопустимый тип worker: {args.worker_type}",
                    f"Допустимые значения: {', '.join(VALID_WORKER_TYPES)}",
                )
            )
        kwargs["WorkerType"] = args.worker_type

    if args.number_of_workers is not None:
        if args.number_of_workers < 1:
            raise ValueError(
                _friendly_error(
                    "Некорректное количество worker'ов.",
                    "--number-of-workers должно быть >= 1. Получено: {}.".format(args.number_of_workers),
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

    # Добавляем session_id в вывод для удобства
    if isinstance(result.get("Session"), dict):
        result["Session"]["SessionId"] = session_id

    return result


def _parse_tags(tags: List[str]) -> Dict[str, str]:
    """Парсит список 'key=value' в словарь.

    Выбрасывает ValueError с понятным сообщением при неверном формате.
    """
    result: Dict[str, str] = {}
    for item in tags:
        if "=" not in item:
            raise ValueError(
                _friendly_error(
                    f"Неверный формат тега: '{item}'",
                    "Каждый тег должен быть в формате key=value (например: Environment=production).",
                )
            )
        key, value = item.split("=", 1)
        if not key:
            raise ValueError(
                _friendly_error(
                    f"Пустой ключ тега: '{item}'",
                    "Ключ тега не может быть пустым. Используйте: key=value.",
                )
            )
        result[key] = value
    return result


def action_list_sessions(client: boto3.client, args: argparse.Namespace) -> Dict[str, Any]:
    """Список Glue сессий с опциональным пост-фильтром по статусу."""
    kwargs: Dict[str, Any] = {}

    if args.max_results is not None:
        kwargs["MaxResults"] = args.max_results

    if args.next_token:
        kwargs["NextToken"] = args.next_token

    result = client.list_sessions(**kwargs)

    # Пост-фильтрация по статусу, если указан --status
    # API list_sessions НЕ ПОДДЕРЖИВАЕТ фильтрацию по статусу на сервере
    if args.status and isinstance(result.get("Sessions"), list):
        filtered = [
            s for s in result["Sessions"]
            if isinstance(s, dict) and s.get("Status") == args.status
        ]
        result["Sessions"] = filtered
        result["_filtered_by_status"] = args.status
        result["_total_matching"] = len(filtered)

    return result


def action_stop_session(client: boto3.client, args: argparse.Namespace) -> dict:
    """Остановить Glue сессию по ID."""
    kwargs: dict = {
        "Id": args.id,
    }
    return client.stop_session(**kwargs)


# ---------------------------------------------------------------------------
# Парсер аргументов
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="glue-spark-cli",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=textwrap.dedent("""\
            CLI для AWS Glue Spark Connect сессий (июнь 2026).

            Управляйте интерактивными Spark Connect сессиями: запуск,
            просмотр и остановка через boto3.

            Зависимости: pip install boto3>=1.43.25
            Учётные данные: ~/.aws/credentials или AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY
        """),
        epilog=textwrap.dedent("""\
            Примеры:
              # Запустить Spark Connect сессию
              python3 glue-spark-cli.py --region us-east-1 start-session \\
                  --name "Analytics Session" --worker-type G.1X \\\\
                  --number-of-workers 3 --role-arn arn:aws:iam::123456789012:role/GlueSparkRole

              # Список сессий (первые 20)
              python3 glue-spark-cli.py --region us-east-1 list-sessions --max-results 20

              # Остановить сессию
              python3 glue-spark-cli.py --region us-east-1 stop-session --id 4d5a8b2c-1234-5678-9abc-def012345678
        """),
    )

    # Глобальные параметры
    parser.add_argument("--region", default=None, help="AWS-регион (например, us-east-1)")
    parser.add_argument("--profile", default=None, help="Профиль из ~/.aws/credentials")
    parser.add_argument("--endpoint-url", default=None, help="Кастомный endpoint URL")
    parser.add_argument("--debug", action="store_true", help="Включить debug-логирование boto3")

    # Сабпарсеры
    subparsers = parser.add_subparsers(dest="action", required=True, help="Доступные действия")

    # --- start-session ---
    start_parser = subparsers.add_parser("start-session", help="Запустить Spark Connect сессию")
    start_parser.add_argument("--id", default=None, help="ID сессии (UUID). Если не указан — генерируется автоматически")
    start_parser.add_argument("--name", default=None, help="Описание/название сессии")
    start_parser.add_argument(
        "--worker-type", default=None,
        choices=VALID_WORKER_TYPES,
        help=f"Тип worker'а. По умолчанию: Standard. Значения: {', '.join(VALID_WORKER_TYPES)}",
    )
    start_parser.add_argument(
        "--number-of-workers", type=int, default=None,
        help="Количество worker'ов указанного типа",
    )
    start_parser.add_argument(
        "--role-arn", required=True,
        help="ARN IAM-роли для сессии (arn:aws:iam::...:role/...)",
    )
    start_parser.add_argument(
        "--glue-version", default=None,
        help="Версия Glue (например, 4.0, 5.0). Должна быть > 2.0",
    )
    start_parser.add_argument(
        "--timeout", type=int, default=None,
        help="Тайм-аут сессии в минутах (по умолчанию: 2880 = 48ч)",
    )
    start_parser.add_argument(
        "--idle-timeout", type=int, default=None,
        help="Тайм-аут простоя в минутах",
    )
    start_parser.add_argument(
        "--tags", nargs="*", default=None,
        help="Теги: key=value key=value ...",
    )

    # --- list-sessions ---
    list_parser = subparsers.add_parser("list-sessions", help="Список Glue сессий")
    list_parser.add_argument(
        "--max-results", type=int, default=None,
        help="Максимальное количество результатов",
    )
    list_parser.add_argument(
        "--next-token", default=None,
        help="Токен пагинации (NextToken из предыдущего ответа)",
    )
    list_parser.add_argument(
        "--status", default=None,
        choices=VALID_SESSION_STATUSES,
        help="Фильтр по статусу сессии",
    )

    # --- stop-session ---
    stop_parser = subparsers.add_parser("stop-session", help="Остановить сессию по ID")
    stop_parser.add_argument("--id", required=True, help="ID сессии для остановки")
    return parser


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def _setup_debug_logging() -> None:
    """Включает debug-логирование boto3 через современный API.

    ВНИМАНИЕ: debug-режим может вывести AWS credentials в stdout.
    Используйте только в изолированных средах.
    """
    logging.basicConfig(level=logging.DEBUG)
    for logger_name in ("boto3", "botocore", "s3transfer", "urllib3"):
        logging.getLogger(logger_name).setLevel(logging.DEBUG)
        logging.getLogger(logger_name).propagate = True


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    # Сессия boto3
    session_kwargs = {}
    if args.profile:
        session_kwargs["profile_name"] = args.profile
    if args.region:
        session_kwargs["region_name"] = args.region

    try:
        session = boto3.Session(**session_kwargs)
    except Exception as e:
        print(_friendly_error("Не удалось создать AWS-сессию.", str(e)), file=sys.stderr)
        return EXIT_FAILURE

    # Клиент glue
    client_kwargs: Dict[str, Any] = {
        "service_name": SERVICE_NAME,
        "config": BOTO_TIMEOUT_CONFIG,
    }
    if args.endpoint_url:
        client_kwargs["endpoint_url"] = args.endpoint_url
    if args.debug:
        import logging
        _setup_debug_logging()

    try:
        client = session.client(**client_kwargs)
    except Exception as e:
        print(_friendly_error("Не удалось создать boto3-клиент.", str(e)), file=sys.stderr)
        return EXIT_FAILURE

    # ---- Диспетчер действий ----
    action_map = {
        "start-session": action_start_session,
        "list-sessions": action_list_sessions,
        "stop-session": action_stop_session,
    }

    handler = action_map.get(args.action)
    if handler is None:
        print(_friendly_error(f"Неизвестное действие: {args.action}"), file=sys.stderr)
        return EXIT_FAILURE

    try:
        result = handler(client, args)
    except ValueError as e:
        print(str(e), file=sys.stderr)
        return EXIT_FAILURE
    except Exception as e:
        print(_handle_boto3_error(e), file=sys.stderr)
        return EXIT_FAILURE

    # Если результат — пустой список, даём дружественное сообщение
    preview = json.dumps(result, indent=2, ensure_ascii=False, default=str)
    if _is_empty_listing(result, args.action):
        print("[INFO] Ресурсы не найдены. API вернул пустой список.", file=sys.stderr)
    print(preview)
    return EXIT_SUCCESS


if __name__ == "__main__":
    sys.exit(main())