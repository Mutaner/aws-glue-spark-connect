"""Tests for aws-glue-spark-connect."""

import argparse
import sys
import uuid
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, "..")

from glue_spark_cli import (
    _auto_id,
    _friendly_error,
    _is_empty_listing,
    _parse_tags,
    _setup_debug_logging,
    build_parser,
    cmd_get_endpoint,
    cmd_list_sessions,
    cmd_stop_session,
    main,
)


# ---------------------------------------------------------------------------
# _friendly_error
# ---------------------------------------------------------------------------

class TestFriendlyError:
    def test_with_msg_only(self):
        r = _friendly_error("Something went wrong.")
        assert r == "[ERROR] Something went wrong."

    def test_with_msg_and_detail(self):
        r = _friendly_error("Failed.", "Check the ID.")
        assert r == "[ERROR] Failed.\nCheck the ID."

    def test_empty_detail(self):
        r = _friendly_error("Error", "")
        assert r == "[ERROR] Error"


# ---------------------------------------------------------------------------
# _auto_id
# ---------------------------------------------------------------------------

class TestAutoId:
    def test_returns_valid_uuid_string(self):
        result = _auto_id()
        parsed = uuid.UUID(result)
        assert str(parsed) == result

    def test_unique_across_calls(self):
        ids = {_auto_id() for _ in range(100)}
        assert len(ids) == 100


# ---------------------------------------------------------------------------
# _parse_tags
# ---------------------------------------------------------------------------

class TestParseTags:
    def test_valid_tags(self):
        result = _parse_tags(["env=prod", "team=data"])
        assert result == {"env": "prod", "team": "data"}

    def test_missing_equals_raises_value_error(self):
        with pytest.raises(ValueError) as exc:
            _parse_tags(["badformat"])
        assert "Invalid tag format" in str(exc.value)

    def test_empty_key_raises_value_error(self):
        with pytest.raises(ValueError) as exc:
            _parse_tags(["=value"])
        assert "Empty key" in str(exc.value)

    def test_value_with_equals(self):
        result = _parse_tags(["key=val=ue"])
        assert result == {"key": "val=ue"}

    def test_multiple_tags(self):
        result = _parse_tags(["a=1", "b=2", "c=3"])
        assert result == {"a": "1", "b": "2", "c": "3"}


# ---------------------------------------------------------------------------
# _is_empty_listing
# ---------------------------------------------------------------------------

class TestIsEmptyListing:
    def test_empty_sessions_returns_true(self):
        assert _is_empty_listing({"Sessions": []}, "list-sessions") is True

    def test_non_empty_sessions_returns_false(self):
        assert _is_empty_listing({"Sessions": [{"Id": "1"}]}, "list-sessions") is False

    def test_wrong_action_returns_false(self):
        assert _is_empty_listing({"Sessions": []}, "start-session") is False

    def test_not_a_dict_returns_false(self):
        assert _is_empty_listing("not a dict", "list-sessions") is False

    def test_missing_key_returns_false(self):
        assert _is_empty_listing({}, "list-sessions") is False

    def test_sessions_is_none_returns_false(self):
        assert _is_empty_listing({"Sessions": None}, "list-sessions") is False

    def test_irrelevant_keys_ignored(self):
        assert _is_empty_listing({"Flows": [], "Ids": []}, "list-sessions") is False


# ---------------------------------------------------------------------------
# build_parser
# ---------------------------------------------------------------------------

class TestBuildParser:
    def test_parser_created(self):
        parser = build_parser()
        assert isinstance(parser, argparse.ArgumentParser)

    def test_help_exits_ok(self):
        parser = build_parser()
        with pytest.raises(SystemExit) as exc:
            parser.parse_args(["--help"])
        assert exc.value.code == 0

    def test_missing_action_raises_system_exit(self):
        parser = build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["--region", "us-east-1"])

    def test_start_session_without_role_arn_raises_system_exit(self):
        parser = build_parser()
        with pytest.raises(SystemExit) as exc:
            parser.parse_args(["start-session", "--name", "test"])
        assert exc.value.code == 2

    def test_start_session_minimal_args(self):
        parser = build_parser()
        args = parser.parse_args([
            "start-session",
            "--role-arn", "arn:aws:iam::1:role/Test",
        ])
        assert args.action == "start-session"
        assert args.role_arn == "arn:aws:iam::1:role/Test"

    def test_list_sessions_minimal(self):
        parser = build_parser()
        args = parser.parse_args(["list-sessions"])
        assert args.action == "list-sessions"

    def test_stop_session_minimal(self):
        parser = build_parser()
        args = parser.parse_args(["stop-session", "--id", "abc-123"])
        assert args.action == "stop-session"
        assert args.id == "abc-123"

    def test_get_endpoint_minimal(self):
        parser = build_parser()
        args = parser.parse_args(["get-endpoint", "--id", "abc-123"])
        assert args.action == "get-endpoint"
        assert args.id == "abc-123"

    def test_get_endpoint_missing_id_raises_system_exit(self):
        parser = build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["get-endpoint"])

    def test_invalid_worker_type_raises_system_exit(self):
        parser = build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args([
                "start-session",
                "--worker-type", "G.99X",
                "--role-arn", "arn:aws:iam::1:role/Test",
            ])

    def test_non_int_number_of_workers_raises_system_exit(self):
        parser = build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args([
                "start-session",
                "--number-of-workers", "abc",
                "--role-arn", "arn:aws:iam::1:role/Test",
            ])

    def test_debug_flag(self):
        parser = build_parser()
        args = parser.parse_args(["--debug", "list-sessions"])
        assert args.debug is True


# ---------------------------------------------------------------------------
# cmd_list_sessions
# ---------------------------------------------------------------------------

class TestCmdListSessions:
    def test_minimal(self):
        client = MagicMock()
        client.list_sessions.return_value = {"Sessions": []}
        args = argparse.Namespace(max_results=None, next_token=None, status=None)
        result = cmd_list_sessions(client, args)
        assert result == {"Sessions": []}

    def test_with_pagination(self):
        client = MagicMock()
        args = argparse.Namespace(max_results=10, next_token="tok", status=None)
        cmd_list_sessions(client, args)
        call_kwargs = client.list_sessions.call_args[1]
        assert call_kwargs["MaxResults"] == 10
        assert call_kwargs["NextToken"] == "tok"

    def test_status_filter(self):
        client = MagicMock()
        client.list_sessions.return_value = {
            "Sessions": [
                {"Id": "1", "Status": "READY"},
                {"Id": "2", "Status": "STOPPED"},
            ]
        }
        args = argparse.Namespace(max_results=None, next_token=None, status="READY")
        result = cmd_list_sessions(client, args)
        assert len(result["Sessions"]) == 1
        assert result["Sessions"][0]["Id"] == "1"


# ---------------------------------------------------------------------------
# cmd_stop_session
# ---------------------------------------------------------------------------

class TestCmdStopSession:
    def test_stop_called_with_id(self):
        client = MagicMock()
        client.stop_session.return_value = {"Id": "abc-123"}
        args = argparse.Namespace(id="abc-123")
        result = cmd_stop_session(client, args)
        assert client.stop_session.call_args[1]["Id"] == "abc-123"
        assert result == {"Id": "abc-123"}


# ---------------------------------------------------------------------------
# cmd_get_endpoint
# ---------------------------------------------------------------------------

class TestCmdGetEndpoint:
    def test_get_endpoint_called_with_id(self):
        client = MagicMock()
        client.get_session_endpoint.return_value = {
            "SparkConnect": {
                "Url": "grpc://spark-connect.glue.us-east-1.amazonaws.com:443",
                "AuthToken": "token-abc",
                "AuthTokenExpirationTime": "2026-06-10T18:00:00Z",
            }
        }
        args = argparse.Namespace(id="abc-123")
        result = cmd_get_endpoint(client, args)
        assert client.get_session_endpoint.call_args[1]["SessionId"] == "abc-123"
        assert result["SparkConnect"]["Url"].startswith("grpc://")


# ---------------------------------------------------------------------------
# _setup_debug_logging
# ---------------------------------------------------------------------------

class TestSetupDebugLogging:
    def test_runs_without_exception(self):
        _setup_debug_logging()

    def test_warning_printed_to_stderr(self, capsys):
        _setup_debug_logging()
        captured = capsys.readouterr()
        assert "WARNING" in captured.err


# ---------------------------------------------------------------------------
# main() integration smoke test
# ---------------------------------------------------------------------------

class TestMain:
    def test_no_args_exits_failure(self):
        with pytest.raises(SystemExit) as exc:
            main()
        assert exc.value.code != 0