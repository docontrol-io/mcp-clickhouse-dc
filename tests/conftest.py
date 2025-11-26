"""Shared test fixtures and configuration."""

import pytest
from unittest.mock import patch


@pytest.fixture
def request_context():
    """Create a request context dict with valid company_id and user_name."""
    return {"user_name": "test_user", "company_id": "test_company"}


@pytest.fixture
def request_context_no_company():
    """Create a request context dict without company_id (should fail validation)."""
    return {"user_name": "test_user"}


@pytest.fixture(scope="function", autouse=True)
def mock_set_role_command():
    """Mock SET ROLE command globally to avoid permission issues."""
    from clickhouse_connect.driver.httpclient import HttpClient

    original_command = HttpClient.command

    def command_wrapper(self, cmd, *args, **kwargs):
        if isinstance(cmd, str) and cmd.startswith("SET ROLE"):
            # Mock SET ROLE - don't actually execute it during tests
            return None
        return original_command(self, cmd, *args, **kwargs)

    with patch.object(HttpClient, "command", command_wrapper):
        yield
