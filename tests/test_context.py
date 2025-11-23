"""Tests for MCP context handling and row-level security."""

import pytest
from unittest.mock import Mock, MagicMock, patch
from fastmcp.exceptions import ToolError

from mcp_clickhouse.context_manager import get_user_context, UserContext


class TestGetUserContext:
    """Tests for get_user_context helper function."""

    def test_get_user_context_with_valid_metadata(self):
        """Test successful context extraction with both user_name and company_id."""
        # Create mock context with metadata
        ctx = Mock()
        ctx.request_context = Mock()
        ctx.request_context.meta = Mock()
        ctx.request_context.meta.user_name = "john_doe"
        ctx.request_context.meta.company_id = "acme_corp"

        result = get_user_context(ctx)

        assert result is not None
        assert isinstance(result, UserContext)
        assert result.user_name == "john_doe"
        assert result.company_id == "acme_corp"

    def test_get_user_context_with_only_company_id(self):
        """Test context extraction with only company_id (user_name missing)."""
        ctx = Mock()
        ctx.request_context = Mock()
        ctx.request_context.meta = Mock()
        ctx.request_context.meta.company_id = "acme_corp"
        # user_name attribute doesn't exist

        result = get_user_context(ctx)

        assert result is not None
        assert result.user_name is None
        assert result.company_id == "acme_corp"

    def test_get_user_context_with_no_metadata(self):
        """Test returns None when context has no metadata."""
        ctx = Mock()
        ctx.request_context = Mock()
        ctx.request_context.meta = None

        result = get_user_context(ctx)

        assert result is None

    def test_get_user_context_with_no_request_context(self):
        """Test returns None when context has no request_context."""
        ctx = Mock()
        ctx.request_context = None

        result = get_user_context(ctx)

        assert result is None

    def test_get_user_context_with_none_context(self):
        """Test returns None when context is None."""
        result = get_user_context(None)
        assert result is None


class TestExecuteQuery:
    """Tests for execute_query with context validation."""

    @patch("mcp_clickhouse.mcp_server.create_clickhouse_client")
    @patch("mcp_clickhouse.mcp_server.get_readonly_setting")
    def test_execute_query_without_company_id_raises_error(
        self, mock_get_readonly, mock_create_client
    ):
        """Test that execute_query raises ToolError when company_id is missing."""
        from mcp_clickhouse.mcp_server import execute_query

        # Create context without company_id
        ctx = Mock()
        ctx.request_context = Mock()
        ctx.request_context.meta = Mock()
        ctx.request_context.meta.user_name = "john_doe"
        # company_id missing

        with pytest.raises(ToolError) as exc_info:
            execute_query("SELECT 1", ctx)

        assert "company_id is required" in str(exc_info.value)
        mock_create_client.assert_called_once()

    @patch("mcp_clickhouse.mcp_server.create_clickhouse_client")
    @patch("mcp_clickhouse.mcp_server.get_readonly_setting")
    def test_execute_query_without_context_raises_error(
        self, mock_get_readonly, mock_create_client
    ):
        """Test that execute_query raises ToolError when context is None."""
        from mcp_clickhouse.mcp_server import execute_query

        ctx = Mock()
        ctx.request_context = None

        with pytest.raises(ToolError) as exc_info:
            execute_query("SELECT 1", ctx)

        assert "company_id is required" in str(exc_info.value)

    @patch("mcp_clickhouse.mcp_server.create_clickhouse_client")
    @patch("mcp_clickhouse.mcp_server.get_readonly_setting")
    def test_execute_query_with_company_id_sets_role(
        self, mock_get_readonly, mock_create_client
    ):
        """Test that execute_query calls SET role when company_id is present."""
        from mcp_clickhouse.mcp_server import execute_query

        # Create valid context
        ctx = Mock()
        ctx.request_context = Mock()
        ctx.request_context.meta = Mock()
        ctx.request_context.meta.user_name = "john_doe"
        ctx.request_context.meta.company_id = "acme_corp"

        # Mock client
        mock_client = MagicMock()
        mock_create_client.return_value = mock_client
        mock_get_readonly.return_value = "1"

        # Mock query result
        mock_result = Mock()
        mock_result.column_names = ["col1"]
        mock_result.result_rows = [[1]]
        mock_client.query.return_value = mock_result

        result = execute_query("SELECT 1", ctx)

        # Verify SET role was called
        mock_client.command.assert_called_once_with("SET role=acme_corp")

        # Verify query was executed
        mock_client.query.assert_called_once()

        # Verify result
        assert result == {"columns": ["col1"], "rows": [[1]]}

    @patch("mcp_clickhouse.mcp_server.create_clickhouse_client")
    def test_execute_query_set_role_failure_raises_error(self, mock_create_client):
        """Test that execute_query raises ToolError when SET role fails."""
        from mcp_clickhouse.mcp_server import execute_query

        # Create valid context
        ctx = Mock()
        ctx.request_context = Mock()
        ctx.request_context.meta = Mock()
        ctx.request_context.meta.user_name = "john_doe"
        ctx.request_context.meta.company_id = "invalid_role"

        # Mock client that fails on SET role
        mock_client = MagicMock()
        mock_create_client.return_value = mock_client
        mock_client.command.side_effect = Exception("Role not found")

        with pytest.raises(ToolError) as exc_info:
            execute_query("SELECT 1", ctx)

        assert "Failed to set role" in str(exc_info.value)
        mock_client.command.assert_called_once_with("SET role=invalid_role")


class TestListDatabases:
    """Tests for list_databases with context validation."""

    @patch("mcp_clickhouse.mcp_server.create_clickhouse_client")
    def test_list_databases_without_company_id_raises_error(self, mock_create_client):
        """Test that list_databases raises ToolError when company_id is missing."""
        from mcp_clickhouse.mcp_server import list_databases

        # Create context without company_id
        ctx = Mock()
        ctx.request_context = Mock()
        ctx.request_context.meta = Mock()
        ctx.request_context.meta.user_name = "john_doe"

        with pytest.raises(ToolError) as exc_info:
            list_databases(ctx)

        assert "company_id is required" in str(exc_info.value)

    @patch("mcp_clickhouse.mcp_server.create_clickhouse_client")
    def test_list_databases_with_company_id_sets_role(self, mock_create_client):
        """Test that list_databases calls SET role when company_id is present."""
        from mcp_clickhouse.mcp_server import list_databases

        # Create valid context
        ctx = Mock()
        ctx.request_context = Mock()
        ctx.request_context.meta = Mock()
        ctx.request_context.meta.user_name = "john_doe"
        ctx.request_context.meta.company_id = "acme_corp"

        # Mock client
        mock_client = MagicMock()
        mock_create_client.return_value = mock_client
        mock_client.command.side_effect = [
            None,
            "default\nsystem",
        ]  # First for SET role, second for SHOW DATABASES

        result = list_databases(ctx)

        # Verify SET role was called first
        assert mock_client.command.call_count == 2
        mock_client.command.assert_any_call("SET role=acme_corp")
        mock_client.command.assert_any_call("SHOW DATABASES")


class TestListTables:
    """Tests for list_tables with context validation."""

    @patch("mcp_clickhouse.mcp_server.create_clickhouse_client")
    def test_list_tables_without_company_id_raises_error(self, mock_create_client):
        """Test that list_tables raises ToolError when company_id is missing."""
        from mcp_clickhouse.mcp_server import list_tables

        # Create context without company_id
        ctx = Mock()
        ctx.request_context = Mock()
        ctx.request_context.meta = Mock()
        ctx.request_context.meta.user_name = "john_doe"

        with pytest.raises(ToolError) as exc_info:
            list_tables("default", ctx=ctx)

        assert "company_id is required" in str(exc_info.value)

    @patch("mcp_clickhouse.mcp_server.fetch_table_names_from_system")
    @patch("mcp_clickhouse.mcp_server.get_paginated_table_data")
    @patch("mcp_clickhouse.mcp_server.create_clickhouse_client")
    def test_list_tables_with_company_id_sets_role(
        self, mock_create_client, mock_get_paginated, mock_fetch_names
    ):
        """Test that list_tables calls SET role when company_id is present."""
        from mcp_clickhouse.mcp_server import list_tables

        # Create valid context
        ctx = Mock()
        ctx.request_context = Mock()
        ctx.request_context.meta = Mock()
        ctx.request_context.meta.user_name = "john_doe"
        ctx.request_context.meta.company_id = "acme_corp"

        # Mock client
        mock_client = MagicMock()
        mock_create_client.return_value = mock_client

        # Mock helper functions
        mock_fetch_names.return_value = ["table1", "table2"]
        mock_get_paginated.return_value = ([], 0, False)

        result = list_tables("default", ctx=ctx)

        # Verify SET role was called
        mock_client.command.assert_called_once_with("SET role=acme_corp")

        # Verify result structure
        assert "tables" in result
        assert "next_page_token" in result
        assert "total_tables" in result
