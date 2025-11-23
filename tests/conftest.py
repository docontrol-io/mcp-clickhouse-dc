"""Shared test fixtures and configuration."""

import pytest
from unittest.mock import Mock


@pytest.fixture
def mock_context():
    """Create a mock Context with valid company_id and user_name."""
    ctx = Mock()
    ctx.request_context = Mock()
    ctx.request_context.meta = Mock()
    ctx.request_context.meta.user_name = "test_user"
    ctx.request_context.meta.company_id = "test_company"
    return ctx


@pytest.fixture
def mock_context_no_company():
    """Create a mock Context without company_id (should fail validation)."""
    ctx = Mock()
    ctx.request_context = Mock()
    ctx.request_context.meta = Mock()
    # Use spec to ensure company_id attribute doesn't exist
    ctx.request_context.meta = Mock(spec=['user_name'])
    ctx.request_context.meta.user_name = "test_user"
    # company_id is not in spec, so hasattr will return False
    return ctx

