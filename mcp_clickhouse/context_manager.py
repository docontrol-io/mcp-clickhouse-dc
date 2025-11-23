"""Context management for MCP ClickHouse server.

This module handles extraction of user context (user_name, company_id) from
MCP request metadata for row-level security and auditing.
"""

from dataclasses import dataclass
from typing import Optional
from fastmcp import Context


@dataclass
class UserContext:
    """User context extracted from MCP request metadata.

    Attributes:
        user_name: Optional username for logging and auditing
        company_id: Company identifier used for row-level security via SET role
    """

    user_name: Optional[str]
    company_id: Optional[str]


def get_user_context(ctx: Context) -> Optional[UserContext]:
    """Safely extract user_name and company_id from context metadata.

    Args:
        ctx: FastMCP Context object containing request metadata

    Returns:
        UserContext if metadata exists, None otherwise

    Example:
        >>> user_context = get_user_context(ctx)
        >>> if user_context and user_context.company_id:
        >>>     print(f"Company: {user_context.company_id}")
    """
    if not ctx or not ctx.request_context or not ctx.request_context.meta:
        return None

    meta = ctx.request_context.meta
    return UserContext(
        user_name=getattr(meta, "user_name", None),
        company_id=getattr(meta, "company_id", None),
    )
