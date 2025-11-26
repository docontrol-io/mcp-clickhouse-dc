"""Environment configuration for the MCP ClickHouse server.

This module handles all environment variable configuration with sensible defaults
and type conversion.
"""

from dataclasses import dataclass
import os
from typing import Optional
from enum import Enum
import logging

logger = logging.getLogger("mcp-clickhouse")


class TransportType(str, Enum):
    """Supported MCP server transport types."""

    STDIO = "stdio"
    HTTP = "http"
    SSE = "sse"

    @classmethod
    def values(cls) -> list[str]:
        """Get all valid transport values."""
        return [transport.value for transport in cls]


@dataclass
class ClickHouseConfig:
    """Configuration for ClickHouse connection settings.

    This class handles all environment variable configuration with sensible defaults
    and type conversion. It provides typed methods for accessing each configuration value.

    Required environment variables (only when CLICKHOUSE_ENABLED=true):
        CLICKHOUSE_HOST: The hostname of the ClickHouse server
        CLICKHOUSE_USER: The username for authentication
        CLICKHOUSE_PASSWORD: The password for authentication

    Optional environment variables (with defaults):
        CLICKHOUSE_PORT: The port number (default: 8443 if secure=True, 8123 if secure=False)
        CLICKHOUSE_SECURE: Enable HTTPS (default: true)
        CLICKHOUSE_VERIFY: Verify SSL certificates (default: true)
        CLICKHOUSE_CONNECT_TIMEOUT: Connection timeout in seconds (default: 30)
        CLICKHOUSE_SEND_RECEIVE_TIMEOUT: Send/receive timeout in seconds (default: 300)
        CLICKHOUSE_DATABASE: Default database to use (default: None)
        CLICKHOUSE_PROXY_PATH: Path to be added to the host URL. For instance, for servers behind an HTTP proxy (default: None)
        CLICKHOUSE_ENABLED: Enable ClickHouse server (default: true)
    """

    def __init__(self):
        """Initialize the configuration from environment variables."""
        if self.enabled:
            self._validate_required_vars()

    @property
    def enabled(self) -> bool:
        """Get whether ClickHouse server is enabled.

        Default: True
        """
        return os.getenv("CLICKHOUSE_ENABLED", "true").lower() == "true"

    @property
    def host(self) -> str:
        """Get the ClickHouse host."""
        return os.environ["CLICKHOUSE_HOST"]

    @property
    def port(self) -> int:
        """Get the ClickHouse port.

        Defaults to 8443 if secure=True, 8123 if secure=False.
        Can be overridden by CLICKHOUSE_PORT environment variable.
        """
        if "CLICKHOUSE_PORT" in os.environ:
            return int(os.environ["CLICKHOUSE_PORT"])
        return 8443 if self.secure else 8123

    @property
    def username(self) -> str:
        """Get the ClickHouse username."""
        return os.environ["CLICKHOUSE_USER"]

    @property
    def password(self) -> str:
        """Get the ClickHouse password.

        Tries to fetch from AWS SSM if CLICKHOUSE_PASSWORD_FROM_SSM_PATH is set,
        otherwise uses CLICKHOUSE_PASSWORD environment variable.
        """
        ssm_path = os.getenv("CLICKHOUSE_PASSWORD_FROM_SSM_PATH")
        if ssm_path:
            return self._get_password_from_ssm(ssm_path)
        return os.environ["CLICKHOUSE_PASSWORD"]

    def _get_password_from_ssm(self, parameter_path: str) -> str:
        """Retrieve password from AWS Systems Manager Parameter Store.

        Args:
            parameter_path: SSM parameter path (e.g., /prod/clickhouse/password)

        Returns:
            The password value from SSM

        Raises:
            ValueError: If SSM retrieval fails
        """
        try:
            import boto3
            from botocore.exceptions import ClientError, BotoCoreError

            logger.info(f"Retrieving ClickHouse password from SSM: {parameter_path}")
            ssm = boto3.client("ssm")
            response = ssm.get_parameter(Name=parameter_path, WithDecryption=True)
            password = response["Parameter"]["Value"]
            logger.info("Successfully retrieved password from SSM")
            return password
        except (ClientError, BotoCoreError) as e:
            error_msg = (
                f"Failed to retrieve password from SSM path '{parameter_path}': {e}"
            )
            logger.error(error_msg)
            raise ValueError(error_msg)
        except ImportError:
            error_msg = "boto3 is required for SSM password retrieval. Install with: pip install boto3"
            logger.error(error_msg)
            raise ValueError(error_msg)

    @property
    def database(self) -> Optional[str]:
        """Get the default database name if set."""
        return os.getenv("CLICKHOUSE_DATABASE")

    @property
    def secure(self) -> bool:
        """Get whether HTTPS is enabled.

        Default: True
        """
        return os.getenv("CLICKHOUSE_SECURE", "true").lower() == "true"

    @property
    def verify(self) -> bool:
        """Get whether SSL certificate verification is enabled.

        Default: True
        """
        return os.getenv("CLICKHOUSE_VERIFY", "true").lower() == "true"

    @property
    def connect_timeout(self) -> int:
        """Get the connection timeout in seconds.

        Default: 30
        """
        return int(os.getenv("CLICKHOUSE_CONNECT_TIMEOUT", "30"))

    @property
    def send_receive_timeout(self) -> int:
        """Get the send/receive timeout in seconds.

        Default: 300 (ClickHouse default)
        """
        return int(os.getenv("CLICKHOUSE_SEND_RECEIVE_TIMEOUT", "300"))

    @property
    def proxy_path(self) -> str:
        return os.getenv("CLICKHOUSE_PROXY_PATH")

    def get_client_config(self) -> dict:
        """Get the configuration dictionary for clickhouse_connect client.

        Returns:
            dict: Configuration ready to be passed to clickhouse_connect.get_client()
        """
        config = {
            "host": self.host,
            "port": self.port,
            "username": self.username,
            "password": self.password,
            "interface": "https" if self.secure else "http",
            "secure": self.secure,
            "verify": self.verify,
            "connect_timeout": self.connect_timeout,
            "send_receive_timeout": self.send_receive_timeout,
            "client_name": "mcp_clickhouse",
        }

        # Add optional database if set
        if self.database:
            config["database"] = self.database

        if self.proxy_path:
            config["proxy_path"] = self.proxy_path

        return config

    def _validate_required_vars(self) -> None:
        """Validate that all required environment variables are set.

        Raises:
            ValueError: If any required environment variable is missing.
        """
        missing_vars = []
        for var in ["CLICKHOUSE_HOST", "CLICKHOUSE_USER", "CLICKHOUSE_PASSWORD"]:
            if var not in os.environ:
                missing_vars.append(var)

        if missing_vars:
            raise ValueError(
                f"Missing required environment variables: {', '.join(missing_vars)}"
            )


@dataclass
class ChDBConfig:
    """Configuration for chDB connection settings.

    This class handles all environment variable configuration with sensible defaults
    and type conversion. It provides typed methods for accessing each configuration value.

    Required environment variables:
        CHDB_DATA_PATH: The path to the chDB data directory (only required if CHDB_ENABLED=true)
    """

    def __init__(self):
        """Initialize the configuration from environment variables."""
        if self.enabled:
            self._validate_required_vars()

    @property
    def enabled(self) -> bool:
        """Get whether chDB is enabled.

        Default: False
        """
        return os.getenv("CHDB_ENABLED", "false").lower() == "true"

    @property
    def data_path(self) -> str:
        """Get the chDB data path."""
        return os.getenv("CHDB_DATA_PATH", ":memory:")

    def get_client_config(self) -> dict:
        """Get the configuration dictionary for chDB client.

        Returns:
            dict: Configuration ready to be passed to chDB client
        """
        return {
            "data_path": self.data_path,
        }

    def _validate_required_vars(self) -> None:
        """Validate that all required environment variables are set.

        Raises:
            ValueError: If any required environment variable is missing.
        """
        pass


# Global instance placeholders for the singleton pattern
_CONFIG_INSTANCE = None
_CHDB_CONFIG_INSTANCE = None


def get_config():
    """
    Gets the singleton instance of ClickHouseConfig.
    Instantiates it on the first call.
    """
    global _CONFIG_INSTANCE
    if _CONFIG_INSTANCE is None:
        # Instantiate the config object here, ensuring load_dotenv() has likely run
        _CONFIG_INSTANCE = ClickHouseConfig()
    return _CONFIG_INSTANCE


def get_chdb_config() -> ChDBConfig:
    """
    Gets the singleton instance of ChDBConfig.
    Instantiates it on the first call.

    Returns:
        ChDBConfig: The chDB configuration instance
    """
    global _CHDB_CONFIG_INSTANCE
    if _CHDB_CONFIG_INSTANCE is None:
        _CHDB_CONFIG_INSTANCE = ChDBConfig()
    return _CHDB_CONFIG_INSTANCE


@dataclass
class MCPServerConfig:
    """Configuration for MCP server-level settings.

    These settings control the server transport and tool behavior and are
    intentionally independent of ClickHouse connection validation.

    Optional environment variables (with defaults):
        CLICKHOUSE_MCP_SERVER_TRANSPORT: "stdio", "http", or "sse" (default: stdio)
        CLICKHOUSE_MCP_BIND_HOST: Bind host for HTTP/SSE (default: 127.0.0.1)
        CLICKHOUSE_MCP_BIND_PORT: Bind port for HTTP/SSE (default: 8000)
        CLICKHOUSE_MCP_QUERY_TIMEOUT: SELECT tool timeout in seconds (default: 30)
    """

    @property
    def server_transport(self) -> str:
        transport = os.getenv(
            "CLICKHOUSE_MCP_SERVER_TRANSPORT", TransportType.STDIO.value
        ).lower()
        if transport not in TransportType.values():
            valid_options = ", ".join(f'"{t}"' for t in TransportType.values())
            raise ValueError(
                f"Invalid transport '{transport}'. Valid options: {valid_options}"
            )
        return transport

    @property
    def bind_host(self) -> str:
        return os.getenv("CLICKHOUSE_MCP_BIND_HOST", "127.0.0.1")

    @property
    def bind_port(self) -> int:
        return int(os.getenv("CLICKHOUSE_MCP_BIND_PORT", "8000"))

    @property
    def query_timeout(self) -> int:
        return int(os.getenv("CLICKHOUSE_MCP_QUERY_TIMEOUT", "30"))


_MCP_CONFIG_INSTANCE = None


def get_mcp_config() -> MCPServerConfig:
    """Gets the singleton instance of MCPServerConfig."""
    global _MCP_CONFIG_INSTANCE
    if _MCP_CONFIG_INSTANCE is None:
        _MCP_CONFIG_INSTANCE = MCPServerConfig()
    return _MCP_CONFIG_INSTANCE
