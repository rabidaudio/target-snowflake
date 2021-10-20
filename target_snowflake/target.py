"""Snowflake target class."""

from logging import Logger
from typing import Any, Dict, Optional, Union, List
import snowflake.connector

from singer_sdk.target_base import Target
from singer_sdk import typing as th

from target_snowflake.sinks import (
    SnowflakeSink,
)

from target_snowflake.stages import NamedStage


class Connection:
    """
    A wrapper for a Snowflake database connection.

    This is to give each sink it's own connection to avoid threading
    issues.
    """

    def __init__(self, logger: Logger, **kwargs) -> None:
        self.logger = logger
        self.connection = snowflake.connector.connect(**kwargs)

    def execute(self, sql: str, *args) -> None:
        with self.connection.cursor() as cur:
            self.logger.debug(sql)
            cur.execute(sql, *args)

    def query(self, sql: Union[str, List[str]], **kwargs) -> List[Dict[str, Any]]:
        with self.connection.cursor(snowflake.connector.DictCursor) as cur:
            is_transaction = False
            if isinstance(sql, list):
                self.logger.debug("START TRANSACTION")
                cur.execute("START TRANSACTION")
                is_transaction = True
            else:
                sql = [sql]
            for query in sql:
                self.logger.debug(query)
                cur.execute(query, kwargs)
                result = cur.fetchall()
            if is_transaction:
                cur.execute("COMMIT")
            return result

    def close(self) -> None:
        self.connection.close()


class SnowflakeTarget(Target):
    """Singer Target for Snowflake database."""

    name = "target-snowflake"
    default_sink_class = SnowflakeSink
    stage_class = NamedStage

    config_jsonschema = th.PropertiesList(
        th.Property(
            "snowflake",
            th.PropertiesList(
                th.Property("account", th.StringType, required=True),
                th.Property("user", th.StringType, required=True),
                th.Property("password", th.StringType, required=True),
                th.Property("database", th.StringType, required=True),
                th.Property("role", th.StringType),
                th.Property("schema", th.StringType, default="PUBLIC"),
                th.Property("warehouse", th.StringType),
            ),
            required=True,
        ),
        # Stage and Storage config
        # Flattening config
        # metadata flag?
        # csv:
        #   record_sort_property_name
        #   overwrite_behavior
        #   output_path_prefix
        #   timestamp_timezone
        #   datestamp_format
        #   timestamp_format
        th.Property("stage", th.StringType, default="target-snowflake"),
        th.Property("batch_size_rows", th.IntegerType, default=100000),
        th.Property("raise_on_column_conflicts", th.BooleanType, default=False),
    ).to_dict()

    def __init__(
        self,
        config: Optional[Dict[str, Any]] = None,
        parse_env_config: bool = False,
    ) -> None:
        super().__init__(config=config, parse_env_config=parse_env_config)
        self.table_schema = self.config["snowflake"]["schema"].upper()
        self.stage = self.stage_class(self)

        # TODO: perhaps Target should have a setup callback hook?
        self._prepare_load()

    def _prepare_load(self) -> None:
        connection = self.connect()
        connection.execute('CREATE SCHEMA IF NOT EXISTS "{}"'.format(self.table_schema))

        connection.close()

    def connect(self) -> Connection:
        """Create a new database connection."""
        return Connection(self.logger, **self.config["snowflake"])
