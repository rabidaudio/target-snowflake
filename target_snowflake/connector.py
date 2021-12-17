"""Defines the SnowflakeConnector class."""

from typing import Any, Dict
from singer_sdk import SQLConnector

from snowflake.sqlalchemy import URL, VARIANT, ARRAY, TIMESTAMP_TZ
import sqlalchemy


class SnowflakeConnector(SQLConnector):
    """The SQLConnector class for Snowflake."""

    def get_sqlalchemy_url(self, config: Dict[str, Any]) -> str:
        """Concatenate a SQLAlchemy connection URL.format

        Args:
            config: The tap's config dict.

        Returns:
            The connection url as a string.
        """
        conf = config["snowflake"]
        return URL(
            account=conf["account"],
            user=conf["user"],
            password=conf["password"],
            role=conf["role"],
            warehouse=conf["warehouse"],
            database=conf["database"],
            # schema=conf["schema"],  # Target schema may not be created yet
        )

    @staticmethod
    def to_sql_type(jsonschema_type: dict) -> sqlalchemy.types.TypeEngine:
        """Convert JSON Schema to the Snowflake SQL type.

        Handle special Snowflake types and then call the default handler for the rest.

        Note: ignores "null" types and assumes all types are nullable
        https://docs.snowflake.com/en/sql-reference/intro-summary-data-types.html

        Args:
            jsonschema_type (dict): [description]

        Returns:
            sqlalchemy.types.TypeEngine: [description]
        """
        if "array" in jsonschema_type["type"]:
            return ARRAY
        if "object" in jsonschema_type["type"]:
            return VARIANT
        if (
            "format" in jsonschema_type
            and jsonschema_type["format"] == "date-time"
            and "string" in jsonschema_type["type"]
        ):
            return TIMESTAMP_TZ

        return SQLConnector.to_sql_type(jsonschema_type)
