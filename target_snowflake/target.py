"""Snowflake target class."""

from singer_sdk.target_base import Target
from singer_sdk import typing as th

from target_snowflake.sinks import (
    SnowflakeSink,
)


class TargetSnowflake(Target):
    """Sample target for Snowflake."""

    name = "target-snowflake"
    config_jsonschema = th.PropertiesList(
        th.Property("filepath", th.StringType),
        th.Property("file_naming_scheme", th.StringType),
    ).to_dict()
    default_sink_class = SnowflakeSink
