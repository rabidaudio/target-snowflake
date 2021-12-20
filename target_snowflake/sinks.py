"""Snowflake target sink class, which handles writing streams."""

from typing import Dict, List, Optional

from singer_sdk.target_base import Target

from target_snowflake.database_target.csv_sink import CSVSink
from target_snowflake.migrator import SnowflakeSchemaMigrator


class SnowflakeSink(CSVSink):
    """Snowflake target sink class."""

    def __init__(
        self,
        target: Target,
        stream_name: str,
        schema: Dict,
        key_properties: Optional[List[str]],
    ) -> None:
        super().__init__(target, stream_name, schema, key_properties)

        self.connection = target.connect()
        self._has_migrated = False
        self.migrator = SnowflakeSchemaMigrator(sink=self)
        self.table_schema = target.table_schema

    @property
    def max_size(self):
        return self.config["batch_size_rows"]

    def start_batch(self, context: dict) -> None:
        # TODO: perhaps Sync should have a callback hook at the beginning of execution?
        if not self._has_migrated:
            self.migrator.sync_table_schema()
            self._has_migrated = True
        super().start_batch(context)

    def process_batch(self, context: dict) -> None:
        super().process_batch(context)
        self.logger.info(f"TODO: load '{self.destination_path}' into Snowflake...")

        # copy the file to the stage
        # load the file from the stage into the database
        # (optional) archive the file
