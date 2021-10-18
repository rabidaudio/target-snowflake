"""Snowflake target sink class, which handles writing streams."""

from target_snowflake.database_target.csv_sink import CSVSink
from target_snowflake.database_target.schema_migrator import SchemaMigrator
from target_snowflake.snowflake_migrator import SnowflakeSchemaMigrator


class SnowflakeSink(CSVSink):
    # def __init__(
    #     self,
    #     target: SnowflakeTarget,
    #     stream_name: str,
    #     schema: Dict,
    #     key_properties: Optional[List[str]],
    # ) -> None:
    #     super().__init__(target, stream_name, schema, key_properties)

    def migrator(self) -> SchemaMigrator:
        if not self._migrator:
            self._migrator = SnowflakeSchemaMigrator(
                self.target, self.stream_name, self.schema, self.key_properties
            )
        return self._migrator

    def start_batch(self, context: dict) -> None:
        self.migrator.sync_table()
        super().start_batch(context)

    def process_batch(self, context: dict) -> None:
        super().process_batch(context)
        # copy the file to the stage
        # load the file from the stage into the database
        # (optional) archive the file


# class SnowflakeSink(BatchSink):
#     """Snowflake target sink class."""

#     max_size = 10000  # Max records to write in one batch

#     def start_batch(self, context: dict) -> None:
#         """Start a batch.

#         Developers may optionally add additional markers to the `context` dict,
#         which is unique to this batch.
#         """
#         # Sample:
#         # ------
#         # batch_key = context["batch_id"]
#         # context["file_path"] = f"{batch_key}.csv"

#     def process_record(self, record: dict, context: dict) -> None:
#         """Process the record.

#         Developers may optionally read or write additional markers within the
#         passed `context` dict from the current batch.
#         """
#         # Sample:
#         # ------
#         # with open(context["file_path"], "a") as csvfile:
#         #     csvfile.write(record)

#     def process_batch(self, context: dict) -> None:
#         """Write out any prepped records and return once fully written."""
#         # Sample:
#         # ------
#         # client.upload(context["file_path"])  # Upload file
#         # Path(context["file_path"]).unlink()  # Delete local copy
