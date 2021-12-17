"""Snowflake target sink class, which handles writing streams."""

import datetime
import tempfile
import pytz
from pathlib import Path
from typing import Dict, Iterable, List, Optional
from typing import Any, Dict, List

from singer_sdk import SQLSink, SQLTarget

from target_snowflake.connector import SnowflakeConnector
from target_snowflake.stages import NamedStage

import csv


class SnowflakeSink(SQLSink):
    """Snowflake target sink class."""

    connector_class = SnowflakeConnector
    stage_class = NamedStage

    def __init__(
        self,
        target: SQLTarget,
        stream_name: str,
        schema: Dict,
        key_properties: Optional[List[str]],
    ) -> None:
        super().__init__(target, stream_name, schema, key_properties)
        self.prepare_sink()

    @property
    def table_name(self) -> str:
        return self.stream_name.split("-")[-1].upper()

    @property
    def schema_name(self) -> Optional[str]:
        """Return the target schema name."""
        return self.config["snowflake"]["schema"].upper()

    @property
    def database_name(self) -> Optional[str]:
        """Return the target DB name."""
        return self.config["snowflake"]["database"].upper()

    def prepare_sink(self) -> None:
        self.connector.connection.execute(
            'CREATE SCHEMA IF NOT EXISTS "{self.schema_name}"'
        )
        self.connector.prepare_table(
            self.full_table_name, schema=self.schema, primary_keys=self.key_properties
        )
        self.stage = self.stage_class(self)

    @property
    def max_size(self):
        return self.config["batch_size_rows"]

    def _write_csv(self, filepath: Path, records: Iterable[dict]) -> None:
        """Write a CSV file."""
        with open(filepath, "wt") as fp:
            writer = csv.writer(fp, delimiter=",")
            for i, record in enumerate(records, start=1):
                if i == 1:
                    writer.writerow(record.keys())

                writer.writerow(record.values())
        self.logger.info(f"Completed writing {i} records to file...")

    def _upload_csv_to_table_stage(self, file_path: str, full_table_name: str) -> None:
        """Upload data to the table's built-in stage."""
        self.connector.connection.execute(
            f"PUT file://{file_path} @%{full_table_name};"
        )

    def _load_csv_files_from_table_stage(self, full_table_name: str) -> None:
        """Loads all files from the table's built-in stage."""
        self.connector.connection.execute(
            f"COPY INTO {full_table_name} "
            f"FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = '\\t' SKIP_HEADER = 1);"
        )

    @property
    def timestamp_time(self) -> datetime.datetime:
        try:
            return self._timestamp_time
        except AttributeError:
            self._timestamp_time = datetime.datetime.now(
                tz=pytz.timezone(self.config["timestamp_timezone"])
            )
            return self._timestamp_time

    @property
    def filepath_replacement_map(self) -> Dict[str, str]:
        return {
            "stream_name": self.stream_name,
            "datestamp": self.timestamp_time.strftime(self.config["datestamp_format"]),
            "timestamp": self.timestamp_time.strftime(self.config["timestamp_format"]),
        }

    def _tmp_dir(self) -> Path:
        """Return a temp directory path."""
        return Path(tempfile.mkdtemp(prefix="target-snowflake"))

    @property
    def destination_path(self) -> Path:
        result = str(self._tmp_dir / self.config["file_naming_scheme"])
        for key, val in self.filepath_replacement_map.items():
            replacement_pattern = "{" f"{key}" "}"
            if replacement_pattern in result:
                result = result.replace(replacement_pattern, val)

        if self.config.get("output_path_prefix", None) is not None:
            result = f"{self.config['output_path_prefix']}{result}"

        return Path(result)

    def process_batch(self, context: dict) -> None:
        """Write out any prepped records and return once fully written."""
        records: List[Dict[str, Any]] = context["records"]
        if "record_sort_property_name" in self.config:
            sort_property_name = self.config["record_sort_property_name"]
            records = sorted(records, key=lambda x: x[sort_property_name])

    def bulk_insert_records(
        self, full_table_name: str, schema: dict, records: Iterable[Dict[str, Any]]
    ) -> Optional[int]:
        if not records:
            self.logger.warning(f"No values in {self.stream_name} records collection.")
            return 0

        output_file: Path = self.destination_path
        self.logger.info(f"Writing to destination file '{output_file.resolve()}'...")

        self._write_csv(output_file, records)
        self._upload_csv_to_table_stage(output_file, full_table_name)
        self._load_csv_files_from_table_stage(full_table_name)
        # TODO: Optionally delete file.
