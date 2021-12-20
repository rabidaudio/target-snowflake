"""CSV target sink class, which handles writing streams."""

import csv
import datetime
import sys
from pathlib import Path
from typing import Any, Dict, List

import pytz
from singer_sdk.sinks import BatchSink


class CSVSink(BatchSink):
    """CSV target sink class."""

    max_size = sys.maxsize  # We want all records in one batch

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

    @property
    def destination_path(self) -> Path:
        result = self.config["file_naming_scheme"]
        for key, val in self.filepath_replacement_map.items():
            replacement_pattern = "{" f"{key}" "}"
            if replacement_pattern in result:
                result = result.replace(replacement_pattern, val)

        if self.config.get("output_path_prefix", None) is not None:
            result = f"{self.config['output_path_prefix']}{result}"

        return Path(result)

    def _write_csv(self, filepath: Path, records: List[dict]) -> None:
        """Write a CSV file."""
        with open(filepath, "wt") as fp:
            writer = csv.writer(fp, delimiter=",")
            for i, record in enumerate(records, start=1):
                if i == 1:
                    writer.writerow(record.keys())

                writer.writerow(record.values())

    def process_batch(self, context: dict) -> None:
        """Write out any prepped records and return once fully written."""
        output_file: Path = self.destination_path
        self.logger.info(f"Writing to destination file '{output_file.resolve()}'...")
        new_contents: dict
        create_new = (
            self.config["overwrite_behavior"] == "replace_file"
            or not output_file.exists()
        )
        if not create_new:
            raise NotImplementedError("Append mode is not yet supported.")

        if not isinstance(context["records"], list):
            self.logger.warning(f"No values in {self.stream_name} records collection.")
            context["records"] = []

        records: List[Dict[str, Any]] = context["records"]
        if "record_sort_property_name" in self.config:
            sort_property_name = self.config["record_sort_property_name"]
            records = sorted(records, key=lambda x: x[sort_property_name])

        self.logger.info(f"Writing {len(context['records'])} records to file...")

        self._write_csv(output_file, context["records"])
