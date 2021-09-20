import abc
import time
import logging

from typing import Any, Dict, Optional, List

from singer_sdk import PluginBase
from singer_sdk.helpers._classproperty import classproperty

# class Flattener:

#   def flatten_schema()
#   def flatten_rows()

# Metadata: not this classes responsibility

#  Metadata -> Flattening -> Schema Syncing


def convert_jsonschema_types_to_lists(jsonschema: dict) -> dict:
    """
    Walk the JSON Schema definition, converting any type definitions that are
    not lists to lists.
    """
    new_schema = jsonschema.copy()
    if "type" in new_schema and not isinstance(new_schema["type"], list):
        new_schema["type"] = [new_schema["type"]]
    for keyword in ["oneOf", "allOf", "anyOf"]:
        if keyword in jsonschema:
            new_schema[keyword] = [
                convert_jsonschema_types_to_lists(defn) for defn in jsonschema[keyword]
            ]
    if "items" in new_schema:
        new_schema["items"] = convert_jsonschema_types_to_lists(new_schema["items"])
    if "properties" in new_schema:
        new_schema["properties"] = {
            name: convert_jsonschema_types_to_lists(defn)
            for name, defn in new_schema["properties"].items()
        }
    return new_schema


class BaseSchemaSync(PluginBase, metaclass=abc.ABCMeta):
    def __init__(
        self,
        config: Optional[Dict[str, Any]] = None,
        parse_env_config: bool = False,
    ) -> None:
        """Initialize the target."""
        super().__init__(config=config, parse_env_config=parse_env_config)

    @classproperty
    def logger(cls) -> logging.Logger:
        """Get logger."""
        return logging.getLogger(str(cls))

    def sync_table(
        self, stream_name: str, key_properties: List[str], jsonschema: dict
    ) -> Dict[str, str]:
        """Create or update the table schema for the given stream."""
        schema = self.jsonschema_to_table_schema(jsonschema)
        table_name = self.stream_name_to_table_name(stream_name)
        existing_schema = self.get_table(table_name)
        if not existing_schema:
            self.logger.info(
                f"Creating table '{table_name}' for stream "
                f"'{stream_name}' with key properties: {key_properties}"
            )
            self.create_table(table_name, key_properties, schema)
            return schema

        for column_name, type in schema.items():
            if column_name not in existing_schema:
                self.logger.info(
                    "Adding column '{column_name}' to table '{table_name}'"
                )
                self.add_column(table_name, column_name, type)
            elif type != existing_schema[column_name]:
                new_column_name = self.version_column_name(column_name)
                self.logger.info(
                    f"Column '{column_name}' on table '{table_name}' changed from "
                    f"{existing_schema[column_name]} to {type}. "
                    f"Versioning column by renaming it to '{new_column_name}' and "
                    "adding a new column in it's place."
                )
                self.rename_column(table_name, column_name, new_column_name)
                self.add_column(table_name, column_name, type)
            else:
                self.logger.debug(
                    f"Table '{table_name}' for stream '{stream_name}' "
                    " already exists with the proper schema"
                )

        return schema

    def jsonschema_to_table_schema(self, jsonschema: dict) -> Optional[Dict[str, str]]:
        """
        Convert a JSON Schema definition of a stream to a map of column names to their
        SQL data types.
        """
        # TODO: can flattening be handled completely outside this class?
        # Normalize types to always be arrays for ease of type checking
        normalized = convert_jsonschema_types_to_lists(jsonschema)
        return {
            self.property_name_to_column_name(name): self.jsonschema_to_sql_type(
                definition
            )
            for name, definition in normalized["properties"].items()
        }

    def stream_name_to_table_name(self, stream_name: str) -> str:
        """Convert a stream name to a name for the table."""
        return stream_name

    def property_name_to_column_name(self, column_name: str) -> str:
        """Convert the name of a record property to a column name for the table."""
        return column_name

    def version_column_name(self, name: str) -> str:
        return "{}_{}".format(name, time.strftime("%Y%m%d_%H%M"))

    @abc.abstractmethod
    def jsonschema_to_sql_type(self, jsonschema: dict) -> str:
        """
        Convert the given JSON Schema property definition to a compatible SQL data
        type.
        """
        pass

    @abc.abstractmethod
    def get_table(self, table_name: str) -> Optional[Dict[str, str]]:
        """
        Check if a table already exists for `table_name`. If it does,
        return a map of existing column names to their SQL data types.
        If it does not exist, return None.
        """
        pass

    @abc.abstractmethod
    def create_table(
        self, table_name: str, key_properties: List[str], schema: Dict[str, str]
    ) -> None:
        """Create a table for the given stream."""
        pass

    @abc.abstractmethod
    def add_column(self, table_name: str, column_name: str, type: str) -> None:
        """Add a column to an existing table."""
        pass

    @abc.abstractmethod
    def rename_column(self, table_name: str, old_name: str, new_name: str) -> None:
        """Rename an existing column on the table."""
        pass
