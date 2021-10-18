import abc
import time
from types import MappingProxyType

from typing import Any, Dict, Mapping, Optional, List

from singer_sdk.plugin_base import PluginBase


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

ColumnType = str

class SchemaMigrator(metaclass=abc.ABCMeta):
    def __init__(
        self,
        target: PluginBase,
        stream_name: str,
        schema: Dict,
        key_properties: Optional[List[str]],
    ) -> None:
        """
        Initialize the schema migrator.
        
        Args:
            target: Target instance.
            stream_name: Name of the stream to sink.
            schema: Schema of the stream to sink.
            key_properties: Primary key of the stream to sink.
        """
        self.target = target
        self.logger = target.logger
        self._config = dict(target.config)
        self.stream_name = stream_name
        self.key_properties = key_properties
        # Normalize types to always be arrays for ease of type checking
        # TODO: maybe there's a better way to do this?
        self.schema = convert_jsonschema_types_to_lists(schema)
        self._table_name = None
        self._column_definitions = None

    def sync_table(self) -> Dict[str, ColumnType]:
        """Create or update the table schema for the given stream."""
        existing_schema = self.get_table(self.table_name)
        if not existing_schema:
            self.logger.info(
                f"Creating table '{self.table_name}' for stream "
                f"'{self.stream_name}' with key properties: {self.key_properties}"
            )
            self.create_table(self.table_name, self.key_properties, self.column_definitions)
            return self.column_definitions

        for column_name, type in self.column_definitions.items():
            if column_name not in existing_schema:
                self.logger.info(
                    "Adding column '{column_name}' to table '{table_name}'"
                )
                self.add_column(self.table_name, column_name, type)
            elif type != existing_schema[column_name]:
                self.on_column_conflict(column_name, existing_schema[column_name], type)
            else:
                self.logger.debug(
                    f"Table '{self.table_name}' for stream '{self.stream_name}' "
                    " already exists with the proper schema"
                )

        return self.column_definitions

    @property
    def config(self) -> Mapping[str, Any]:
        """Get plugin configuration.

        Returns:
            A frozen (read-only) config dictionary map.
        """
        return MappingProxyType(self._config)

    @property
    def column_definitions(self) -> Dict[str, ColumnType]:
        """Return a mapping of column names to data types."""
        if not self._column_definitions:
            self._column_definitions = self.create_column_definitions(self.schema)
        return self._column_definitions

    @property
    def table_name(self) -> str:
        if not self._table_name:
            self._table_name = self.convert_stream_name_to_table_name(self.stream_name)
        return self._table_name

    def on_column_conflict(self, column_name: str, old_type: ColumnType, new_type: ColumnType):
        """
        Handle a change in data type for a column.

        The default implementation versions the column by renaming the old column
        unless `raise_on_column_conflicts` is enabled.
        """
        if self.raise_on_column_conflicts:
            raise Exception(
                f"Column '{column_name}' on table '{self.table_name}' changed from ",
                f"{old_type} to {new_type}."
            )

        new_column_name = self.version_column_name(column_name)
        self.logger.info(
            f"Column '{column_name}' on table '{self.table_name}' changed from "
            f"{old_type} to {new_type}. "
            f"Versioning column by renaming it to '{new_column_name}' and "
            "adding a new column in it's place."
        )
        self.rename_column(self.table_name, column_name, new_column_name)
        self.add_column(self.table_name, column_name, new_type)

    @property
    def raise_on_column_conflicts(self) -> bool:
      """Set to `True` to disable default versioning behavior on column conflicts."""
      return self.config.get('raise_on_column_conflicts', False)

    def create_column_definitions(self, schema: dict) -> Dict[str, ColumnType]:
        """
        Convert a JSON Schema definition of a stream to a map of column names to their
        SQL data types.
        """
        return {
            self.convert_property_name_to_column_name(name): self.convert_jsonschema_to_sql_type(definition)
            for name, definition in schema["properties"].items()
        }

    def convert_stream_name_to_table_name(self, stream_name: str) -> str:
        """Convert a stream name to a name for the table."""
        return stream_name

    def convert_property_name_to_column_name(self, column_name: str) -> str:
        """Convert the name of a record property to a column name for the table."""
        return column_name

    def version_column_name(self, column_name: str) -> str:
        """Return a new name for a versioned column."""
        return "{}_{}".format(column_name, time.strftime("%Y%m%d_%H%M"))

    @abc.abstractmethod
    def convert_jsonschema_to_sql_type(self, property_schema: dict) -> ColumnType:
        """
        Convert the given JSON Schema property definition to a compatible SQL data
        type.

        This method must be overridden.
        """
        pass

    @abc.abstractmethod
    def get_table(self, table_name: str) -> Optional[Dict[str, ColumnType]]:
        """
        Check if a table already exists for `table_name`.
        
        If it does, return a map of existing column names to their SQL data types.
        If it does not exist, return None.

        This method must be overridden.
        """
        pass

    @abc.abstractmethod
    def create_table(
        self, table_name: str, key_properties: List[str], column_definitions: Dict[str, ColumnType]
    ) -> None:
        """
        Create a table with the given name, columns, and key properties.
        
        This method must be overridden.
        """
        pass

    @abc.abstractmethod
    def add_column(self, table_name: str, column_name: str, type: ColumnType) -> None:
        """
        Add a column to an existing table.
        
        This method must be overridden.
        """
        pass

    @abc.abstractmethod
    def rename_column(self, table_name: str, old_name: str, new_name: str) -> None:
        """
        Rename an existing column on the table.
        
        This method must be overridden if the default versioning behavior is used.
        """
        pass
