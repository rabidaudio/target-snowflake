"""Database table creator and migrator."""

from typing import Dict, Optional, List

from target_snowflake.database_target.schema_migrator import SchemaMigrator, ColumnType


class SnowflakeSchemaMigrator(SchemaMigrator):
    @property
    def table_schema(self):
        return self.sink.table_schema

    @property
    def connection(self):
        return self.sink.connection

    def get_table(self, table_name: str) -> Optional[Dict[str, ColumnType]]:
        res = self.connection.query(
            [
                'SHOW COLUMNS IN SCHEMA "{}"."{}";'.format(
                    self.config["snowflake"]["database"],
                    self.table_schema,
                ),
                """
            SELECT "column_name" AS column_name,
                    CASE PARSE_JSON("data_type"):type::varchar
                        WHEN 'FIXED' THEN 'NUMBER'
                        WHEN 'REAL'  THEN 'FLOAT'
                        ELSE PARSE_JSON("data_type"):type::varchar
                    END data_type
            FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
            WHERE "table_name" = %(table_name)s;
            """,
            ],
            table_name=table_name,
        )
        if not res:
            return None
        return {column["COLUMN_NAME"]: column["DATA_TYPE"] for column in res}

    def create_table(
        self,
        table_name: str,
        key_properties: List[str],
        column_definitions: Dict[str, ColumnType],
    ) -> None:
        sql = 'CREATE TABLE "{}"."{}" ('.format(self.table_schema, table_name)
        columns = []
        for name, type in column_definitions.items():
            columns.append('"{}" {}'.format(name, type))
        columns.append(
            "PRIMARY KEY (" + ", ".join([kp.upper() for kp in key_properties]) + ")"
        )
        sql += ", ".join(columns)
        sql += ")"

        self.connection.execute(sql)

    def convert_stream_name_to_table_name(self, stream_name: str) -> str:
        return stream_name.upper()

    def convert_property_name_to_column_name(self, column_name: str) -> str:
        return column_name.upper()

    def add_column(self, table_name: str, column_name: str, type: ColumnType) -> None:
        self.connection.execute(
            'ALTER TABLE "{}"."{}" ADD COLUMN "{}" {}'.format(
                self.table_schema, table_name, column_name, type
            )
        )

    def rename_column(self, table_name: str, old_name: str, new_name: str) -> None:
        self.connection.execute(
            'ALTER TABLE "{}"."{}" RENAME COLUMN "{}" to "{}"'.format(
                self.table_schema,
                table_name,
                old_name,
                new_name,
            )
        )

    def convert_jsonschema_to_sql_type(self, property_schema: dict) -> str:
        # https://docs.snowflake.com/en/sql-reference/intro-summary-data-types.html
        # Note: ignores "null" types and assumes all types are nullable
        if "array" in property_schema["type"]:
            return "ARRAY"
        if "object" in property_schema["type"]:
            return "VARIANT"
        if (
            "format" in property_schema
            and property_schema["format"] == "date-time"
            and "string" in property_schema["type"]
        ):
            return "TIMESTAMP_TZ"
        if (
            "format" in property_schema
            and property_schema["format"] == "date"
            and "string" in property_schema["type"]
        ):
            return "DATE"
        if "string" in property_schema["type"]:
            return "TEXT"  # equivalent to VARCHAR(16777216)
        if "number" in property_schema["type"]:
            return "FLOAT"  # equivalent to REAL, arbitrary precision
        if "integer" in property_schema["type"]:
            return "NUMBER"  # defaults to NUMBER(38, 0), equivalent to BIGINT
        if "boolean" in property_schema["type"]:
            return "BOOLEAN"

        return "TEXT"
