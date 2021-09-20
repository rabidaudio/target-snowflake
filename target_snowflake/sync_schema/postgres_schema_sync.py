from typing import Any, Dict, Optional, List

import psycopg2
from psycopg2.extras import DictCursor
from psycopg2.sql import SQL, Identifier

from target_snowflake.sync_schema import BaseSchemaSync


class PostgresSchemaSync(BaseSchemaSync):
    def __init__(
        self,
        connection: psycopg2.extensions.connection,
        schema: str,
        config: Optional[Dict[str, Any]] = None,
        parse_env_config: bool = False,
    ) -> None:
        super().__init__(config=config, parse_env_config=parse_env_config)
        self.connection = connection
        self.schema = schema

    # def sanitize_name(self, name: str) -> str:
    #   return f'"{name.lower()}"'

    def get_table(self, table_name: str) -> Optional[Dict[str, str]]:
        res = self.query(
            "SELECT column_name, data_type FROM information_schema.columns "
            + "WHERE lower(table_schema) = %s and lower(table_name) = %s",
            (self.schema, table_name),
        )
        if not res:
            return None
        return {column["column_name"]: column["data_type"] for column in res}

    def create_table(
        self, table_name: str, key_properties: List[str], schema: Dict[str, str]
    ) -> None:
        sql = "CREATE TABLE {}.{} ("
        args = [Identifier(self.schema), Identifier(table_name)]
        columns = []
        for name, type in schema.items():
            columns.append("{} " + type)
            args.append(Identifier(name))
        primary_keys = []
        for kp in key_properties:
            primary_keys.append("{}")
            args.append(Identifier(kp))
        columns.append("PRIMARY KEY (" + ", ".join(primary_keys) + ")")
        sql += ", ".join(columns)
        sql += ")"

        self.execute(SQL(sql).format(*args))

    def add_column(self, table_name: str, column_name: str, type: str) -> None:
        self.execute(
            SQL("ALTER TABLE {}.{} ADD COLUMN {} " + type).format(
                Identifier(self.schema), Identifier(table_name), Identifier(column_name)
            )
        )

    def rename_column(self, table_name: str, old_name: str, new_name: str) -> None:
        self.execute(
            SQL("ALTER TABLE {}.{} RENAME COLUMN {} to {}").format(
                Identifier(self.schema),
                Identifier(table_name),
                Identifier(old_name),
                Identifier(new_name),
            )
        )

    def jsonschema_to_sql_type(self, jsonschema: dict) -> str:
        if "array" in jsonschema["type"] or "object" in jsonschema["type"]:
            return "jsonb"
        if (
            "format" in jsonschema
            and jsonschema["format"] == "date-time"
            and "string" in jsonschema["type"]
        ):
            return "timestamp with time zone"
        if (
            "format" in jsonschema
            and jsonschema["format"] == "date"
            and "string" in jsonschema["type"]
        ):
            return "date"
        if "string" in jsonschema["type"]:
            return "character varying"
        if "number" in jsonschema["type"]:
            return "numeric"
        if "integer" in jsonschema["type"]:
            return "bigint"
        if "boolean" in jsonschema["type"]:
            return "boolean"

        return "character varying"

    def execute(self, sql: str, *args) -> None:
        with self.connection.cursor() as cur:
            self.logger.info(sql)
            cur.execute(sql, *args)

    def query(self, sql: str, *args) -> List[Dict[str, Any]]:
        with self.connection.cursor(cursor_factory=DictCursor) as cur:
            self.logger.info(sql)
            cur.execute(sql, *args)
            return cur.fetchall()
