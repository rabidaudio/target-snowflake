from freezegun import freeze_time
from snowflake.connector import SnowflakeConnection

from target_snowflake.snowflake_target import SnowflakeTarget
from target_snowflake.snowflake_migrator import SnowflakeSchemaMigrator


def test_create_table(
    db_connection: SnowflakeConnection, snowflake_target: SnowflakeTarget
):
    migrator = SnowflakeSchemaMigrator(
        target=snowflake_target,
        stream_name="users",
        key_properties=["id"],
        schema={
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "name": {"type": ["null", "string"]},
            },
        },
    )
    column_defs = migrator.sync_table()

    assert column_defs["ID"] == "NUMBER"
    assert column_defs["NAME"] == "TEXT"

    with db_connection.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM INFORMATION_SCHEMA.TABLES "
            + "where TABLE_SCHEMA='TEST_SCHEMA' and TABLE_NAME='USERS' LIMIT 1"
        )
        res = cur.fetchall()
        assert len(res) == 1


def test_add_column(
    db_connection: SnowflakeConnection, snowflake_target: SnowflakeTarget
):
    with db_connection.cursor() as cur:
        cur.execute(
            "CREATE TABLE TEST_SCHEMA.USERS "
            + "(ID NUMBER, NAME TEXT, PRIMARY KEY (ID))"
        )

    migrator = SnowflakeSchemaMigrator(
        target=snowflake_target,
        stream_name="users",
        key_properties=["id"],
        schema={
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "name": {"type": ["null", "string"]},
                "email": {"type": ["null", "string"]},
            },
        },
    )
    column_defs = migrator.sync_table()

    assert column_defs["EMAIL"] == "TEXT"

    with db_connection.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS "
            + "WHERE TABLE_SCHEMA='TEST_SCHEMA' AND TABLE_NAME='USERS' "
            + "AND COLUMN_NAME='EMAIL' AND DATA_TYPE='TEXT' LIMIT 1"
        )
        res = cur.fetchall()
        assert len(res) == 1


@freeze_time("2021-09-20 12:45", tz_offset=-5)
def test_alter_column(
    db_connection: SnowflakeConnection, snowflake_target: SnowflakeTarget
):
    with db_connection.cursor() as cur:
        cur.execute(
            "CREATE TABLE TEST_SCHEMA.USERS (ID NUMBER, AGE NUMBER, PRIMARY KEY (ID))"
        )

    migrator = SnowflakeSchemaMigrator(
        target=snowflake_target,
        stream_name="users",
        key_properties=["id"],
        schema={
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "age": {"type": ["null", "number"]},
            },
        },
    )
    column_defs = migrator.sync_table()

    assert column_defs["AGE"] == "FLOAT"

    with db_connection.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS "
            + "WHERE TABLE_SCHEMA='TEST_SCHEMA' AND TABLE_NAME='USERS' "
            + "AND COLUMN_NAME='AGE' AND DATA_TYPE='FLOAT' LIMIT 1"
        )
        res = cur.fetchall()
        assert len(res) == 1

    with db_connection.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS "
            + "WHERE TABLE_SCHEMA='TEST_SCHEMA' AND TABLE_NAME='USERS' "
            + "AND COLUMN_NAME='AGE_20210920_0745' AND DATA_TYPE='NUMBER' LIMIT 1"
        )
        res = cur.fetchall()
        assert len(res) == 1


def test_data_types(
    db_connection: SnowflakeConnection, snowflake_target: SnowflakeTarget
):

    migrator = SnowflakeSchemaMigrator(
        target=snowflake_target,
        stream_name="users",
        key_properties=["id"],
        schema={
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "optional_string": {"type": ["null", "string"]},
                "string": {"type": "string"},
                "boolean": {"type": ["null", "boolean"]},
                "integer": {"type": ["null", "integer"]},
                "decimal": {"type": ["null", "number"], "format": "float"},
                "date": {"type": ["null", "string"], "format": "date"},
                "datetime": {"type": ["null", "string"], "format": "date-time"},
                "primitive_array": {"type": "array", "items": {"type": "integer"}},
                "nested_object": {
                    "type": "object",
                    "properties": {
                        "name": {"type": ["null", "string"]},
                    },
                },
                "object_array": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "name": {"type": ["null", "string"]},
                        },
                    },
                },
                "uncertain_type": {"type": ["string", "integer"]},
            },
        },
    )
    column_defs = migrator.sync_table()

    assert column_defs["OPTIONAL_STRING"] == "TEXT"  # NOTE: NO 'NOT NULL'
    assert column_defs["STRING"] == "TEXT"
    assert column_defs["BOOLEAN"] == "BOOLEAN"
    assert column_defs["INTEGER"] == "NUMBER"
    assert column_defs["DECIMAL"] == "FLOAT"
    assert column_defs["DATE"] == "DATE"
    assert column_defs["DATETIME"] == "TIMESTAMP_TZ"
    assert column_defs["PRIMITIVE_ARRAY"] == "ARRAY"
    assert column_defs["NESTED_OBJECT"] == "VARIANT"
    assert column_defs["OBJECT_ARRAY"] == "ARRAY"
    assert column_defs["UNCERTAIN_TYPE"] == "TEXT"
