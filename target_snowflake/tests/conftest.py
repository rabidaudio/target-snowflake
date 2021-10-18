import os
import pytest

import snowflake.connector
from snowflake.connector import SnowflakeConnection

from target_snowflake.snowflake_target import SnowflakeTarget


@pytest.fixture
def db_config() -> dict:
    try:
        return {
            "account": os.environ["TEST_DATABASE_ACCOUNT"],
            "user": os.getenv("TEST_DATABASE_USER"),
            "password": os.getenv("TEST_DATABASE_PASSWORD"),
            "database": os.getenv("TEST_DATABASE_DBNAME", "test-target-snowflake"),
            "role": os.getenv("TEST_DATABASE_ROLE"),
            "warehouse": os.getenv("TEST_DATABASE_WAREHOUSE"),
        }
    except KeyError as e:
        raise Exception(
            "Set TEST_DATABASE_* environment variables in order to run the test suite"
        ) from e


@pytest.fixture
def db_connection(db_config: dict) -> SnowflakeConnection:
    conn = snowflake.connector.connect(**db_config)
    conn.cursor().execute("CREATE SCHEMA IF NOT EXISTS TEST_SCHEMA")
    yield conn
    conn.cursor().execute("DROP SCHEMA TEST_SCHEMA CASCADE")


@pytest.fixture
def snowflake_target(
    db_config: dict, db_connection: SnowflakeConnection
) -> SnowflakeTarget:
    yield SnowflakeTarget(config={"snowflake": db_config, "schema": "TEST_SCHEMA"})
