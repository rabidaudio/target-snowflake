import logging
import os
import pytest

from target_snowflake.target import SnowflakeTarget, Connection


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
def db_connection(db_config: dict) -> Connection:
    conn = Connection(logger=logging.getLogger(), **db_config)
    try:
        conn.execute("CREATE SCHEMA IF NOT EXISTS TEST_SCHEMA")
        yield conn
    finally:
        conn.execute("DROP SCHEMA TEST_SCHEMA CASCADE")
        conn.close()


@pytest.fixture
def snowflake_target(db_config: dict, db_connection: Connection) -> SnowflakeTarget:
    yield SnowflakeTarget(config={"snowflake": {"schema": "TEST_SCHEMA", **db_config}})
