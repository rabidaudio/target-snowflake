import os
import psycopg2
import pytest

from freezegun import freeze_time

from target_snowflake.sync_schema.postgres_schema_sync import PostgresSchemaSync

@pytest.fixture
def db() -> psycopg2.extensions.connection:
  conn = psycopg2.connect(
    dbname=os.getenv('TEST_DATABASE_DBNAME', 'test-target-snowflake'),
    user=os.getenv('TEST_DATABASE_USER'),
    password=os.getenv('TEST_DATABASE_PASSWORD'),
    host=os.getenv('TEST_DATABASE_HOST'),
  )
  conn.cursor().execute("CREATE SCHEMA IF NOT EXISTS test_schema")
  yield conn
  conn.cursor().execute("DROP SCHEMA test_schema CASCADE")

@pytest.fixture
def schema_sync(db):
  yield PostgresSchemaSync(db, schema="test_schema")

def test_create_table(db: psycopg2.extensions.connection, schema_sync: PostgresSchemaSync):
  schema = schema_sync.sync_table(
    stream_name="users", 
    key_properties=["id"],
    jsonschema={
      "type": "object",
      "properties": {
        "id": { "type": "integer" },
        "name": { "type": ["null", "string"] },
      }
    }
  )

  assert schema['id'] == 'bigint'
  assert schema['name'] == 'character varying'
  
  with db.cursor() as cur:
    cur.execute("SELECT 1 FROM information_schema.tables where table_schema='test_schema' and table_name='users' LIMIT 1")
    res = cur.fetchall()
    assert len(res) == 1

def test_add_column(db: psycopg2.extensions.connection, schema_sync: PostgresSchemaSync):
  with db.cursor() as cur:
    cur.execute("CREATE TABLE test_schema.users (id bigint, name character varying, PRIMARY KEY (id))")

  schema = schema_sync.sync_table(
    stream_name="users", 
    key_properties=["id"],
    jsonschema={
      "type": "object",
      "properties": {
        "id": { "type": "integer" },
        "name": { "type": ["null", "string"] },
        "email": { "type": ["null", "string"] },
      }
    }
  )

  assert schema['email'] == 'character varying'

  with db.cursor() as cur:
    cur.execute("SELECT 1 FROM information_schema.columns " +
      "where table_schema='test_schema' and table_name='users' and column_name='email' and data_type='character varying' LIMIT 1")
    res = cur.fetchall()
    assert len(res) == 1

@freeze_time("2021-09-20 12:45", tz_offset=-5)
def test_alter_column(db: psycopg2.extensions.connection, schema_sync: PostgresSchemaSync):
  with db.cursor() as cur:
    cur.execute("CREATE TABLE test_schema.users (id bigint, age bigint, PRIMARY KEY (id))")

  schema = schema_sync.sync_table(
    stream_name="users", 
    key_properties=["id"],
    jsonschema={
      "type": "object",
      "properties": {
        "id": { "type": "integer" },
        "age": { "type": ["null", "number"] },
      }
    }
  )

  assert schema['age'] == 'numeric'

  with db.cursor() as cur:
    cur.execute("SELECT 1 FROM information_schema.columns " +
      "where table_schema='test_schema' and table_name='users' and column_name='age' and data_type='numeric' LIMIT 1")
    res = cur.fetchall()
    assert len(res) == 1

  with db.cursor() as cur:
    cur.execute("SELECT 1 FROM information_schema.columns " +
      "where table_schema='test_schema' and table_name='users' and column_name='age_20210920_0745' and data_type='bigint' LIMIT 1")
    res = cur.fetchall()
    assert len(res) == 1

def test_data_types(db: psycopg2.extensions.connection, schema_sync: PostgresSchemaSync):

  schema = schema_sync.sync_table(
    stream_name="users", 
    key_properties=["id"],
    jsonschema={
      "type": "object",
      "properties": {
        "id": { "type": "integer" },
        "optional_string": { "type": ["null", "string"] },
        "string": { "type": "string" },
        "boolean": { "type": ["null", "boolean"] },
        "integer": { "type": ["null", "integer"] },
        "date": { "type": ["null", "string"], "format": "date" },
        "datetime": { "type": ["null", "string"], "format": "date-time" },
        "primitive_array": { "type": "array", "items": { "type": "integer" } },
        "nested_object": {
          "type": "object",
          "properties": {
            "name": { "type": ["null", "string"] },
          }
        },
        "object_array": {
          "type": "array",
          "items": {
            "type": "object",
            "properties": {
              "name": { "type": ["null", "string"] },
            }
          }
        },
        "uncertain_type": { "type": ["string", "integer"] },
      }
    }
  )

  assert schema['optional_string'] == 'character varying' # Note: no "NOT NULL"
  assert schema['string'] == 'character varying'
  assert schema['boolean'] == 'boolean'
  assert schema['integer'] == 'bigint'
  assert schema['date'] == 'date'
  assert schema['datetime'] == 'timestamp with time zone'
  assert schema['primitive_array'] == 'jsonb'
  assert schema['nested_object'] == 'jsonb'
  assert schema['object_array'] == 'jsonb'
  assert schema['uncertain_type'] == 'character varying'
