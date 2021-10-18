from typing import Any, Dict, Optional, List, Union

import snowflake.connector

from target_snowflake.sinks import SnowflakeSink
from singer_sdk import Target

class SnowflakeTarget(Target):

    name = 'SnowflakeTarget'
    default_sink_class = SnowflakeSink

    def __init__(
        self,
        config: Optional[Dict[str, Any]] = None,
        parse_env_config: bool = False,
    ) -> None:
        super().__init__(config=config, parse_env_config=parse_env_config)
        self.connection = snowflake.connector.connect(**self.config["snowflake"])
        self.table_schema = self.config.get("schema", "PUBLIC").upper()
        # create the schema if not exists
        # create/update the stage if necessary

    def execute(self, sql: str, *args) -> None:
        with self.connection.cursor() as cur:
            self.logger.debug(sql)
            cur.execute(sql, *args)

    def query(self, sql: Union[str, List[str]], **kwargs) -> List[Dict[str, Any]]:
        with self.connection.cursor(snowflake.connector.DictCursor) as cur:
            is_transaction = False
            if isinstance(sql, list):
                self.logger.debug("START TRANSACTION")
                cur.execute("START TRANSACTION")
                is_transaction = True
            else:
                sql = [sql]
            for i, query in enumerate(sql):
                self.logger.debug(query)
                cur.execute(query, kwargs)
                result = cur.fetchall()
            if is_transaction:
                cur.execute("COMMIT")
            return result
