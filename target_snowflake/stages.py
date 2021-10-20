from abc import abstractmethod
from types import MappingProxyType
from typing import Any, Mapping

from singer_sdk.target_base import Target


class Stage:
    def __init__(self, target: Target) -> None:
        self.connection = target.connect()
        self.logger = target.logger
        self._config = dict(target.config)

    @property
    def config(self) -> Mapping[str, Any]:
        """Get plugin configuration.

        Returns:
            A frozen (read-only) config dictionary map.
        """
        return MappingProxyType(self._config)

    @abstractmethod
    def prepare(self):
        pass

    @abstractmethod
    def load(self, local_csv_file: str) -> None:
        pass

    @abstractmethod
    def cleanup(self):
        pass


# Table/User stage implementation is probably very similar (but perhaps unnecessary?)
# External stage: depends on storage implementation
# NOTE: you don't want to do non-stage replication, as Snowflake has a 16K line limit for
# expressions in queries: https://community.snowflake.com/s/article/maxi-expressions-exceeded


class NamedStage(Stage):
    """Stage implementation for internal named stages."""

    @property
    def stage_name(self):
        """
        The name of the stage.

        The user needs to have rights to create the stage
        if it doesn't exist and to add and remove files from it.
        """
        return self.config["stage"]

    @property
    def purge_stage_on_complete(self) -> bool:
        """Return `True` to"""

    def prepare(self):
        pass
        # self.connection.execute("""
        # CREATE OR REPLACE STAGE "{}"
        # FILE_FORMAT = (TYPE = 'CSV' )
        # """)

    def cleanup(self):
        # COPY INTO...
        # (optional) REMOVE
        pass
