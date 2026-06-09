#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
TNSHistorian2

A VOLTTRON historian that properly leverages BaseHistorian features to store TNS data records.
The historian subscribes to TNS topics and stores the complete record data in the value field of the historian.
"""
__version__ = "3.0.0"

import logging
import json

from importlib.metadata import distribution, PackageNotFoundError
from tent.data_manager.local_data_manager import LocalDataManager

# Dynamic imports for VOLTTRON compatibility (old vs new modular version)
try:
    distribution('volttron-core')
    # New modular VOLTTRON imports
    from historian.base.base_historian import BaseHistorian
    from volttron.client.logs import setup_logging
    from volttron.client.vip.agent import RPC
    from volttron.client.messaging import headers as headers_mod
    from volttron.utils import vip_main, load_config
except PackageNotFoundError:
    # Old VOLTTRON imports
    from volttron.platform.vip.agent import RPC, compat
    from volttron.platform.agent.base_historian import BaseHistorian
    from volttron.platform.messaging import topics, headers as headers_mod
    from volttron.platform.agent.utils import setup_logging, vip_main, load_config


setup_logging()
_log = logging.getLogger(__name__)

def historian(config_path, **kwargs):
    """
    Parse the config file and initialize the historian with it.
    """
    # Note: schema is not part of the historian configuration because it is only passed to LocalDataManager.
    config = load_config(config_path) or {
        "connection": {
            "type": "sqlite",
            "params": {"url": "sqlite:///historian_data.sqlite"}
        },
        "topic_replace_list": [],
        "custom_topics": {"capture_record_data": ["TNS/"]}  # Correct format
    }
    connection = config.get("connection")
    if "connection" in config:
        del config["connection"]
    kwargs.update(config)
    return TNSHistorian2(connection=connection, **kwargs)


class TNSHistorian2(BaseHistorian):
    """
    Historian implementation that extends the BaseHistorian for TNS records.
    It subscribes to TNS topics and stores the complete record data in the value field.
    """

    def __init__(self, connection, **kwargs):
        _log.info("TNSHistorian2 initialization started")
        self.connection = connection or {}
        # Track which tables are automatically stored.
        self.tracked_tables = set()
        self.unknown_table_initialized = False

        # Initialize BaseHistorian – it handles subscriptions and caching.
        super(TNSHistorian2, self).__init__(**kwargs)

        self.data_manager = self._create_data_manager()

    def _create_data_manager(self):
        """Create the TENT-owned local archive from historian connection settings."""
        try:
            data_manager = LocalDataManager(self, self.connection.get("params"), 100)
            _log.info("Successfully created TENT data manager")
            return data_manager
        except Exception as e:
            _log.error(f"Failed to create TENT data manager: {e}")
            return None

    def historian_setup(self):
        """Called at startup and after configuration changes."""
        _log.info("Setting up TNSHistorian2")
        if self.data_manager is None:
            self.data_manager = self._create_data_manager()
            if self.data_manager is None:
                return
        try:
            self.data_manager.init_archive()
            _log.info("Successfully initialized TENT data manager")
        except Exception as e:
            _log.error(f"Failed to initialize TENT data manager: {e}")

    def _ensure_table_exists(self, table_name, table_columns):
        """
        Ensure the table exists in the TENT data manager.
        """
        _log.debug(f"Ensuring table {table_name} exists")
        if not self.data_manager.has_table(table_name):
            _log.info(f"Creating table {table_name}")
            table_def = {"name": table_name, "columns": table_columns}
            self.data_manager.register_tables([table_def])
        else:
            _log.debug(f"Table {table_name} already exists")

    def _ensure_unknown_table_exists(self):
        """
        Ensure the unknown_table_records table exists in the data manager and the database.
        """
        if self.unknown_table_initialized:
            return
        unknown_table_def = {
            "name": "unknown_table_records",
            "columns": [
                {"name": "id", "type": "Integer", "kwargs": {"primary_key": True, "autoincrement": True}},
                {"name": "original_table", "type": "String"},
                {"name": "data", "type": "JSON"},
                {"name": "timestamp", "type": "DateTime"}
            ]
        }
        self.data_manager.register_tables([unknown_table_def])
        self.unknown_table_initialized = True

    @RPC.export
    def register_table(self, table_name, table_columns):
        """
        RPC method to register a table for automatic storage.
        """
        _log.info(f"Registering table: {table_name}")
        if not table_name or not isinstance(table_columns, list):
            return {"status": "error", "message": "Invalid table name or columns"}
        if self.data_manager is None:
            self.data_manager = self._create_data_manager()
            if self.data_manager is None:
                return {"status": "error", "message": "Data manager initialization failed"}
        try:
            self._ensure_table_exists(table_name, table_columns)
            self.tracked_tables.add(table_name)
            _log.info(f"Table {table_name} registered. Tracked tables: {list(self.tracked_tables)}")
            return {"status": "success", "message": f"Table {table_name} registered", "tracked_tables": list(self.tracked_tables)}
        except Exception as e:
            _log.error(f"Error registering table: {e}", exc_info=True)
            return {"status": "error", "message": f"Exception: {str(e)}"}

    @RPC.export
    def create_tables(self, message):
        try:
            payload = json.loads(message) if isinstance(message, str) else message
            if isinstance(payload, list):
                definitions = payload
            elif isinstance(payload, dict) and "definitions" in payload:
                definitions = payload["definitions"]
                definitions = json.loads(definitions) if isinstance(definitions, str) else definitions
            elif isinstance(payload, dict):
                # VolttronDataManager sends its definitions mapping directly.
                definitions = list(payload.values())
            else:
                raise ValueError("Table definitions must be a list or mapping")

            if not isinstance(definitions, list):
                raise ValueError("Table definitions must resolve to a list")
        except Exception as e:
            _log.error("Failed to parse create_tables message: %s", e)
            return {"status": "error", "message": "Failed to parse message"}

        if self.data_manager is None:
            self.data_manager = self._create_data_manager()
            if self.data_manager is None:
                return {"status": "error", "message": "Data manager initialization failed"}

        try:
            self.data_manager.register_tables(definitions)
            self.tracked_tables.update(definition["name"] for definition in definitions)
        except Exception as e:
            _log.error("Failed to register tables with the TENT data manager: %s", e, exc_info=True)
            return {"status": "error", "message": f"Failed to register tables: {e}"}

        _log.info("Tables have been registered with the TENT data manager.")
        return {"status": "success", "message": "Tables created successfully."}

    def publish_to_historian(self, to_publish_list):
        """
        Main publish method for TNSHistorian2.
        """
        _log.debug(f"Processing {len(to_publish_list)} records")
        if not to_publish_list:
            return
        try:
            records_to_store = []
            unknown_records_to_store = []
            _log.info(f"Tracked tables: {self.tracked_tables}")
            for record in to_publish_list:
                _log.info(f"Processing record: {record}")
                topic = record.get('topic', '')
                if topic.startswith('TNS/'):
                    _log.info("Found TNS topic")
                    if 'value' in record:
                        value = record['value']
                    elif record.get('readings'):
                        timestamp, value = record['readings'][0]
                    else:
                        _log.warning("No value or readings found in record")
                        continue
                    if isinstance(value, dict):
                        table_name = value.get('table_name')
                        if table_name and table_name in self.tracked_tables:
                            data = value.get('data', {}).copy()
                            if isinstance(data.get('timestamp'), str):
                                try:
                                    from datetime import datetime
                                    import pytz
                                    dt = datetime.fromisoformat(data['timestamp'].replace('Z', '+00:00'))
                                    if dt.tzinfo is None:
                                        dt = pytz.UTC.localize(dt)
                                    data['timestamp'] = dt
                                except ValueError as e:
                                    _log.error(f"Timestamp parse error: {data['timestamp']} - {e}")
                            # Table schema was already created via register_table RPC call
                            # Don't re-infer schema from data as it won't have primary key info
                            records_to_store.append({'table_name': table_name, 'data': data})
                        else:
                            _log.info(f"Table {table_name} not tracked, storing in unknown_table_records")
                            self._ensure_unknown_table_exists()
                            import datetime as dtmod
                            unknown_data = {
                                "original_table": table_name or "unknown",
                                "data": value.get('data', value),
                                "timestamp": dtmod.datetime.utcnow()
                            }
                            unknown_records_to_store.append({'table_name': "unknown_table_records", 'data': unknown_data})
                    else:
                        _log.warning(f"Value is not a dict: {type(value)}")
                else:
                    _log.debug(f"Skipping non-TNS topic: {topic}")
            _log.info(f"Records to store: {len(records_to_store)}")
            _log.info(f"Unknown records to store: {len(unknown_records_to_store)}")
            if records_to_store:
                try:
                    _log.info("Archiving records...")
                    self.data_manager.archive_data(records_to_store)
                    _log.info("Records archived successfully")
                except Exception as e:
                    _log.error(f"Error storing records: {e}", exc_info=True)
            if unknown_records_to_store:
                try:
                    _log.info("Archiving unknown records...")
                    self.data_manager.archive_data(unknown_records_to_store)
                    _log.info("Unknown records archived successfully")
                except Exception as e:
                    _log.error(f"Error storing unknown records: {e}", exc_info=True)
            if records_to_store or unknown_records_to_store:
                self.report_handled(to_publish_list)
            else:
                _log.info("No records to store")
        except Exception as e:
            _log.error(f"Error in publish_to_historian: {e}", exc_info=True)

    def version(self):
        """Return the version of the historian."""
        return __version__

    def query_historian(self, topic, start=None, end=None, agg_type=None, agg_period=None, skip=0, count=None, order="FIRST_TO_LAST"):
        """Query the historian for data."""
        # This historian stores TNS data in custom tables, not the standard historian format
        # Return empty result for now
        return {}

    def query_topic_list(self):
        """Return list of topics in the historian."""
        # TNS topics are stored in custom tables
        return []

    def query_topics_by_pattern(self, topic_pattern):
        """Query topics matching a pattern."""
        return []

    def query_aggregate_topics(self):
        """Return aggregate topics."""
        return []

    def query_topics_metadata(self, topics):
        """Return metadata for topics."""
        return {}


def main():
    try:
        vip_main(historian, version=__version__)
    except Exception as e:
        _log.error(f"Historian error: {e}", exc_info=True)
        raise


if __name__ == '__main__':
    main()
