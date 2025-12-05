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
import os
import sqlite3
from sqlalchemy.orm import Session

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
        # Save connection info before calling parent constructor.
        self.connection = connection
        # Track which tables are automatically stored.
        self.tracked_tables = set()
        self.unknown_table_initialized = False

        # Ensure SQLite connection parameters include a URL if using SQLite.
        if self.connection.get("type", "").lower() == "sqlite":
            db_params = self.connection.get("params", {})
            # NOTE: The URL in the config is preferred. If the directory for the database cannot be created,
            # or if the URL is missing, a fallback path in the agent's working directory will be used instead.
            # This ensures the historian always has a writable database location, but may confuse users
            # if the config URL is not respected due to directory issues.
            if "url" in db_params and db_params["url"].startswith("sqlite:///"):
                db_path = db_params["url"].replace("sqlite:///", "")
                if db_path.startswith("/"):
                    db_dir = os.path.dirname(db_path)
                    if not os.path.exists(db_dir):
                        try:
                            _log.info(f"Creating directory for database: {db_dir}")
                            os.makedirs(db_dir, exist_ok=True)
                        except Exception as e:
                            _log.warning(f"Cannot create directory: {e}")
                            agent_data_dir = self._get_agent_data_dir()
                            new_db_path = os.path.join(agent_data_dir, "historian_data.sqlite")
                            _log.info(f"Using fallback database path: {new_db_path}")
                            db_params["url"] = f"sqlite:///{new_db_path}"
            if "url" not in db_params:
                agent_data_dir = self._get_agent_data_dir()
                db_path = os.path.join(agent_data_dir, "historian_data.sqlite")
                db_params["url"] = f"sqlite:///{db_path}"
            self.connection["params"] = db_params
            _log.info(f"Using SQLite connection parameters: {db_params}")

        # Initialize BaseHistorian – it handles subscriptions and caching.
        super(TNSHistorian2, self).__init__(**kwargs)

        # Initialize the TENTS data manager after BaseHistorian is set up.
        # We pass schema "tns" to LocalDataManager.
        try:
            self.data_manager = LocalDataManager(
                None,  # transactive_node
                self.connection["params"],
                100
            )
            _log.info("Successfully created TENTS data manager")
        except Exception as e:
            _log.error(f"Failed to create TENTS data manager: {e}")
            self.data_manager = None

    def _get_agent_data_dir(self):
        """
        Get (and create if necessary) the agent's data directory.
        """
        current_dir = os.getcwd()
        agent_name = os.path.basename(current_dir)
        agent_data_dir = os.path.join(current_dir, f"{agent_name}.agent-data")
        if not os.path.exists(agent_data_dir):
            try:
                os.makedirs(agent_data_dir, exist_ok=True)
            except Exception as e:
                _log.warning(f"Cannot create agent data directory: {e}")
                agent_data_dir = current_dir
        return agent_data_dir

    def historian_setup(self):
        """Called at startup and after configuration changes."""
        _log.info("Setting up TNSHistorian2")
        if self.data_manager is None:
            try:
                self.data_manager = LocalDataManager(
                    None,
                    self.connection["params"],
                    100
                )
            except Exception as e:
                _log.error(f"Failed to initialize TENTS data manager: {e}")
                return
        try:
            self.data_manager.init_archive()
            _log.info("Successfully initialized TENTS data manager")
        except Exception as e:
            _log.error(f"Failed to initialize TENTS data manager: {e}")

    def _ensure_table_exists(self, table_name, table_columns):
        """
        Ensure the table exists in the TENTS data manager.
        """
        _log.debug(f"Ensuring table {table_name} exists")
        if not hasattr(self.data_manager, 'orm') or table_name not in self.data_manager.orm:
            _log.info(f"Creating table {table_name}")
            table_def = {"name": table_name, "columns": table_columns}
            self.data_manager.add_tables([table_def])
            self.data_manager.init_archive()
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
        self.data_manager.add_tables([unknown_table_def])
        self.data_manager.init_archive()
        # Explicitly create the table in the database
        self.data_manager.registry.metadata.create_all(self.data_manager.engine, checkfirst=True)
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
            try:
                _log.info(f"Initializing data manager with params: {self.connection['params']}")
                self.data_manager = LocalDataManager(
                    None,
                    self.connection["params"],
                    100
                )
                self.data_manager.init_archive()
            except Exception as e:
                _log.error(f"Failed to initialize data manager: {e}")
                return {"status": "error", "message": f"Data manager init failed: {str(e)}"}
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
            # Parse the message.
            if isinstance(message, str):
                payload = json.loads(message)
            else:
                payload = message
            definitions_str = payload.get("definitions", "[]")
            # schema_from_msg = payload.get("schema")  # Schema-related processing commented out
            # if schema_from_msg:
            #     self.schema = schema_from_msg
            definitions = json.loads(definitions_str)
        except Exception as e:
            _log.error("Failed to parse create_tables message: %s", e)
            return {"status": "error", "message": "Failed to parse message"}
        # _log.info("Creating tables with schema: %s", self.schema)  # Schema-related logging commented out
        self.data_manager.add_tables(definitions)
        self.data_manager.registry.metadata.create_all(self.data_manager.engine, checkfirst=True)
        _log.info("Tables have been created and committed to the database.")
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
