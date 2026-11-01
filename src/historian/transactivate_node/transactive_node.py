#!/usr/bin/env python
"""
A modular VOLTTRON historian backed by TENT's LocalDataManager.

It serves two kinds of record through one BaseHistorian subscription set:

- standard VOLTTRON records (`devices/`, `analysis/`, `datalogger/`, `record/`), stored in the standard `topics` and
  `data` tables and answered through the standard query RPCs (`query`, `get_topic_list`, `get_topics_by_pattern`,
  `get_topics_metadata`), so that the agent can be installed as the platform's historian;
- TENT archive envelopes, `{"table_name": ..., "data": {...}}` or lists of them, which a TransactiveNodeAgent's
  VolttronDataManager publishes on `<db_topic>/<node>/archive` after registering its table definitions with the
  `create_tables` RPC. Envelopes are stored in the registered archive tables.

Routing is by payload shape, not topic. All database handling is in `HistorianStore` (store.py); this module is the
VOLTTRON shell: configuration from the config store, RPC exports and the BaseHistorian publishing contract.

Identity: VOLTTRON historians are conventionally installed as `platform.historian` (`DEFAULT_VIP_IDENTITY`); the
modular platform assigns the identity at `vctl install --vip-identity`, so a deployment may choose another (the TENT
containers use `tns.historian`) as long as every node's `historian_vip` names the same one.

Aggregation (`agg_type`/`agg_period` on `query`, `get_aggregate_topics`) is not supported by this historian: there are
no aggregate tables, and an aggregate query raises rather than returning an empty result.
"""
import logging
import sys

from historian.base.base_historian import BaseHistorian
from volttron.client.known_identities import PLATFORM_HISTORIAN
from volttron.client.logs import setup_logging
from volttron.client.vip.agent import RPC
from volttron.utils import load_config, vip_main

from historian.transactivate_node.definitions import TableDefinitionMismatch, parse_definitions
from historian.transactivate_node.store import HistorianStore

__version__ = "3.0.0"

setup_logging()
_log = logging.getLogger(__name__)

DEFAULT_VIP_IDENTITY = PLATFORM_HISTORIAN

CONFIGURATION_HELP = ('Store the historian configuration as the config-store entry "config" of the historian\'s VIP '
                      'identity, e.g. vctl config store <identity> config tns-historian.json. It must contain '
                      '{"connection": {"params": {"url": "<SQLAlchemy URL>"}}}.')


def historian(config_path, **kwargs):
    """
    Build the historian. `config_path` is the `--agent-config` file when one was given at install time; its contents
    become the defaults for the config-store entry `config`, which is the normal place to configure the historian.
    """
    config = load_config(config_path) or {}
    connection = config.pop("connection", None)
    tables_def = config.pop("tables_def", None)
    table_definitions = config.pop("table_definitions", None)
    kwargs.update(config)
    return TransactiveNodeHistorian(
        connection=connection,
        tables_def=tables_def,
        table_definitions=table_definitions,
        **kwargs,
    )


class TransactiveNodeHistorian(BaseHistorian):
    """Store standard VOLTTRON records and registered TENT archive records in one SQLAlchemy database."""

    def __init__(self, connection=None, tables_def=None, table_definitions=None, **kwargs):
        # The RPC handlers (create_tables, queries) run on the agent's greenlets; the publishing loop must as well so
        # that the data manager is never touched from a second OS thread.
        if not kwargs.pop("process_loop_in_greenlet", True):
            raise ValueError("TransactiveNodeHistorian requires process_loop_in_greenlet=True (the default).")
        if connection is not None:
            HistorianStore.validate_connection(connection)  # fail at install time, not when the first record arrives
        self.connection = connection
        self.tables_def, self.table_names = self.parse_table_def(tables_def)
        self.configured_table_definitions = parse_definitions(table_definitions)
        self.store: HistorianStore | None = None
        super().__init__(process_loop_in_greenlet=True, **kwargs)
        self.update_default_config({
            "connection": connection,
            "tables_def": tables_def,
            "table_definitions": self.configured_table_definitions,
        })

    # ---------------------------------------------------------------- BaseHistorian contract
    def configure(self, configuration):
        """
        Called by BaseHistorian with the merged defaults and config-store entry `config`, with the processing loop
        stopped. (Re)builds the store from `connection`; `historian_setup` then opens the database on the loop.
        """
        connection = configuration.get("connection")
        if connection is None:
            raise ValueError(f'The historian has no "connection"; nothing will be stored. {CONFIGURATION_HELP}')
        self.tables_def, self.table_names = self.parse_table_def(configuration.get("tables_def"))
        self.configured_table_definitions = parse_definitions(configuration.get("table_definitions"))
        self.store = HistorianStore(connection, self.table_names)  # raises ValueError for a URL that is not SQLAlchemy's
        self.connection = connection
        _log.info(f"Historian database: {self.store.redacted_url}")

    def historian_setup(self):
        """Open the database, create the historian's tables and reload the registered table definitions."""
        if self.store is None:
            raise RuntimeError(f'The historian is not configured (see the configuration error logged above). '
                               f'{CONFIGURATION_HELP}')
        self.store.setup(self.configured_table_definitions)
        _log.info(f"Historian ready; archive tables: {sorted(self.store.writable_tables)}")

    def publish_to_historian(self, to_publish_list):
        """Persist one BaseHistorian batch. Every record is reported handled: rows that cannot be stored are logged."""
        if self.store is None or not self.store.ready:
            raise RuntimeError(f'The historian database is not open; {len(to_publish_list)} record(s) stay cached. '
                               f'{CONFIGURATION_HELP}')
        result = self.store.write(to_publish_list)
        if result.dropped:
            _log.error(f"Dropped {result.dropped} archive row(s) in this batch for unregistered tables: "
                       f"{dict(result.skipped_unknown_table)}.")
        self.report_all_handled()

    # ---------------------------------------------------------------- TENT archive registration
    @RPC.export
    def create_tables(self, definitions):
        """
        Register archive table definitions (the structure TENT's `get_archive_definitions` returns) and create the
        tables. Definitions are checked against an allow-list of keys, column types and column keywords, persisted in
        `known_table_definitions`, and compared with the stored definition when a table is registered again.

        Returns {"status": "success", "registered_tables": [names new in this call], "known_tables": [all]} or
        {"status": "error", "message": ..., "differences": {table: unified diff}} when a re-registration differs.

        This RPC runs DDL; the modular platform has no `RPC.allow` decorator, so protect it per deployment:
        `vctl authz add agent <historian identity> --protected-rpcs create_tables` and give each node
        `--rpc-capabilities <historian identity>.create_tables`.
        """
        if self.store is None or not self.store.ready:
            return {"status": "error", "message": f"The historian database is not open. {CONFIGURATION_HELP}"}
        try:
            parsed = parse_definitions(definitions)
            registered = self.store.register_definitions(parsed)
        except TableDefinitionMismatch as exc:
            _log.error(f"create_tables rejected: {exc}")
            return {"status": "error", "message": str(exc), "differences": exc.differences}
        except ValueError as exc:
            _log.error(f"create_tables rejected: {exc}")
            return {"status": "error", "message": str(exc)}
        return {
            "status": "success",
            "registered_tables": registered,
            "known_tables": sorted(self.store.writable_tables),
        }

    # ---------------------------------------------------------------- standard query contract
    def query_topic_list(self):
        return self.store.topic_list()

    def query_topics_by_pattern(self, topic_pattern):
        """`topic_pattern` is a case-insensitive shell-style glob (`devices/*/power`), not a regular expression."""
        return self.store.topics_by_pattern(topic_pattern)

    def query_topics_metadata(self, topics):
        topics = [topics] if isinstance(topics, str) else topics
        return self.store.topics_metadata(topics)

    def query_aggregate_topics(self):
        """This historian keeps no aggregate tables, so the list of aggregate topics is empty."""
        return []

    def query_historian(self, topic, start=None, end=None, agg_type=None, agg_period=None,
                        skip=0, count=None, order="FIRST_TO_LAST"):
        """Raw values only; aggregation is not implemented and is refused rather than answered with nothing."""
        if agg_type or agg_period:
            raise NotImplementedError("The transactive-node historian does not support aggregate queries "
                                      "(agg_type/agg_period); query the raw topic instead.")
        topics = [topic] if isinstance(topic, str) else list(topic)
        return self.store.query(topics, start=start, end=end, skip=skip, count=count, order=order)

    def version(self):
        return __version__


def main():
    try:
        vip_main(historian, version=__version__)
    except Exception:
        _log.exception("Historian error")
        raise


if __name__ == "__main__":
    sys.exit(main())
