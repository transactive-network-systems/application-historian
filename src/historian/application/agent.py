#!/usr/bin/env python
"""
The Application Historian: a modular VOLTTRON historian that stores applications' archive records in a SQLAlchemy
database through lib-archivable's LocalDataManager, beside the platform's standard records.

It serves two kinds of record through one BaseHistorian subscription set:

- standard VOLTTRON records (`devices/`, `analysis/`, `datalogger/`, `record/`), stored in the standard `topics` and
  `data` tables and answered through the standard query RPCs (`query`, `get_topic_list`, `get_topics_by_pattern`,
  `get_topics_metadata`), so that the agent can be installed as the platform's historian;
- archive envelopes, `{"table_name": ..., "data": {...}}` or lists of them, which a publisher (any agent that archives
  through this historian) publishes on `<prefix>/archive/<publisher>/records` after registering its table
  definitions, either with the `create_tables` RPC (default) or by publishing them on
  `<prefix>/archive/<publisher>/definitions`; the historian answers the pubsub path on
  `<prefix>/archive/<publisher>/status`. Envelopes are stored in the registered archive tables. A registration
  states its application's archive schema (application, version, fingerprint) and the enumerations its tables name
  beside its definitions; the historian keeps one archive_schema row per application and refuses a registration whose
  version differs from the one it recorded for that application.

`<publisher>` is an opaque segment chosen by the publisher; the historian only echoes it in its status replies. Topics
under `<prefix>/archive/` are routed by their last segment (records, definitions, status); every other captured topic
is a standard record, except that an archive envelope is recognised by shape wherever it arrives. All database
handling is in `HistorianStore` (store.py); this module is the VOLTTRON shell: configuration from the config store,
RPC exports, the pubsub registration path and the BaseHistorian publishing contract.

Identity: VOLTTRON historians are conventionally installed as `platform.historian` (`DEFAULT_VIP_IDENTITY`); the
modular platform assigns the identity at `vctl install --vip-identity`, so a deployment may choose another, as long as
every publisher addresses the same one.

Aggregation (`agg_type`/`agg_period` on `query`, `get_aggregate_topics`) is not supported by this historian: there are
no aggregate tables, and an aggregate query raises rather than returning an empty result.
"""
import logging
import sys
from collections.abc import Mapping
from datetime import datetime, timezone

from historian.base.base_historian import BaseHistorian
from volttron.client.known_identities import PLATFORM_HISTORIAN
from volttron.client.logs import setup_logging
from volttron.client.messaging import headers as headers_mod
from volttron.client.vip.agent import RPC
from volttron.utils import load_config, vip_main

from historian.application.definitions import check_enumerations, parse_definitions
from historian.application.store import HistorianStore, RegistrationResult

__version__ = "3.0.0"

setup_logging()
_log = logging.getLogger(__name__)

DEFAULT_VIP_IDENTITY = PLATFORM_HISTORIAN

ARCHIVE_SEGMENT = "archive"
RECORDS_TOPIC, DEFINITIONS_TOPIC, STATUS_TOPIC = "records", "definitions", "status"

CONFIGURATION_HELP = ('Store the historian configuration as the config-store entry "config" of the historian\'s VIP '
                      'identity, e.g. vctl config store <identity> config <config file>. It must contain '
                      '{"connection": {"params": {"url": "<SQLAlchemy URL>"}}}.')
REGISTRATION_KEYS = ('{"publisher", "request_id", "definitions", "enumerations", "application", "schema_version", '
                     '"fingerprint", "application_version"}')


def historian(config_path, **kwargs):
    """
    Build the historian. `config_path` is the `--agent-config` file when one was given at install time; its contents
    become the defaults for the config-store entry `config`, which is the normal place to configure the historian.
    """
    config = load_config(config_path) or {}
    connection = config.pop("connection", None)
    tables_def = config.pop("tables_def", None)
    table_definitions = config.pop("table_definitions", None)
    enumerations = config.pop("enumerations", None)
    kwargs.update(config)
    return ApplicationHistorian(
        connection=connection,
        tables_def=tables_def,
        table_definitions=table_definitions,
        enumerations=enumerations,
        **kwargs,
    )


class ApplicationHistorian(BaseHistorian):
    """Store standard VOLTTRON records and registered application archive records in one SQLAlchemy database."""

    def __init__(self, connection=None, tables_def=None, table_definitions=None, enumerations=None, **kwargs):
        # The RPC handlers (create_tables, queries) run on the agent's greenlets; the publishing loop must as well so
        # that the data manager is never touched from a second OS thread.
        if not kwargs.pop("process_loop_in_greenlet", True):
            raise ValueError("ApplicationHistorian requires process_loop_in_greenlet=True (the default).")
        if connection is not None:
            HistorianStore.validate_connection(connection)  # fail at install time, not when the first record arrives
        self.connection = connection
        self.tables_def, self.table_names = self.parse_table_def(tables_def)
        self.configured_table_definitions = parse_definitions(table_definitions)
        self.configured_enumerations = check_enumerations(enumerations)
        self.store: HistorianStore | None = None
        super().__init__(process_loop_in_greenlet=True, **kwargs)
        self.update_default_config({
            "connection": connection,
            "tables_def": tables_def,
            "table_definitions": self.configured_table_definitions,
            "enumerations": self.configured_enumerations,
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
        self.configured_enumerations = check_enumerations(configuration.get("enumerations"))
        self.store = HistorianStore(connection, self.table_names)  # raises ValueError for a URL that is not SQLAlchemy's
        self.connection = connection
        _log.info(f"Historian database: {self.store.redacted_url}")

    def historian_setup(self):
        """Open the database, create the historian's tables and reload the registered enumerations and table
        definitions, then register the configured ones (a rejected configured definition raises and the historian does
        not start)."""
        if self.store is None:
            raise RuntimeError(f'The historian is not configured (see the configuration error logged above). '
                               f'{CONFIGURATION_HELP}')
        self.store.setup(self.configured_table_definitions, self.configured_enumerations)
        _log.info(f"Historian ready; archive tables: {sorted(self.store.writable_tables)}; applications: "
                  f"{self.store.schema_versions()}")

    def publish_to_historian(self, to_publish_list):
        """
        Persist one BaseHistorian batch. Topics under `<prefix>/archive/<publisher>/` are handled by kind: `records` carry
        envelopes, `definitions` carry a registration request (answered on `status`), `status` is the historian's own
        reply coming back through its capture and is ignored. Everything else is written as it comes. Every record is
        reported handled: rows that cannot be stored are logged, not retried.
        """
        if self.store is None or not self.store.ready:
            raise RuntimeError(f'The historian database is not open; {len(to_publish_list)} record(s) stay cached. '
                               f'{CONFIGURATION_HELP}')
        to_write = []
        for record in to_publish_list:
            parsed = self.parse_archive_topic(record["topic"])
            if parsed is None:
                to_write.append(record)
                continue
            prefix, publisher, kind = parsed
            if kind == RECORDS_TOPIC:
                if HistorianStore.is_envelope_payload(record.get("value")):
                    to_write.append(record)
                else:
                    _log.error(f'Dropped a message on {record["topic"]!r} that is not an archive envelope or a list '
                               f'of them: {str(record.get("value"))[:200]!r}')
            elif kind == DEFINITIONS_TOPIC:
                self._handle_definitions_message(prefix, publisher, record.get("value"))
            else:  # STATUS_TOPIC: the historian's own publication
                _log.debug(f'Ignoring the historian\'s own status publication on {record["topic"]!r}')
        if to_write:
            result = self.store.write(to_write)
            if result.dropped:
                _log.error(f"Dropped {result.dropped} archive row(s) in this batch for unregistered tables: "
                           f"{dict(result.skipped_unknown_table)}.")
        self.report_all_handled()

    @staticmethod
    def parse_archive_topic(topic):
        """(prefix, publisher, kind) for `<prefix>/archive/<publisher>/<records|definitions|status>`, else None."""
        parts = topic.split("/")
        if (len(parts) < 4 or parts[-3] != ARCHIVE_SEGMENT or not parts[-2]
                or parts[-1] not in (RECORDS_TOPIC, DEFINITIONS_TOPIC, STATUS_TOPIC)):
            return None
        return "/".join(parts[:-3]), parts[-2], parts[-1]

    # ---------------------------------------------------------------- pubsub registration path
    def _handle_definitions_message(self, prefix, publisher, message):
        """
        Register the definitions in a `{"publisher", "request_id", "definitions", "enumerations", "application",
        "schema_version", "fingerprint", "application_version"}` message and publish the outcome on
        `<prefix>/archive/<publisher>/status`. A malformed message is logged; it gets an "error" status reply only when
        it carries a request_id to answer.
        """
        request_id = message.get("request_id") if isinstance(message, Mapping) else None
        if not isinstance(request_id, str) or not request_id:
            request_id = None
        if not isinstance(message, Mapping) or "definitions" not in message:
            _log.error(f'Malformed definitions message from publisher {publisher!r} on {prefix}/{ARCHIVE_SEGMENT}/'
                       f'{publisher}/{DEFINITIONS_TOPIC}: expected {REGISTRATION_KEYS}, got {str(message)[:200]!r}')
            if request_id is not None:
                self._publish_status(prefix, publisher, request_id, self._error_reply(
                    f'Malformed definitions message: expected {REGISTRATION_KEYS}.'))
            return
        named = message.get("publisher")
        if named is not None and named != publisher:
            _log.warning(f'Definitions message names publisher {named!r} but arrived on the topic of {publisher!r}; '
                         f'replying on {publisher!r}\'s status topic.')
        if request_id is None:
            _log.error(f'Definitions message from publisher {publisher!r} has no request_id; the publisher cannot '
                       f'match a reply. The definitions are registered anyway.')
        reply = self._register({key: value for key, value in message.items() if key not in ("publisher", "request_id")})
        if request_id is not None:
            self._publish_status(prefix, publisher, request_id, reply)

    def _publish_status(self, prefix, publisher, request_id, reply):
        topic = f"{prefix}/{ARCHIVE_SEGMENT}/{publisher}/{STATUS_TOPIC}"
        now = datetime.now(timezone.utc).isoformat()
        message = {"request_id": request_id, "publisher": publisher, **reply}
        try:
            self.vip.pubsub.publish(peer="pubsub", topic=topic, headers={headers_mod.DATE: now,
                                                                        headers_mod.TIMESTAMP: now},
                                    message=message).get(timeout=10.0)
        except Exception as exc:
            _log.error(f"Could not publish the registration status for publisher {publisher!r} (request {request_id}) "
                       f"on {topic!r}: {exc!r}. Outcome was: {reply['status']}: {reply['message']}")

    # ---------------------------------------------------------------- archive table registration
    @RPC.export
    def create_tables(self, definitions):
        """
        Register an application's archive tables and create them. The argument is `{"definitions": [...],
        "enumerations": [...], "application", "schema_version", "fingerprint", "application_version"}`: the table
        definitions (the structure an application's archivable classes declare), the enumerations their columns name
        (`{"name", "members"}` each) and the application's archive schema. The application's schema is recorded on its
        first registration and checked afterwards: a registration whose version differs from the recorded one, one
        whose tables are already in the database without a record (version 1), or one that states no application,
        version or fingerprint is refused with "error" (naming both versions where there are two) and nothing is
        registered. The enumerations are registered next (a malformed one or a changed member list refuses the whole
        registration). Each definition is then checked against an allow-list of keys, column types and column
        keywords (an `enumeration` must be in the registration or already registered), persisted in
        `known_table_definitions`, and compared with the stored definition when its table is registered again (column
        order does not matter). Tables are handled one by one: the unchanged ones are registered even when another in
        the same call is rejected.

        Returns {"status": "success" | "partial" | "error", "registered_tables": [accepted names],
        "rejected_tables": {name: {"message": str, "differences": unified diff or None}}, "message": str,
        "application": the application registered for, "schema_version": the archive schema version the historian
        holds for it}; "success" means every table is registered (created now or already known and identical),
        "partial" that at least one was and at least one was rejected, "error" that none was.

        This RPC runs DDL; the modular platform has no `RPC.allow` decorator, so protect it per deployment:
        `vctl authz add agent <historian identity> --protected-rpcs create_tables` and give each publisher
        `--rpc-capabilities <historian identity>.create_tables`.
        """
        if self.store is None or not self.store.ready:
            return self._error_reply(f"The historian database is not open. {CONFIGURATION_HELP}")
        return self._register(definitions)

    def _register(self, payload):
        try:
            result = self.store.register(payload)
        except Exception as exc:  # the database itself failed; nothing was registered
            _log.exception("Table registration failed")
            return self._error_reply(f"Registration failed: {exc!r}")
        return self._registration_reply(result)

    @staticmethod
    def _registration_reply(result: RegistrationResult):
        return {
            "status": result.status,
            "registered_tables": list(result.registered),
            "rejected_tables": dict(result.rejected),
            "message": result.message,
            "application": result.application,
            "schema_version": result.schema_version,
        }

    @staticmethod
    def _error_reply(message):
        return {"status": "error", "registered_tables": [], "rejected_tables": {}, "message": message,
                "application": None, "schema_version": None}

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
            raise NotImplementedError("The Application Historian does not support aggregate queries "
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
