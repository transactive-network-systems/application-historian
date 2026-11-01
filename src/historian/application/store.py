"""
Database side of the Application Historian, kept free of VOLTTRON so it can be exercised against a SQLite file.

`HistorianStore` owns one TENT `LocalDataManager` (SQLAlchemy) and in it:

- the standard historian tables (`topics`, `data`) for ordinary VOLTTRON records;
- `known_table_definitions`: one row per registered archive table (name, canonical definition JSON, time registered).
  Definitions arrive from each publisher's registration (RPC or pubsub); persisting them beside the data is what lets
  the historian restart without the publishers re-registering, and is what the mismatch check compares against;
- `archive_schema`: TENT's one-row record of the archive schema version the database was written with
  (`tent.data_manager.ARCHIVE_SCHEMA_VERSION`), written by the data manager when the database is new and checked on
  every start-up: a database of another version is refused (`ArchiveSchemaMismatch`, both versions in the message),
  nothing is migrated. A publisher's registration states its own schema version and is refused when it differs from the
  historian's TENT (`publisher_schema_version_problem`);
- the archive tables themselves, created from those definitions.

On start-up (`setup`) the stored definitions are reloaded, any table in the database that has no stored definition is
reflected and accepted for writes with a logged warning (its column semantics are then whatever reflection recovers:
on SQLite an `Interval` reflects as DATETIME, an `Enum` as VARCHAR, a `Uuid` as CHAR(32)), and the tables are created
if missing. Registering a definition for a reflected table replaces the reflected table with the definition.
"""
import json
import logging
import threading
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from fnmatch import fnmatchcase

from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError
from tent.data_manager import ARCHIVE_SCHEMA_TABLE, ARCHIVE_SCHEMA_VERSION, ArchiveSchemaMismatch
from tent.data_manager.local_data_manager import LocalDataManager

from historian.application.definitions import (canonical_json, check_definitions, describe_difference,
                                                      reject_unresolved)

_log = logging.getLogger(__name__)

KNOWN_TABLE_DEFINITIONS = 'known_table_definitions'


def format_timestamp(value: datetime) -> str:
    """The base historian's timestamp string: YYYY-MM-DDTHH:MM:SS.ffffff with +HH:MM when the datetime is aware."""
    text = value.strftime('%Y-%m-%dT%H:%M:%S.%f')
    if value.tzinfo is not None:
        offset = value.strftime('%z')
        text += f'{offset[:3]}:{offset[3:]}'
    return text


@dataclass
class RegistrationResult:
    """Outcome of one registration call; the agent turns it into the create_tables reply / status message."""
    registered: list[str] = field(default_factory=list)  # accepted tables: created now or already known and identical
    created: list[str] = field(default_factory=list)  # the subset that was new in this call
    rejected: dict[str, dict] = field(default_factory=dict)  # table -> {"message": str, "differences": str | None}

    @property
    def status(self) -> str:
        if self.registered and not self.rejected:
            return 'success'
        if self.registered:
            return 'partial'
        return 'error'

    @property
    def message(self) -> str:
        parts = []
        if self.created:
            parts.append(f'{len(self.created)} table(s) created: {sorted(self.created)}')
        already = sorted(set(self.registered) - set(self.created))
        if already:
            parts.append(f'{len(already)} table(s) already registered with an identical definition: {already}')
        if self.rejected:
            parts.append(f'{len(self.rejected)} table(s) rejected: {sorted(self.rejected)}')
        return '; '.join(parts) + '.' if parts else 'No table definitions were supplied.'


@dataclass
class WriteResult:
    standard_rows: int = 0
    archive_rows: int = 0  # rows handed to the data manager; it logs any it quarantines
    skipped_unknown_table: Counter = field(default_factory=Counter)  # table name -> rows dropped

    @property
    def dropped(self) -> int:
        return sum(self.skipped_unknown_table.values())


class HistorianStore:

    def __init__(self, connection: Mapping, table_names: Mapping[str, str]):
        self.url: str = self.validate_connection(connection)
        self.topics_table: str = table_names['topics_table']
        self.data_table: str = table_names['data_table']
        self.own_tables: set[str] = {self.topics_table, self.data_table, KNOWN_TABLE_DEFINITIONS, ARCHIVE_SCHEMA_TABLE}
        self.data_manager: LocalDataManager | None = None
        self.reflected_tables: set[str] = set()
        self.topic_id_map: dict[str, int] = {}
        self.topic_name_map: dict[str, str] = {}
        self.topic_meta: dict[int, dict] = {}
        # The agent's RPC handlers and its publishing loop both reach the data manager; one lock serialises them.
        self._lock = threading.RLock()

    # ---------------------------------------------------------------- configuration
    @staticmethod
    def validate_connection(connection) -> str:
        """Return the SQLAlchemy URL from a `connection` block, or raise ValueError saying what a valid one looks like."""
        example = ('{"connection": {"params": {"url": "sqlite:////absolute/path/historian.sqlite"}}} '
                   '(four slashes for an absolute SQLite path, three for a path relative to the agent directory; '
                   'any SQLAlchemy URL such as postgresql://user:password@host/dbname is accepted)')
        if not isinstance(connection, Mapping):
            raise ValueError(f'"connection" must be a mapping, not {type(connection).__name__}. Expected {example}.')
        unexpected = set(connection) - {'params'}
        if unexpected:
            raise ValueError(f'"connection" has unsupported key(s) {sorted(unexpected)}; the database is selected by '
                             f'the URL alone. Expected {example}.')
        params = connection.get('params')
        if not isinstance(params, Mapping):
            raise ValueError(f'"connection.params" must be a mapping. Expected {example}.')
        unexpected = set(params) - {'url'}
        if unexpected:
            raise ValueError(f'"connection.params" has unsupported key(s) {sorted(unexpected)}. Expected {example}.')
        url = params.get('url')
        if not isinstance(url, str) or not url:
            raise ValueError(f'"connection.params.url" is missing. Expected {example}.')
        try:
            make_url(url)
        except ArgumentError as exc:
            hint = f' Did you mean "sqlite:///{url}"?' if url.startswith('/') else ''
            raise ValueError(f'"connection.params.url" {url!r} is not a SQLAlchemy URL ({exc}).{hint} '
                             f'Expected {example}.') from exc
        return url

    @property
    def redacted_url(self) -> str:
        return make_url(self.url).render_as_string(hide_password=True)

    @property
    def ready(self) -> bool:
        return self.data_manager is not None

    # ---------------------------------------------------------------- archive schema version
    schema_version: int = ARCHIVE_SCHEMA_VERSION
    """The archive schema version this historian's TENT writes; the database is checked to be of it at set-up."""

    @staticmethod
    def publisher_schema_version_problem(schema_version) -> str | None:
        """
        None when a registration's declared archive schema version is this historian's; otherwise the reason it is
        refused, naming both versions. A registration that states no version comes from a TENT before 2 November 2026
        (archive schema version 1) and is refused the same way; nothing is migrated or translated.
        """
        if schema_version == ARCHIVE_SCHEMA_VERSION and isinstance(schema_version, int):
            return None
        if schema_version is None:
            described = 'does not state its archive schema version, so its TENT writes archive schema version 1'
        else:
            described = f'states archive schema version {schema_version!r}'
        return (f'The publisher {described}; this historian\'s TENT writes archive schema version {ARCHIVE_SCHEMA_VERSION}. '
                f'Nothing was registered. Run the publisher and the historian on TENT installations with the same archive '
                f'schema version.')

    @property
    def writable_tables(self) -> set[str]:
        """Archive tables an envelope may name: every mapped table that is not one of the historian's own."""
        if self.data_manager is None:
            return set()
        return set(self.data_manager.orm) - self.own_tables

    @property
    def registered_tables(self) -> set[str]:
        """Tables with a stored definition (as opposed to merely reflected)."""
        if self.data_manager is None:
            return set()
        return set(self.data_manager.definitions) - self.own_tables

    # ---------------------------------------------------------------- set-up
    def setup(self, configured_definitions: Sequence[Mapping] = ()) -> None:
        """Open the database, create the historian's tables, reload stored definitions, register configured ones."""
        with self._lock:
            self._build_data_manager()
            if configured_definitions:
                result = self.register_definitions(configured_definitions)
                if result.rejected:
                    raise ValueError(f'Configured table_definitions rejected: {result.rejected}')
            self._load_topic_cache()

    def _own_table_definitions(self) -> list[dict]:
        return [
            {
                'name': self.topics_table,
                'columns': [
                    {'name': 'topic_id', 'type': 'Integer', 'kwargs': {'primary_key': True, 'autoincrement': True}},
                    {'name': 'topic_name', 'type': 'String', 'type_args': [512],
                     'kwargs': {'nullable': False, 'unique': True}},
                    {'name': 'metadata', 'type': 'Text'},
                ],
            },
            {
                'name': self.data_table,
                'columns': [
                    {'name': 'ts', 'type': 'DateTime', 'kwargs': {'nullable': False, 'primary_key': True}},
                    {'name': 'topic_id', 'type': 'Integer', 'foreign_key': f'{self.topics_table}.topic_id',
                     'kwargs': {'nullable': False, 'primary_key': True}},
                    {'name': 'value_string', 'type': 'Text', 'kwargs': {'nullable': False}},
                ],
            },
            {
                'name': KNOWN_TABLE_DEFINITIONS,
                'columns': [
                    {'name': 'name', 'type': 'String', 'type_args': [63], 'kwargs': {'primary_key': True}},
                    {'name': 'definition', 'type': 'Text', 'kwargs': {'nullable': False}},
                    {'name': 'registered_time', 'type': 'DateTime', 'kwargs': {'nullable': False}},
                ],
            },
        ]

    def _build_data_manager(self) -> None:
        data_manager = LocalDataManager(None, {'url': self.url})
        data_manager.add_tables(self._own_table_definitions())
        try:
            # Creates the historian's own tables when missing; writes the archive_schema row into a new database and
            # refuses a database written with another archive schema version.
            data_manager.init_archive()
        except ArchiveSchemaMismatch as error:
            raise ArchiveSchemaMismatch(error.database_version, error.tent_schema_version,
                                        where=f'Historian database {self.redacted_url!r}') from error
        stored = self._stored_definitions(data_manager)
        if stored:
            data_manager.add_tables(list(stored.values()))
            _log.info(f'Reloaded {len(stored)} registered table definition(s): {sorted(stored)}')
        reflected = data_manager.reflect_unknown_tables()
        for name in reflected:
            if name in data_manager.orm:
                _log.warning(f'Table {name!r} exists in the database without a registered definition; it was '
                             f'reflected and will accept archive rows, but column types are only what reflection '
                             f'recovers (intervals, enumerations and UUIDs are stored as the database reports them).')
            else:
                _log.warning(f'Table {name!r} exists in the database without a registered definition and has no '
                             f'primary key; archive rows for it will be dropped.')
        data_manager.init_archive()  # creates tables for the reloaded definitions when missing
        self.data_manager = data_manager
        self.reflected_tables = set(reflected)

    @staticmethod
    def _stored_definitions(data_manager: LocalDataManager) -> dict[str, dict]:
        rows = data_manager.query_data(KNOWN_TABLE_DEFINITIONS, order_by=[('registered_time', 'asc'), ('name', 'asc')])
        return {row['name']: json.loads(row['definition']) for row in rows}

    # ---------------------------------------------------------------- registration
    def register_definitions(self, definitions: Sequence) -> RegistrationResult:
        """
        Check each definition, compare it with what is stored, persist and create the new ones. Tables are handled one
        by one: a rejected table (invalid, or stored with a different definition) does not stop the others, and an
        identical re-registration is accepted without change. Column order does not matter in the comparison; a new
        table is created with its columns in the order given. Raises only when the database itself fails.
        """
        with self._lock:
            self._require_ready()
            result = RegistrationResult()
            # Any table in the database other than the historian's own (registered or reflected) may be referred to.
            present = set(self.data_manager.tables) - self.own_tables
            valid, rejected = check_definitions(definitions, reserved_names=self.own_tables, known_names=present)
            result.rejected.update({name: {'message': message, 'differences': None}
                                    for name, message in rejected.items()})
            stored = self._stored_definitions(self.data_manager)
            new: list[dict] = []
            for definition in valid:
                name = definition['name']
                if name not in stored:
                    new.append(definition)
                    result.registered.append(name)
                elif canonical_json(stored[name]) == canonical_json(definition):
                    result.registered.append(name)
                else:
                    result.rejected[name] = {
                        'message': f'Table {name!r} is already registered with a different definition. The archive '
                                   f'keeps the definition a table was created with; use a new table name or a new '
                                   f'database if the schema has changed.',
                        'differences': describe_difference(stored[name], definition),
                    }
            # A table that refers to one just rejected for a mismatch cannot be created either. Any table in the
            # database (registered, reflected or the historian's own) can be referred to.
            reasons = {name: rejection['message'] for name, rejection in result.rejected.items()}
            reject_unresolved(new, reasons, available=present | set(stored))
            for name, message in reasons.items():
                if name not in result.rejected:
                    result.registered.remove(name)
                    result.rejected[name] = {'message': message, 'differences': None}
            for name, rejection in result.rejected.items():
                _log.error(f'Table definition rejected: {rejection["message"]}')
            if not new:
                return result
            result.created = [definition['name'] for definition in new]
            if any(name in self.reflected_tables for name in result.created):
                # A reflected Table is already in the metadata under this name; persist, then rebuild so the
                # definition wins over the reflection.
                self._persist_definitions(new)
                self._build_data_manager()
                self._load_topic_cache()
            else:
                try:
                    self.data_manager.add_tables(new)
                    self.data_manager.init_archive()
                except Exception:
                    # The metadata now holds Table objects that could not be created; rebuild from what is stored so
                    # the data manager stays consistent, and let the caller report the failure.
                    self._build_data_manager()
                    self._load_topic_cache()
                    raise
                self._persist_definitions(new)
            _log.info(f'Registered archive table(s): {result.created}')
            return result

    def _persist_definitions(self, definitions: Sequence[Mapping]) -> None:
        registered_time = datetime.now(timezone.utc)
        for definition in definitions:
            self.data_manager.upsert_data(KNOWN_TABLE_DEFINITIONS, {
                'name': definition['name'],
                'definition': json.dumps(definition, sort_keys=True),  # columns in the order given: the DDL order
                'registered_time': registered_time,
            }, ['name'])

    def _require_ready(self) -> None:
        if self.data_manager is None:
            raise RuntimeError('HistorianStore.setup() has not run; the database is not open.')

    # ---------------------------------------------------------------- writes
    def write(self, records: Sequence[Mapping]) -> WriteResult:
        """
        Store one base-historian batch. A record whose value is an archive envelope ({"table_name", "data"}) or a list
        of them goes to the named archive table(s); anything else is a standard record and goes to `topics`/`data`.
        Envelopes naming a table that is not registered are dropped and counted (the data manager quarantines rows the
        database refuses), so one bad row cannot block the batch forever.
        """
        result = WriteResult()
        with self._lock:
            self._require_ready()
            writable = self.writable_tables
            archive_rows: list[dict] = []
            for record in records:
                envelopes = self._envelopes(record.get('value'))
                if envelopes is None:
                    self._store_standard_record(record)
                    result.standard_rows += 1
                    continue
                for envelope in envelopes:
                    table_name = envelope['table_name']
                    if table_name in writable:
                        archive_rows.append({'table_name': table_name, 'data': dict(envelope['data'])})
                    else:
                        result.skipped_unknown_table[table_name] += 1
            for table_name, count in result.skipped_unknown_table.items():
                _log.error(f'Dropped {count} archive row(s) for table {table_name!r}: it has not been registered with '
                           f'create_tables and is not in the database.')
            if archive_rows:
                self._archive(archive_rows, result)
        return result

    def _archive(self, rows: list[dict], result: WriteResult) -> None:
        """
        Hand the rows to the data manager. It quarantines (logs and drops) a row it cannot normalise or the database
        refuses, so one bad row never blocks the batch; it re-queues everything and raises when the database itself is
        unavailable. In that case the base historian keeps the batch in its cache and retries, so the data manager's
        copy is discarded here to avoid writing the rows twice when the database comes back.
        """
        try:
            self.data_manager.archive_data(rows)
        except Exception:
            self.data_manager.archival_queue.clear()
            raise
        result.archive_rows += len(rows)

    @staticmethod
    def is_envelope_payload(value) -> bool:
        return HistorianStore._envelopes(value) is not None

    @staticmethod
    def _envelopes(value):
        if HistorianStore._is_envelope(value):
            return [value]
        if isinstance(value, list) and value and all(HistorianStore._is_envelope(item) for item in value):
            return value
        return None

    @staticmethod
    def _is_envelope(value) -> bool:
        return (isinstance(value, Mapping) and isinstance(value.get('table_name'), str)
                and isinstance(value.get('data'), Mapping))

    def _store_standard_record(self, record: Mapping) -> None:
        topic_id = self._upsert_topic(record['topic'], record.get('meta') or {})
        self.data_manager.upsert_data(self.data_table, {
            'ts': record['timestamp'],
            'topic_id': topic_id,
            'value_string': json.dumps(record.get('value')),
        }, ['topic_id', 'ts'])

    def _upsert_topic(self, topic: str, metadata: Mapping) -> int:
        topic_key = topic.lower()
        topic_id = self.topic_id_map.get(topic_key)
        metadata = dict(metadata)
        if topic_id is None:
            row = self.data_manager.upsert_data(self.topics_table, {
                'topic_name': topic,
                'metadata': json.dumps(metadata),
            }, ['topic_name'])
            topic_id = row['topic_id']
            self.topic_id_map[topic_key] = topic_id
        elif self.topic_name_map.get(topic_key) != topic or self.topic_meta.get(topic_id) != metadata:
            self.data_manager.upsert_data(self.topics_table, {
                'topic_id': topic_id,
                'topic_name': topic,
                'metadata': json.dumps(metadata),
            }, ['topic_id'])
        self.topic_name_map[topic_key] = topic
        self.topic_meta[topic_id] = metadata
        return topic_id

    def _load_topic_cache(self) -> None:
        self.topic_id_map.clear()
        self.topic_name_map.clear()
        self.topic_meta.clear()
        for row in self.data_manager.query_data(self.topics_table):
            topic_key = row['topic_name'].lower()
            self.topic_id_map[topic_key] = row['topic_id']
            self.topic_name_map[topic_key] = row['topic_name']
            self.topic_meta[row['topic_id']] = json.loads(row['metadata'] or '{}')

    # ---------------------------------------------------------------- queries (standard records)
    def topic_list(self) -> list[str]:
        with self._lock:
            return list(self.topic_name_map.values())

    def topics_by_pattern(self, pattern: str) -> dict[str, int]:
        """Topics matching a shell-style glob (`*`, `?`, `[seq]`), case-insensitively. Not a regular expression."""
        pattern = pattern.lower()
        with self._lock:
            return {topic_name: self.topic_id_map[topic_key]
                    for topic_key, topic_name in self.topic_name_map.items() if fnmatchcase(topic_key, pattern)}

    def topics_metadata(self, topics: Sequence[str]) -> dict[str, dict]:
        with self._lock:
            return {topic: self.topic_meta[self.topic_id_map[topic.lower()]]
                    for topic in topics if topic.lower() in self.topic_id_map}

    def query(self, topics: Sequence[str], start: datetime | None = None, end: datetime | None = None,
              skip: int = 0, count: int | None = None, order: str = 'FIRST_TO_LAST') -> dict:
        """
        Values for one or more topics in the base historian's result shape: {"values": [(ts, value), ...],
        "metadata": {...}} for a single topic, {"values": {topic: [...]}, "metadata": {}} for several, {} when nothing
        matched. Timestamps are strings in `format_timestamp` form; `start` is inclusive and `end` exclusive.
        """
        conditions: dict = {}
        if start is not None:
            conditions.setdefault('ts', {})['gte'] = start
        if end is not None:
            conditions.setdefault('ts', {})['lt'] = end
        values: dict[str, list] = {}
        with self._lock:
            self._require_ready()
            for topic_name in topics:
                topic_id = self.topic_id_map.get(topic_name.lower())
                if topic_id is None:
                    continue
                topic_conditions = dict(conditions)
                topic_conditions['topic_id'] = topic_id
                rows = self.data_manager.query_data(
                    self.data_table,
                    conditions=topic_conditions,
                    order_by=[('ts', 'desc' if order == 'LAST_TO_FIRST' else 'asc')],
                    limit=count,
                    offset=skip,
                )
                if rows:
                    values[topic_name] = [(format_timestamp(row['ts']), json.loads(row['value_string']))
                                          for row in rows]
            if not values:
                return {}
            if len(topics) > 1:
                return {'values': values, 'metadata': {}}
            topic_name = next(iter(values))
            return {'values': values[topic_name],
                    'metadata': self.topic_meta.get(self.topic_id_map[topic_name.lower()], {})}
