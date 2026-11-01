# Transactive Node Historian

A modular VOLTTRON historian (`BaseHistorian` subclass) that stores two kinds of record in one SQLAlchemy database
through TENT's `LocalDataManager`:

- **Standard VOLTTRON records** on `devices/`, `analysis/`, `datalogger/` and `record/` topics go to the standard
  `topics` and `data` tables and are served by the standard query RPCs (`query`, `get_topic_list`,
  `get_topics_by_pattern`, `get_topics_metadata`). The agent can therefore be installed as the platform's historian.
- **TENT archive envelopes**, published by a TransactiveNodeAgent's `VolttronDataManager` on
  `<db_topic>/archive/<node name>/records`, go to the archive tables the node registered, either with the
  `create_tables` RPC (the default) or over pubsub (below).

Topics under `<db_topic>/archive/` are routed by their last segment (`records`, `definitions`, `status`); every other
captured topic is a standard record, except that a message shaped like an archive envelope
(`{"table_name": ..., "data": {...}}` or a list of them) is an archive write wherever it arrives.

```json
{"table_name": "market", "data": {"id": "0190a3c0-...", "market_type": "auction", "delivery_start_time": "2026-06-09T13:00:00+00:00"}}
```

Envelope values are the JSON forms TENT's `json_encoder` produces (UUIDs and enumeration names as strings, datetimes as
ISO 8601 with an offset, durations as seconds); the data manager parses them back by column type. A naive datetime is
refused (TENT's archive convention is UTC-aware everywhere).

## Table registration

Before publishing records, each node registers the table definitions its components declare
(`get_archive_definitions()`). Two paths carry the same definitions and return the same outcome:

- **RPC (default):** `create_tables(definitions)` on the historian's identity. It needs an RPC route to the historian,
  which the modular platform only has within one platform.
- **Pubsub (optional):** the node publishes `{"node": "<node>", "request_id": "<uuid>", "definitions": [...]}` on
  `<db_topic>/archive/<node>/definitions`; the historian answers on `<db_topic>/archive/<node>/status` with the
  outcome below plus `request_id` and `node`. The node subscribes to its status topic with `all_platforms=True`, so
  this works across federated platforms. The reply is an ordinary publish; cross-platform visibility comes from the
  subscriber side. A definitions message without `request_id` is registered but gets no reply; a malformed message is
  logged and answered with `"status": "error"` when it carries a `request_id`. The historian ignores its own status
  publications when they come back through its `<db_topic>/archive/` capture.

Either way the historian:

1. **Validates** each definition against an allow-list. Table keys: `name`, `columns`, `inherits`, `indices`,
   `constraints`. Column keys: `name`, `type`, `type_args`, `enumeration`, `foreign_key`, `identity`, `kwargs`,
   `nullable`. Column types: `BigInteger`, `Boolean`, `Date`, `DateTime`, `Float`, `Integer`, `Interval`, `JSON`,
   `Numeric`, `SmallInteger`, `String`, `Text`, `Time`, `Uuid`. Column `kwargs`: the booleans `primary_key`,
   `nullable`, `unique`, `autoincrement`, `index`. Table-level `kwargs` (`quote`, `prefixes`, `schema`, ...) are not
   accepted; names must be plain identifiers; `enumeration` must name an enum in `tent.enumerations`; `inherits` and
   foreign-key targets must be in the same registration or already in the database. The historian's own tables
   (`topics`, `data`, `known_table_definitions`) cannot be redefined.
2. **Compares** a table that is already registered with the stored definition. Key order and **column order do not
   matter**; a changed, added or removed column does. An identical definition is accepted without change.
3. **Creates** the accepted new tables (columns in the order given) and **persists** each definition in the
   `known_table_definitions` table (`name`, `definition` JSON, `registered_time`) in the same database as the data.

Tables are handled one by one. The outcome is
`{"status": ..., "registered_tables": [...], "rejected_tables": {name: {"message": ..., "differences": ...}}, "message": ...}`:

- `success`: every table is registered (created now, or already known and identical);
- `partial`: at least one table registered and at least one rejected;
- `error`: nothing registered (malformed request, every table rejected, or a database failure).

`registered_tables` lists every accepted table, including those already known. `differences` is a unified diff of the
stored and offered definitions for a mismatch, otherwise `null`. A table that refers to a table rejected in the same
registration (through `inherits` or a foreign key) is rejected with it. The archive keeps the definition a table was
created with; a changed schema needs a new table name or a new database.

On restart the historian reloads the stored definitions, so a node that is still running (and will not register again
until its own restart) keeps archiving. A table found in the database with no stored definition is reflected and
accepted with a logged warning; reflection recovers names, columns and keys but not TENT's column semantics (on SQLite
an `Interval` reflects as DATETIME, an `Enum` as VARCHAR, a `Uuid` as CHAR(32)), so register such tables. Registering a
definition for a reflected table replaces the reflection. An envelope naming a table that is neither registered nor
present is dropped and logged at error level with a count; it does not block the rest of the batch. A message on a
`records` topic that is not an envelope or a list of envelopes is dropped and logged.

### Protecting `create_tables`

The RPC runs DDL in the historian's database. Modular VOLTTRON has no `@RPC.allow` decorator in agent code; access
rules are set per installed agent with `vctl authz`. For a historian installed as `tns.historian` and nodes
`tn.campus` and `tn.city`:

```shell
vctl authz add agent tns.historian --protected-rpcs create_tables
vctl authz add agent tn.campus --rpc-capabilities tns.historian.create_tables
vctl authz add agent tn.city --rpc-capabilities tns.historian.create_tables
```

Without these rules any authenticated peer can register tables (within the allow-list above).

The pubsub path has no equivalent: any agent on any federated platform can publish definitions and records for any
node name, and there is no per-publisher capability as with protected RPCs. Until sender identity is checked (planned
work, deferred), the allow-list validation above is the only guard on that path, and the records path accepts envelopes
from any publisher on either path.

## Configuration

The historian is configured through the config store entry `config`. The `connection` block is applied in
`configure()`, which the base historian calls whenever the entry is stored or updated; it rebuilds the database
connection, and the processing loop then opens the database. A file passed with `--agent-config` at install time only
provides the defaults for that entry.

```json
{
  "connection": {
    "params": {
      "url": "sqlite:////home/vuzer/db/historian.sqlite"
    }
  },
  "all_platforms": true,
  "capture_device_data": true,
  "capture_analysis_data": true,
  "capture_log_data": true,
  "capture_record_data": true,
  "custom_topics": {
    "capture_record_data": ["tns/archive/"]
  }
}
```

- `connection.params.url` must be a **SQLAlchemy URL**. For SQLite that is `sqlite:////absolute/path.sqlite` (four
  slashes) or `sqlite:///relative.sqlite` (relative to the agent's working directory); PostgreSQL is
  `postgresql://user:password@host/dbname`. A bare path such as `/home/vuzer/db/historian.sqlite` is refused at
  install time and at configuration time with a message giving the `sqlite:////` form. No other keys are accepted in
  `connection` (the SQL historians' `type` and `database` keys are not used here).
- `custom_topics.capture_record_data` must list `<db_topic>/archive/` with the same case the nodes publish
  (`tns/archive/` in this project's containers); pubsub prefixes are case-sensitive. The prefix is deliberately
  `tns/archive/` and not `tns/`: the nodes exchange transactive signals on `tns/<sender>/<receiver>`, and those are not
  historian records.
- `all_platforms: true` is needed when the nodes run on other platforms (federation), as in the containers.
- `capture_*` are the base historian's standard subscriptions; they default to true and are written out so the
  deployed behaviour is visible.
- `tables_def` (`table_prefix`, `data_table`, `topics_table`) and `table_definitions` (archive definitions to
  register at start-up, same format and checks as `create_tables`) are optional.

Other `BaseHistorian` options (`retry_period`, `submit_size_limit`, `backup_storage_limit_gb`, ...) apply unchanged.
The processing loop always runs as a greenlet (`process_loop_in_greenlet` cannot be set to false): the RPC handlers
and the loop share one data manager and must not run on two OS threads.

## Identity

VOLTTRON historians are conventionally installed as `platform.historian`, and that is this agent's default identity
(`DEFAULT_VIP_IDENTITY`). The modular platform assigns the identity at install time (`--vip-identity`), so a
deployment may choose another: this project's containers install it as `tns.historian`. Whatever it is called, every
node's `data_manager.historian_vip` must name it; there is no fallback on either side.

## Installation

Install TENT into the platform first, then the agent, then store the configuration:

```shell
vctl install-lib /path/to/TENT
vctl install /path/to/volttron-transactive-node-historian --vip-identity tns.historian --tag historian --start
vctl config store tns.historian config /path/to/tns-historian.json
```

Requirements: Python 3.11 or later, `volttron-core >= 2.0.0rc0`, `volttron-lib-base-historian`, and `tent` (which
brings SQLAlchemy; it is not on PyPI, hence the `install-lib`/`pip install -e` step and no entry in `pyproject.toml`).

## Not supported

- **Aggregation.** `query(..., agg_type=..., agg_period=...)` raises `NotImplementedError` and `get_aggregate_topics`
  returns an empty list: the historian has no aggregate tables and does not compute aggregates.
- **Topic patterns are globs.** `get_topics_by_pattern` matches a case-insensitive shell-style pattern (`*`, `?`,
  `[seq]`), not a regular expression as the SQL historians do.
- **Trusted senders.** Envelopes are accepted from any publisher on payload shape alone; sender checks are a planned
  change.

## Development

```shell
pixi run test          # or: python -m pytest
```

The tests need `tent`, SQLAlchemy and pytest but no VOLTTRON platform: the store is tested against a SQLite file in a
temporary directory, and the agent module is imported with the VOLTTRON modules replaced by `tests/volttron_stubs.py`.
`scripts/inject_simple.py` is a manual check against a running platform (it registers a table over RPC and publishes a
few envelopes on `tns/archive/<node>/records` and some standard records); it is not collected by pytest.

Layout: `src/historian/transactivate_node/definitions.py` (definition parsing, allow-list validation, canonical form
and diff), `store.py` (`HistorianStore`: database handling, free of VOLTTRON), `transactive_node.py` (the agent).
