# Transactive Node Historian

A modular VOLTTRON historian (`BaseHistorian` subclass) that stores two kinds of record in one SQLAlchemy database
through TENT's `LocalDataManager`:

- **Standard VOLTTRON records** on `devices/`, `analysis/`, `datalogger/` and `record/` topics go to the standard
  `topics` and `data` tables and are served by the standard query RPCs (`query`, `get_topic_list`,
  `get_topics_by_pattern`, `get_topics_metadata`). The agent can therefore be installed as the platform's historian.
- **TENT archive envelopes**, published by a TransactiveNodeAgent's `VolttronDataManager` on
  `<db_topic>/<node name>/archive`, go to the archive tables the node registered with the `create_tables` RPC.

Routing is by payload shape, not topic: a message that is `{"table_name": ..., "data": {...}}` or a list of such
envelopes is an archive write; anything else is a standard record.

```json
{"table_name": "market", "data": {"id": "0190a3c0-...", "market_type": "auction", "delivery_start_time": "2026-06-09T13:00:00+00:00"}}
```

Envelope values are the JSON forms TENT's `json_encoder` produces (UUIDs and enumeration names as strings, datetimes as
ISO 8601 with an offset, durations as seconds); the data manager parses them back by column type. A naive datetime is
refused (TENT's archive convention is UTC-aware everywhere).

## Table registration (`create_tables`)

Before publishing, each node calls `create_tables(definitions)` with the table definitions its components declare
(`get_archive_definitions()`). The historian:

1. **Validates** every definition against an allow-list. Table keys: `name`, `columns`, `inherits`, `indices`,
   `constraints`. Column keys: `name`, `type`, `type_args`, `enumeration`, `foreign_key`, `identity`, `kwargs`,
   `nullable`. Column types: `BigInteger`, `Boolean`, `Date`, `DateTime`, `Float`, `Integer`, `Interval`, `JSON`,
   `Numeric`, `SmallInteger`, `String`, `Text`, `Time`, `Uuid`. Column `kwargs`: the booleans `primary_key`,
   `nullable`, `unique`, `autoincrement`, `index`. Table-level `kwargs` (`quote`, `prefixes`, `schema`, ...) are not
   accepted; names must be plain identifiers; `enumeration` must name an enum in `tent.enumerations`. The historian's
   own tables (`topics`, `data`, `known_table_definitions`) cannot be redefined.
2. **Persists** each new definition in the `known_table_definitions` table (`name`, canonical `definition` JSON,
   `registered_time`) in the same database as the data, and creates the table.
3. **Compares** a table that is registered again with the stored definition. An identical definition (any key order;
   column order matters, since it is the table's column order) is a no-op. A different one is refused for the whole
   call: the reply is `{"status": "error", "message": ..., "differences": {table: unified diff}}` and nothing changes.
   The archive keeps the definition a table was created with; a changed schema needs a new table name or a new database.

The reply on success is `{"status": "success", "registered_tables": [new in this call], "known_tables": [all]}`.

On restart the historian reloads the stored definitions, so a node that is still running (and will not register again
until its own restart) keeps archiving. A table found in the database with no stored definition is reflected and
accepted with a logged warning; reflection recovers names, columns and keys but not TENT's column semantics (on SQLite
an `Interval` reflects as DATETIME, an `Enum` as VARCHAR, a `Uuid` as CHAR(32)), so register such tables. Registering a
definition for a reflected table replaces the reflection. An envelope naming a table that is neither registered nor
present is dropped and logged at error level with a count; it does not block the rest of the batch.

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
    "capture_record_data": ["tns/"]
  }
}
```

- `connection.params.url` must be a **SQLAlchemy URL**. For SQLite that is `sqlite:////absolute/path.sqlite` (four
  slashes) or `sqlite:///relative.sqlite` (relative to the agent's working directory); PostgreSQL is
  `postgresql://user:password@host/dbname`. A bare path such as `/home/vuzer/db/historian.sqlite` is refused at
  install time and at configuration time with a message giving the `sqlite:////` form. No other keys are accepted in
  `connection` (the SQL historians' `type` and `database` keys are not used here).
- `custom_topics.capture_record_data` must list the nodes' `db_topic` prefix with the same case the nodes publish
  (`tns/` in this project's containers); pubsub prefixes are case-sensitive.
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
`scripts/inject_simple.py` is a manual check against a running platform (it registers a table and publishes a few
envelopes and standard records); it is not collected by pytest.

Layout: `src/historian/transactivate_node/definitions.py` (definition parsing, allow-list validation, canonical form
and diff), `store.py` (`HistorianStore`: database handling, free of VOLTTRON), `transactive_node.py` (the agent).
