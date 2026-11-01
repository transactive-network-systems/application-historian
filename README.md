# Application Historian

A modular VOLTTRON historian (`BaseHistorian` subclass) that stores two kinds of record in one SQLAlchemy database
through TENT's `LocalDataManager`:

- **Standard VOLTTRON records** on `devices/`, `analysis/`, `datalogger/` and `record/` topics go to the standard
  `topics` and `data` tables and are served by the standard query RPCs (`query`, `get_topic_list`,
  `get_topics_by_pattern`, `get_topics_metadata`). The agent can therefore be installed as the platform's historian.
- **Application archive envelopes**, published by any agent (a *publisher*) on `<prefix>/archive/<publisher>/records`,
  go to the archive tables that publisher registered, either with the `create_tables` RPC (the default) or over pubsub
  (below).

The historian is application-agnostic: it knows nothing about the publishers beyond the table definitions they
register and the envelopes they send. `<prefix>` is whatever topic root the application uses; `<publisher>` is an
opaque segment the publisher chooses, echoed in the historian's replies. Topics under `<prefix>/archive/` are routed by
their last segment (`records`, `definitions`, `status`); every other captured topic is a standard record, except that a
message shaped like an archive envelope (`{"table_name": ..., "data": {...}}` or a list of them) is an archive write
wherever it arrives.

```json
{"table_name": "market", "data": {"id": "0190a3c0-...", "market_type": "auction", "delivery_start_time": "2026-06-09T13:00:00+00:00"}}
```

Envelope values are the JSON forms TENT's `json_encoder` produces (UUIDs and enumeration names as strings, datetimes as
ISO 8601 with an offset, durations as seconds); the data manager parses them back by column type. A naive datetime is
refused (the archive convention is UTC-aware everywhere).

Package `historian.application` (`src/historian/application/`): `agent.py` holds the agent class
`ApplicationHistorian` and the entry point `main`; `store.py` holds `HistorianStore` (database handling, free of
VOLTTRON); `definitions.py` holds definition parsing, allow-list validation, canonical form and diff. The distribution
is `volttron-application-historian`; its console script is `volttron-application-historian`.

## Table registration

Before publishing records, each publisher registers the definitions of the tables it will write (the structure TENT's
`get_archive_definitions()` returns). Two paths carry the same definitions and return the same outcome:

- **RPC (default):** `create_tables({"definitions": [...], "schema_version": N})` on the historian's identity. It
  needs an RPC route to the historian, which the modular platform only has within one platform.
- **Pubsub (optional):** the publisher publishes `{"publisher": "<publisher>", "request_id": "<uuid>",
  "definitions": [...], "schema_version": N}` on `<prefix>/archive/<publisher>/definitions`; the historian answers on
  `<prefix>/archive/<publisher>/status` with the outcome below plus `request_id` and `publisher`. The publisher
  subscribes to its status topic with `all_platforms=True`, so this works across federated platforms. The reply is an
  ordinary publish; cross-platform visibility comes from the subscriber side. The `publisher` key of the request is
  informational (a mismatch with the topic segment is logged; the reply goes to the topic's publisher). A definitions
  message without `request_id` is registered but gets no reply; a malformed message is logged and answered with
  `"status": "error"` when it carries a `request_id`. The historian ignores its own status publications when they come
  back through its `<prefix>/archive/` capture.

`schema_version` is the publisher's `tent.data_manager.ARCHIVE_SCHEMA_VERSION`, the version of the set of table
definitions its TENT ships (see "Archive schema version" below). Either way the historian:

0. **Checks the schema version.** A registration whose `schema_version` is not the one this historian's TENT writes,
   or that has none (a bare list or mapping of definitions, which is what a publisher of TENT before 2 November 2026
   sends), is answered with `"status": "error"` naming both versions, and nothing is registered.
1. **Validates** each definition against an allow-list. Table keys: `name`, `columns`, `inherits`, `indices`,
   `constraints`. Column keys: `name`, `type`, `type_args`, `enumeration`, `foreign_key`, `identity`, `kwargs`,
   `nullable`. Column types: `BigInteger`, `Boolean`, `Date`, `DateTime`, `Float`, `Integer`, `Interval`, `JSON`,
   `Numeric`, `SmallInteger`, `String`, `Text`, `Time`, `Uuid`. Column `kwargs`: the booleans `primary_key`,
   `nullable`, `unique`, `autoincrement`, `index`. Table-level `kwargs` (`quote`, `prefixes`, `schema`, ...) are not
   accepted; names must be plain identifiers; `enumeration` must name an enum in `tent.enumerations`; `inherits` and
   foreign-key targets must be in the same registration or already in the database. The historian's own tables
   (`topics`, `data`, `known_table_definitions`, `archive_schema`) cannot be redefined.
2. **Compares** a table that is already registered with the stored definition. Key order and **column order do not
   matter**; a changed, added or removed column does. An identical definition is accepted without change.
3. **Creates** the accepted new tables (columns in the order given) and **persists** each definition in the
   `known_table_definitions` table (`name`, `definition` JSON, `registered_time`) in the same database as the data.

Tables are handled one by one. The outcome is
`{"status": ..., "registered_tables": [...], "rejected_tables": {name: {"message": ..., "differences": ...}}, "message": ..., "schema_version": N}`:

- `success`: every table is registered (created now, or already known and identical);
- `partial`: at least one table registered and at least one rejected;
- `error`: nothing registered (schema version refused, malformed request, every table rejected, or a database
  failure).

`schema_version` is the historian's archive schema version, in every reply. `registered_tables` lists every accepted
table, including those already known. `differences` is a unified diff of the stored and offered definitions for a
mismatch, otherwise `null`. A table that refers to a table rejected in the same registration (through `inherits` or a
foreign key) is rejected with it. The archive keeps the definition a table was created with; a changed schema needs a
new table name or a new database.

On restart the historian reloads the stored definitions, so a publisher that is still running (and will not register
again until its own restart) keeps archiving. A table found in the database with no stored definition is reflected and
accepted with a logged warning; reflection recovers names, columns and keys but not TENT's column semantics (on SQLite
an `Interval` reflects as DATETIME, an `Enum` as VARCHAR, a `Uuid` as CHAR(32)), so register such tables. Registering a
definition for a reflected table replaces the reflection. An envelope naming a table that is neither registered nor
present is dropped and logged at error level with a count; it does not block the rest of the batch. A message on a
`records` topic that is not an envelope or a list of envelopes is dropped and logged.

### Archive schema version

TENT numbers the set of table definitions it ships: `tent.data_manager.ARCHIVE_SCHEMA_VERSION`, incremented whenever
one of them changes (version 1 is every archive written before 2 November 2026, which carries no version). The
historian's database records the version it was written with in TENT's `archive_schema` table (`version`,
`fingerprint`, `tent_version`, `created`): the data manager writes the row when the database is new and checks it at
every start-up. A database written with another version, or one that has tables but no `archive_schema` table
(version 1), makes `historian_setup` raise `ArchiveSchemaMismatch` with both versions in the message, and the historian
does not start; nothing is migrated. The remedy is a new database (a new URL, or delete the file) or a historian whose
TENT is of the database's version. A publisher's registration is refused the same way when its version differs from
the historian's, so "this database is from an older TENT" and "this publisher runs another TENT than the historian" are
each said in so many words instead of appearing as rejected table definitions.

### Authorization

The `create_tables` RPC runs DDL in the historian's database. Modular VOLTTRON has no `@RPC.allow` decorator in agent
code; access rules are set per installed agent with `vctl authz`. For a historian installed as `<historian>` and a
publisher installed as `<publisher identity>`:

```shell
vctl authz add agent <historian> --protected-rpcs create_tables
vctl authz add agent <publisher identity> --rpc-capabilities <historian>.create_tables
```

Without these rules any authenticated peer can register tables (within the allow-list above).

The pubsub path has no equivalent: any agent on any federated platform can publish definitions and records under any
publisher segment, and there is no per-publisher capability as with protected RPCs. Until sender identity is checked
(planned work, deferred), the allow-list validation above is the only guard on that path, and the records path accepts
envelopes from any publisher on either path.

## Configuration

The historian is configured through the config store entry `config`. The `connection` block is applied in
`configure()`, which the base historian calls whenever the entry is stored or updated; it rebuilds the database
connection, and the processing loop then opens the database. A file passed with `--agent-config` at install time only
provides the defaults for that entry. `config.json` in this repository is a sample.

```json
{
  "connection": {
    "params": {
      "url": "sqlite:////var/lib/historian/historian.sqlite"
    }
  },
  "all_platforms": true,
  "capture_device_data": true,
  "capture_analysis_data": true,
  "capture_log_data": true,
  "capture_record_data": true,
  "custom_topics": {
    "capture_record_data": ["app/archive/"]
  }
}
```

- `connection.params.url` must be a **SQLAlchemy URL**. For SQLite that is `sqlite:////absolute/path.sqlite` (four
  slashes) or `sqlite:///relative.sqlite` (relative to the agent's working directory); PostgreSQL is
  `postgresql://user:password@host/dbname`. A bare path such as `/var/lib/historian/historian.sqlite` is refused at
  install time and at configuration time with a message giving the `sqlite:////` form. No other keys are accepted in
  `connection` (the SQL historians' `type` and `database` keys are not used here).
- `custom_topics.capture_record_data` must list `<prefix>/archive/` for each application prefix, with the same case
  the publishers use (pubsub prefixes are case-sensitive). List `<prefix>/archive/`, not `<prefix>/`: an application's
  other topics under the same root would otherwise be stored as records.
- `all_platforms: true` is needed when publishers run on other platforms of a federation.
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
deployment may choose another. Whatever it is called, every publisher must address that identity; there is no fallback
on either side.

## Installation

Install TENT into the platform first, then the agent, then store the configuration:

```shell
vctl install-lib /path/to/TENT
vctl install /path/to/volttron-application-historian --vip-identity platform.historian --tag historian --start
vctl config store platform.historian config /path/to/historian-config.json
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
The tests import `tent` from the environment; the archive schema version they check against is that TENT's.

`scripts/inject_simple.py` is a manual check against a running platform (it registers a table over RPC, with the
installed TENT's `ARCHIVE_SCHEMA_VERSION`, and publishes a few envelopes on `<prefix>/archive/<publisher>/records`
and some standard records); `scripts/probe_queries.py` calls the standard query RPCs of a deployed historian. Neither
is collected by pytest.
