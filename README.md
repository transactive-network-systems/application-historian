# Application Historian

A modular VOLTTRON historian (`BaseHistorian` subclass) that stores two kinds of record in one SQLAlchemy database
through `lib-archivable`'s `LocalDataManager`:

- **Standard VOLTTRON records** on `devices/`, `analysis/`, `datalogger/` and `record/` topics go to the standard
  `topics` and `data` tables and are served by the standard query RPCs (`query`, `get_topic_list`,
  `get_topics_by_pattern`, `get_topics_metadata`). The agent can therefore be installed as the platform's historian.
- **Application archive envelopes**, published by any agent (a *publisher*) on `<prefix>/archive/<publisher>/records`,
  go to the archive tables that publisher registered, either with the `create_tables` RPC (the default) or over pubsub
  (below).

The historian is application-agnostic: it knows nothing about the publishers beyond the table definitions,
enumerations and archive schema they register and the envelopes they send. It imports no application package. `<prefix>` is whatever topic root the application uses; `<publisher>` is an
opaque segment the publisher chooses, echoed in the historian's replies. Topics under `<prefix>/archive/` are routed by
their last segment (`records`, `definitions`, `status`); every other captured topic is a standard record, except that a
message shaped like an archive envelope (`{"table_name": ..., "data": {...}}` or a list of them) is an archive write
wherever it arrives.

```json
{"table_name": "market", "data": {"id": "0190a3c0-...", "market_type": "auction", "delivery_start_time": "2026-06-09T13:00:00+00:00"}}
```

Envelope values are JSON forms (UUIDs and enumeration member names as strings, datetimes as ISO 8601 with an offset,
durations as seconds, a non-finite float as `lib-archivable`'s `{"$float": "inf" | "-inf" | "nan"}` token); the data
manager parses them back by column type, so an inelastic vertex's infinite price is stored as the Float column's
infinity (SQLite has no NaN and keeps NULL for one). A naive datetime is
refused (the archive convention is UTC-aware everywhere).

Package `historian.application` (`src/historian/application/`): `agent.py` holds the agent class
`ApplicationHistorian` and the entry point `main`; `store.py` holds `HistorianStore` (database handling, free of
VOLTTRON); `definitions.py` holds definition parsing, allow-list validation, canonical form and diff; `bindings.py` holds the
optional publisher bindings (which sender may speak for a publisher segment). The distribution
is `volttron-application-historian`; its console script is `volttron-application-historian`.

## Table registration

Before publishing records, each publisher registers the definitions of the tables it will write (the structure an
application's archivable classes declare in `get_archive_definitions()`), the enumerations those tables name, and its
application's archive schema. The registration payload is

```json
{"definitions": [...],
 "enumerations": [{"name": "MarketState", "members": ["Initial", "Active", "..."]}],
 "application": "TENT", "schema_version": 3, "fingerprint": "sha-256 of the application's definitions",
 "application_version": "3.0.0"}
```

Two paths carry the same payload and return the same outcome:

- **RPC (default):** `create_tables(payload)` on the historian's identity. It needs an RPC route to the historian,
  which the modular platform only has within one platform.
- **Pubsub (optional):** the publisher publishes the payload with `"publisher": "<publisher>"` and
  `"request_id": "<uuid>"` beside it on `<prefix>/archive/<publisher>/definitions`; the historian answers on
  `<prefix>/archive/<publisher>/status` with the outcome below plus `request_id` and `publisher`. The publisher
  subscribes to its status topic with `all_platforms=True`, so this works across federated platforms. The reply is an
  ordinary publish; cross-platform visibility comes from the subscriber side. The `publisher` key of the request is
  informational (a mismatch with the topic segment is logged; the reply goes to the topic's publisher). A definitions
  message without `request_id` is registered but gets no reply; a malformed message is logged and answered with
  `"status": "error"` when it carries a `request_id`. The historian ignores its own status publications when they come
  back through its `<prefix>/archive/` capture.

`application`, `schema_version`, `fingerprint` and `application_version` are the application's archive schema (for a
TENT node, `tent.data_manager.archive_schema()`; see "Archive schema versions" below). Either way the historian:

0. **Records or checks the application's schema.** The first registration of an application writes its
   `archive_schema` row. A later one of the same version is accepted (another fingerprint at the same version is a
   warning naming the application: a definition changed without a version bump). One of another version, or one whose
   tables are already in the database without a row for the application (version 1, from before the schema was
   versioned), is answered with `"status": "error"` naming the application and both versions, and nothing is
   registered; the other applications are untouched. A payload without `application`, `schema_version` or
   `fingerprint` (a bare list or mapping of definitions, which is what a publisher from before the schema was versioned
   sends) is refused the same way.
0. **Registers the enumerations.** Each `{"name", "members"}` becomes a Python enumeration the data manager decodes
   the column values with (values are stored by member name) and is persisted in `known_enumerations`. A malformed
   enumeration, or a known name with another member list, refuses the whole registration. A payload without
   `enumerations` is fine as long as none of its tables names an enumeration that is not already registered.
1. **Validates** each definition against an allow-list. Table keys: `name`, `columns`, `inherits`, `indices`,
   `constraints`. Column keys: `name`, `type`, `type_args`, `enumeration`, `foreign_key`, `identity`, `kwargs`,
   `nullable`. Column types: `BigInteger`, `Boolean`, `Date`, `DateTime`, `Float`, `Integer`, `Interval`, `JSON`,
   `Numeric`, `SmallInteger`, `String`, `Text`, `Time`, `Uuid`. Column `kwargs`: the booleans `primary_key`,
   `nullable`, `unique`, `autoincrement`, `index`. Table-level `kwargs` (`quote`, `prefixes`, `schema`, ...) are not
   accepted; names must be plain identifiers; `enumeration` must name an enumeration in the registration or already
   registered; `inherits` and foreign-key targets must be in the same registration or already in the database. The
   historian's own tables (`topics`, `data`, `known_table_definitions`, `known_enumerations`, `archive_schema`) cannot
   be redefined.
2. **Compares** a table that is already registered with the stored definition. Key order and **column order do not
   matter**; a changed, added or removed column does. An identical definition is accepted without change.
3. **Creates** the accepted new tables (columns in the order given) and **persists** each definition in the
   `known_table_definitions` table (`name`, `definition` JSON, `registered_time`) in the same database as the data.

Tables are handled one by one. The outcome is
`{"status": ..., "registered_tables": [...], "rejected_tables": {name: {"message": ..., "differences": ...}}, "message": ..., "application": ..., "schema_version": N}`:

- `success`: every table is registered (created now, or already known and identical);
- `partial`: at least one table registered and at least one rejected;
- `error`: nothing registered (schema refused, enumerations refused, malformed request, every table rejected, or a
  database failure).

`application` is the application the registration was for and `schema_version` the archive schema version the
historian holds for it (the publisher's own once it is on record; `null` when the payload stated none). `registered_tables` lists every accepted
table, including those already known. `differences` is a unified diff of the stored and offered definitions for a
mismatch, otherwise `null`. A table that refers to a table rejected in the same registration (through `inherits` or a
foreign key) is rejected with it. The archive keeps the definition a table was created with; a changed schema needs a
new table name or a new database.

On restart the historian reloads the stored enumerations and definitions, so a publisher that is still running (and
will not register again until its own restart) keeps archiving. A table found in the database with no stored definition
is reflected and accepted with a logged warning; reflection recovers names, columns and keys but not the column semantics (on SQLite
an `Interval` reflects as DATETIME, an `Enum` as VARCHAR, a `Uuid` as CHAR(32)), so register such tables. Registering a
definition for a reflected table replaces the reflection. An envelope naming a table that is neither registered nor
present is dropped and logged at error level with a count; it does not block the rest of the batch. A message on a
`records` topic that is not an envelope or a list of envelopes is dropped and logged.

### Archive schema versions

Each application numbers the set of table definitions and enumerations it ships (TENT: `tent.data_manager.ARCHIVE_SCHEMA_VERSION`,
incremented whenever one of them changes; version 1 is every archive written before the schema was versioned) and
fingerprints it. The historian's database records, in `lib-archivable`'s `archive_schema` table, one row per
application that has registered (`application`, `version`, `fingerprint`, `application_version`, `created`); the
historian itself declares no schema (its data manager is a multi-application host) and starts on any database. A
registration is checked against the row of its application as described above; a mismatch refuses that registration
only. Nothing is migrated: the remedy is a new database (a new URL, or delete the file) or publishers of the recorded
version. `historian_setup` logs the recorded applications and their versions.

### Authorization

The `create_tables` RPC runs DDL in the historian's database. Modular VOLTTRON has no `@RPC.allow` decorator in agent
code; access rules are set per installed agent with `vctl authz`. For a historian installed as `<historian>` and a
publisher installed as `<publisher identity>`:

```shell
vctl authz add agent <historian> --protected-rpcs create_tables
vctl authz add agent <publisher identity> --rpc-capabilities <historian>.create_tables
```

Without these rules any authenticated peer can register tables (within the allow-list above).

The pubsub path has no equivalent capability: any agent on any federated platform can publish definitions and records
under any publisher segment. Publisher bindings (next section) close this per publisher name: with `bind_publishers`
on, a publisher segment accepts one sender only. With bindings off the allow-list validation above is the only guard
on that path, and the records path accepts envelopes from any publisher. With bindings on, the bare-identity
limitation across platforms remains until the router qualifies identities.

### Publisher bindings (optional)

By default the historian stores whatever is published under the prefixes it watches, like every VOLTTRON historian:
any agent may use any `<publisher>` segment. With `"bind_publishers": true` each publisher name is bound to one
*sender*, the identity the platform presents for the publication (the RPC caller's identity on the RPC path):

```json
"bind_publishers": true,
"publisher_bindings": {
  "PNNL_Campus": {"sender": "tn.campus@campus"},
  "CoR":         {"sender": "tn.city@city"}
}
```

- A **listed** publisher is pinned to the configured sender.
- An **unlisted** publisher is bound to the first sender that uses its name, for the life of the database; the binding
  is kept in the `publisher_senders` table (`publisher`, `sender`, `first_seen`) and reloaded at start-up. A configured
  binding wins over a learned one that disagrees (logged at start-up). Bindings learned while the check was on stay in
  the table and are ignored while it is off.
- The check runs before anything is processed: records and definitions messages on `<prefix>/archive/<publisher>/...`
  from another sender are dropped and counted, and a definitions message with a `request_id` is answered with
  `{"status": "error", "message": "Sender '...' may not register or archive for publisher '...'; that publisher is
  bound to sender '...'."}`. A `create_tables` call checks the caller against the payload's `"publisher"` key when it
  carries one; without the key the registration is accepted with a warning and only the records published later are
  bound. A message that carries no sender identity is refused while bindings are on.
- Mismatches are logged at WARNING with a count, one line per minute per publisher and sender
  (`Sender 'tn.x' may not register or archive for publisher 'P'; that publisher is bound to sender 'tn.y'. Dropped
  (N message(s) since the last warning); M dropped for this publisher since start.`); nothing stops and no platform
  alert is published.

Senders are written `identity@instance`. The platform presents bare identities today (`tn.campus`, not
`tn.campus@campus`), so a bare presented identity matches a qualified binding by its identity part, a bare learned
binding matches a later qualified identity the same way, and the historian logs once which form it sees. Until the
router qualifies identities, a binding therefore does not distinguish the same identity installed on two platforms of
a federation; when it does, nothing in the configuration changes and the bare form stops matching. The sender of a
captured publication is what the platform hands the capture callback, stamped into the record's headers under
`ApplicationHistorian-Sender` (any value a publisher sets under that header is overwritten); it is available for topics
captured through `custom_topics.capture_record_data`, which is how archive prefixes are configured.

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
- `tables_def` (`table_prefix`, `data_table`, `topics_table`), `table_definitions` (archive definitions to register
  at start-up, same format and checks as `create_tables`) and `enumerations` (the `{"name", "members"}` enumerations
  those definitions name) are optional; configured definitions belong to no application and get no `archive_schema`
  row.

Other `BaseHistorian` options (`retry_period`, `submit_size_limit`, `backup_storage_limit_gb`, ...) apply unchanged.
The processing loop always runs as a greenlet (`process_loop_in_greenlet` cannot be set to false): the RPC handlers
and the loop share one data manager and must not run on two OS threads.

## Identity

VOLTTRON historians are conventionally installed as `platform.historian`, and that is this agent's default identity
(`DEFAULT_VIP_IDENTITY`). The modular platform assigns the identity at install time (`--vip-identity`), so a
deployment may choose another. Whatever it is called, every publisher must address that identity; there is no fallback
on either side.

## Installation

Install `lib-archivable` into the platform first, then the agent, then store the configuration:

```shell
vctl install-lib /path/to/tns-utilities/lib-archivable
vctl install /path/to/volttron-application-historian --vip-identity platform.historian --tag historian --start
vctl config store platform.historian config /path/to/historian-config.json
```

Requirements: Python 3.11 or later, `volttron-core >= 2.0.0rc0`, `volttron-lib-base-historian`, and `lib-archivable`
(which brings SQLAlchemy; it is not on PyPI, hence the `install-lib`/`pip install -e` step before the agent).

## Not supported

- **Aggregation.** `query(..., agg_type=..., agg_period=...)` raises `NotImplementedError` and `get_aggregate_topics`
  returns an empty list: the historian has no aggregate tables and does not compute aggregates.
- **Topic patterns are globs.** `get_topics_by_pattern` matches a case-insensitive shell-style pattern (`*`, `?`,
  `[seq]`), not a regular expression as the SQL historians do.
- **Trusted senders by default.** With `bind_publishers` off (the default) envelopes are accepted from any publisher
  on payload shape alone; see "Publisher bindings".

## Development

```shell
pixi run test          # or: python -m pytest
```

The tests need `lib-archivable`, SQLAlchemy and pytest but no VOLTTRON platform: the store is tested against a SQLite
file in a temporary directory, and the agent module is imported with the VOLTTRON modules replaced by
`tests/volttron_stubs.py`. The application that registers in the tests is a made-up one with its own schema.

`scripts/inject_simple.py` is a manual check against a running platform (it registers a table over RPC as its own
application, and publishes a few envelopes on `<prefix>/archive/<publisher>/records`
and some standard records); `scripts/probe_queries.py` calls the standard query RPCs of a deployed historian. Neither
is collected by pytest.
