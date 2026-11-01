"""HistorianStore against a SQLite file: configuration, registration, restart, envelope and standard-record writes, queries."""
import logging
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import Column, Integer, MetaData, String, Table, create_engine, inspect, text
from sqlalchemy.exc import OperationalError

from conftest import SAMPLE_DEFINITIONS, TABLE_NAMES, envelope_record, standard_record
from historian.application.store import KNOWN_TABLE_DEFINITIONS, HistorianStore, format_timestamp
from tent.data_manager import ARCHIVE_SCHEMA_TABLE, ARCHIVE_SCHEMA_VERSION, ArchiveSchemaMismatch
from tent.enumerations import MarketTypes

MARKET_ID = "0190a3c0-0000-7000-8000-000000000001"
INTERVAL_ID = "0190a3c0-0000-7000-8000-000000000002"


def open_store(connection, definitions=()):
    store = HistorianStore(connection, TABLE_NAMES)
    store.setup(definitions)
    return store


def market_envelopes():
    return [
        {"table_name": "market", "data": {"id": MARKET_ID, "name": "day-ahead", "market_type": "auction",
                                          "delivery_start_time": "2026-06-09T13:00:00+00:00", "active": True}},
        {"table_name": "time_interval", "data": {"id": INTERVAL_ID, "market_id": MARKET_ID,
                                                 "start_time": "2026-06-09T13:00:00Z", "duration": 3600.0}},
    ]


# ---------------------------------------------------------------- connection block
class TestValidateConnection:

    def test_bare_path_is_rejected_with_the_sqlite_url_form(self):
        with pytest.raises(ValueError) as error:
            HistorianStore.validate_connection({"params": {"url": "/home/vuzer/db/historian.sqlite"}})
        message = str(error.value)
        assert "not a SQLAlchemy URL" in message
        assert "sqlite:////home/vuzer/db/historian.sqlite" in message

    def test_relative_name_without_scheme_is_rejected(self):
        with pytest.raises(ValueError, match="not a SQLAlchemy URL"):
            HistorianStore.validate_connection({"params": {"url": "historian.sqlite"}})

    @pytest.mark.parametrize("connection", [
        None,
        {"type": "sqlite", "params": {"url": "sqlite:///x.sqlite"}},  # the SQL historians' `type` key
        {"params": {"database": "/home/vuzer/db/historian.sqlite"}},  # the SQL historians' `database` key
        {"params": {}},
        {},
    ])
    def test_other_shapes_are_rejected(self, connection):
        with pytest.raises(ValueError, match="connection"):
            HistorianStore.validate_connection(connection)

    def test_sqlalchemy_urls_are_accepted(self):
        assert HistorianStore.validate_connection({"params": {"url": "sqlite:////home/vuzer/db/h.sqlite"}}) \
            == "sqlite:////home/vuzer/db/h.sqlite"
        assert HistorianStore.validate_connection({"params": {"url": "postgresql://u:p@host/db"}}) \
            == "postgresql://u:p@host/db"

    def test_redacted_url_hides_the_password(self):
        store = HistorianStore({"params": {"url": "postgresql://user:secret@host/db"}}, TABLE_NAMES)
        assert "secret" not in store.redacted_url
        assert "user" in store.redacted_url


# ---------------------------------------------------------------- set-up and registration
def test_setup_creates_the_historians_own_tables(connection, db_url):
    open_store(connection)
    present = set(inspect(create_engine(db_url)).get_table_names())
    assert {"topics", "data", KNOWN_TABLE_DEFINITIONS, ARCHIVE_SCHEMA_TABLE} <= present


# ---------------------------------------------------------------- archive schema version
def test_new_database_records_the_archive_schema_version(connection, db_url):
    store = open_store(connection)
    assert store.schema_version == ARCHIVE_SCHEMA_VERSION
    [row] = store.data_manager.query_data(ARCHIVE_SCHEMA_TABLE)
    assert row["version"] == ARCHIVE_SCHEMA_VERSION and row["fingerprint"] and row["tent_version"]
    open_store(connection, SAMPLE_DEFINITIONS)  # restart, same version: accepted, still one row
    assert len(open_store(connection).data_manager.query_data(ARCHIVE_SCHEMA_TABLE)) == 1


def test_database_of_another_schema_version_is_refused_at_setup(connection, db_url):
    open_store(connection, SAMPLE_DEFINITIONS)
    with create_engine(db_url).begin() as conn:
        conn.execute(text(f"UPDATE {ARCHIVE_SCHEMA_TABLE} SET version = {ARCHIVE_SCHEMA_VERSION + 5}"))
    with pytest.raises(ArchiveSchemaMismatch) as error:
        open_store(connection)
    message = str(error.value)
    assert f"version {ARCHIVE_SCHEMA_VERSION + 5}" in message and f"version {ARCHIVE_SCHEMA_VERSION}" in message
    assert "Historian database" in message and "Recreate the database" in message
    assert error.value.database_version == ARCHIVE_SCHEMA_VERSION + 5


def test_database_without_the_archive_schema_table_is_version_one_and_refused(connection, db_url):
    """A historian database from before 2 November 2026 has the historian's tables but no archive_schema."""
    open_store(connection, SAMPLE_DEFINITIONS)
    with create_engine(db_url).begin() as conn:
        conn.execute(text(f"DROP TABLE {ARCHIVE_SCHEMA_TABLE}"))
    with pytest.raises(ArchiveSchemaMismatch, match=f"version 1;.*version {ARCHIVE_SCHEMA_VERSION}"):
        open_store(connection)


def test_archive_schema_is_a_reserved_table(connection):
    store = open_store(connection)
    result = store.register_definitions([{"name": ARCHIVE_SCHEMA_TABLE,
                                          "columns": [{"name": "version", "type": "Integer",
                                                       "kwargs": {"primary_key": True}}]}])
    assert result.status == "error" and "reserved" in result.rejected[ARCHIVE_SCHEMA_TABLE]["message"]


def test_publisher_schema_version_problem_names_both_versions():
    assert HistorianStore.publisher_schema_version_problem(ARCHIVE_SCHEMA_VERSION) is None
    problem = HistorianStore.publisher_schema_version_problem(ARCHIVE_SCHEMA_VERSION + 1)
    assert f"version {ARCHIVE_SCHEMA_VERSION + 1}" in problem and f"version {ARCHIVE_SCHEMA_VERSION}" in problem
    assert "Nothing was registered" in problem
    missing = HistorianStore.publisher_schema_version_problem(None)
    assert "does not state" in missing and "version 1" in missing and f"version {ARCHIVE_SCHEMA_VERSION}" in missing
    assert HistorianStore.publisher_schema_version_problem(str(ARCHIVE_SCHEMA_VERSION)) is not None
    assert HistorianStore.publisher_schema_version_problem(True) is not None


def test_register_persists_definitions_and_creates_tables(connection, db_url):
    store = open_store(connection)
    result = store.register_definitions(SAMPLE_DEFINITIONS)
    assert result.status == "success"
    assert result.registered == ["component", "market", "time_interval"]
    assert result.created == ["component", "market", "time_interval"]
    assert result.rejected == {}
    assert "3 table(s) created" in result.message
    assert store.writable_tables == {"component", "market", "time_interval"}
    present = set(inspect(create_engine(db_url)).get_table_names())
    assert {"component", "market", "time_interval"} <= present
    with create_engine(db_url).connect() as conn:
        rows = conn.execute(text(f"SELECT name, definition, registered_time FROM {KNOWN_TABLE_DEFINITIONS}")).all()
    assert {row[0] for row in rows} == {"component", "market", "time_interval"}
    assert all('"columns"' in row[1] for row in rows)
    assert all(row[2] for row in rows)


def test_identical_re_registration_is_a_success_that_creates_nothing(connection):
    store = open_store(connection)
    store.register_definitions(SAMPLE_DEFINITIONS)
    shuffled_keys = [dict(reversed(list(d.items()))) for d in SAMPLE_DEFINITIONS]
    result = store.register_definitions(shuffled_keys[::-1])  # order of tables and of keys does not matter
    assert result.status == "success"
    assert sorted(result.registered) == ["component", "market", "time_interval"]
    assert result.created == [] and result.rejected == {}
    assert "already registered with an identical definition" in result.message


def test_reordered_columns_are_the_same_definition(connection):
    store = open_store(connection)
    store.register_definitions(SAMPLE_DEFINITIONS)
    reordered = dict(SAMPLE_DEFINITIONS[0], columns=list(reversed(SAMPLE_DEFINITIONS[0]["columns"])))
    result = store.register_definitions([reordered])
    assert result.status == "success" and result.registered == ["component"] and result.created == []


def test_new_table_keeps_the_given_column_order(connection, db_url):
    store = open_store(connection)
    definition = {"name": "ordered", "columns": [{"name": "zeta", "type": "Integer", "kwargs": {"primary_key": True}},
                                                 {"name": "alpha", "type": "Text"}]}
    store.register_definitions([definition])
    columns = [c["name"] for c in inspect(create_engine(db_url)).get_columns("ordered")]
    assert columns == ["zeta", "alpha"]
    restarted = open_store(connection)
    assert [c.name for c in restarted.data_manager.tables["ordered"].columns] == ["zeta", "alpha"]


def test_changed_re_registration_is_rejected_with_the_difference_and_the_rest_registered(connection):
    store = open_store(connection)
    store.register_definitions(SAMPLE_DEFINITIONS)
    changed = dict(SAMPLE_DEFINITIONS[2])
    changed["columns"] = changed["columns"][:-1] + [{"name": "duration", "type": "Float"}]
    result = store.register_definitions([SAMPLE_DEFINITIONS[0], changed])
    assert result.status == "partial"
    assert result.registered == ["component"]
    assert set(result.rejected) == {"time_interval"}
    rejection = result.rejected["time_interval"]
    assert "different definition" in rejection["message"]
    assert '-   "type": "Interval"' in rejection["differences"] and '+   "type": "Float"' in rejection["differences"]
    assert store.writable_tables == {"component", "market", "time_interval"}  # nothing changed in the database
    only_changed = store.register_definitions([changed])
    assert only_changed.status == "error" and only_changed.registered == []


def test_invalid_definition_is_rejected_and_the_valid_ones_registered(connection):
    store = open_store(connection)
    bad = dict(SAMPLE_DEFINITIONS[0], columns=[{"name": "id", "type": "PickleType", "kwargs": {"primary_key": True}}])
    other = {"name": "other", "columns": [{"name": "id", "type": "Integer", "kwargs": {"primary_key": True}}]}
    result = store.register_definitions([bad, other])
    assert result.status == "partial"
    assert result.registered == ["other"]
    assert "PickleType" in result.rejected["component"]["message"]
    assert result.rejected["component"]["differences"] is None
    assert store.registered_tables == {"other"}


def test_tables_referring_to_a_rejected_table_are_rejected_with_it(connection):
    store = open_store(connection)
    bad = dict(SAMPLE_DEFINITIONS[0], columns=[{"name": "id", "type": "PickleType", "kwargs": {"primary_key": True}}])
    result = store.register_definitions([bad, SAMPLE_DEFINITIONS[1], SAMPLE_DEFINITIONS[2]])  # market inherits component,
    assert result.status == "error"                                                     # time_interval -> market
    assert set(result.rejected) == {"component", "market", "time_interval"}
    assert "'component', which was rejected" in result.rejected["market"]["message"]
    assert "'market', which was rejected" in result.rejected["time_interval"]["message"]
    assert store.registered_tables == set()
    assert store.data_manager.query_data(KNOWN_TABLE_DEFINITIONS) == []


def test_foreign_key_to_an_unknown_table_is_rejected_not_created(connection, db_url):
    store = open_store(connection)
    result = store.register_definitions([SAMPLE_DEFINITIONS[2]])  # time_interval.market_id -> market, never registered
    assert result.status == "error"
    assert "neither in this registration nor already registered" in result.rejected["time_interval"]["message"]
    assert "time_interval" not in inspect(create_engine(db_url)).get_table_names()
    store.register_definitions(SAMPLE_DEFINITIONS[:2])
    assert store.register_definitions([SAMPLE_DEFINITIONS[2]]).status == "success"  # target now registered


def test_self_reference_and_reference_to_reflected_table_are_allowed(connection, db_url):
    metadata = MetaData()
    Table("legacy_notes", metadata, Column("id", Integer, primary_key=True))
    metadata.create_all(create_engine(db_url))
    store = open_store(connection)
    result = store.register_definitions([
        {"name": "chain", "columns": [{"name": "id", "type": "Integer", "kwargs": {"primary_key": True}},
                                      {"name": "prior_id", "foreign_key": "chain.id"},
                                      {"name": "note_id", "foreign_key": "legacy_notes.id"}]}])
    assert result.status == "success"


def test_unchanged_child_of_a_mismatched_but_existing_parent_is_registered(connection):
    store = open_store(connection)
    store.register_definitions([SAMPLE_DEFINITIONS[0]])
    changed_parent = dict(SAMPLE_DEFINITIONS[0], columns=SAMPLE_DEFINITIONS[0]["columns"] + [{"name": "x", "type": "Text"}])
    result = store.register_definitions([changed_parent, SAMPLE_DEFINITIONS[1]])
    assert result.status == "partial"  # market is unchanged and its parent exists with the stored definition
    assert result.registered == ["market"] and set(result.rejected) == {"component"}
    assert store.registered_tables == {"component", "market"}


def test_empty_registration_is_an_error_outcome(connection):
    result = open_store(connection).register_definitions([])
    assert result.status == "error" and result.message == "No table definitions were supplied."


def test_registering_a_reserved_table_is_refused(connection):
    store = open_store(connection)
    result = store.register_definitions([{"name": "topics", "columns": [{"name": "x", "type": "Integer"}]}])
    assert result.status == "error" and "reserved" in result.rejected["topics"]["message"]


def test_configured_definitions_are_registered_at_setup(connection):
    store = open_store(connection, SAMPLE_DEFINITIONS)
    assert store.writable_tables == {"component", "market", "time_interval"}
    bad = [dict(SAMPLE_DEFINITIONS[0], columns=[{"name": "id", "type": "PickleType"}])]
    with pytest.raises(ValueError, match="PickleType"):
        open_store(connection, bad)


# ---------------------------------------------------------------- restart
def test_restart_reloads_definitions_and_accepts_envelopes_without_re_registration(connection, caplog):
    first = open_store(connection)
    first.register_definitions(SAMPLE_DEFINITIONS)
    first.write([envelope_record(market_envelopes()[:1])])

    restarted = open_store(connection)  # no register_definitions call
    assert restarted.registered_tables == {"component", "market", "time_interval"}
    assert restarted.reflected_tables == set()
    result = restarted.write([envelope_record(market_envelopes()[1:])])
    assert result.archive_rows == 1 and result.dropped == 0
    rows = restarted.data_manager.query_data("time_interval")
    assert rows[0]["duration"] == timedelta(hours=1)  # the Interval column semantics survived the restart


def test_restart_with_a_changed_definition_is_still_refused(connection):
    open_store(connection).register_definitions(SAMPLE_DEFINITIONS)
    restarted = open_store(connection)
    changed = dict(SAMPLE_DEFINITIONS[0], columns=SAMPLE_DEFINITIONS[0]["columns"] + [{"name": "extra", "type": "Text"}])
    result = restarted.register_definitions([changed])
    assert result.status == "error" and "component" in result.rejected


def test_table_without_definition_is_reflected_with_a_warning_and_accepts_rows(connection, db_url, caplog):
    metadata = MetaData()
    Table("legacy_notes", metadata, Column("id", Integer, primary_key=True), Column("note", String(50)))
    metadata.create_all(create_engine(db_url))
    with caplog.at_level(logging.WARNING, logger="historian.application.store"):
        store = open_store(connection)
    assert "legacy_notes" in store.reflected_tables
    assert "legacy_notes" in store.writable_tables
    assert "legacy_notes" not in store.registered_tables
    assert any("legacy_notes" in r.message and "without a registered definition" in r.message for r in caplog.records)
    result = store.write([envelope_record([{"table_name": "legacy_notes", "data": {"id": 1, "note": "kept"}}])])
    assert result.archive_rows == 1
    assert store.data_manager.query_data("legacy_notes") == [{"id": 1, "note": "kept"}]


def test_registering_a_reflected_table_replaces_reflection_with_the_definition(connection, db_url):
    metadata = MetaData()
    Table("legacy_notes", metadata, Column("id", Integer, primary_key=True), Column("note", String(50)))
    metadata.create_all(create_engine(db_url))
    store = open_store(connection)
    definition = {"name": "legacy_notes", "columns": [{"name": "id", "type": "Integer", "kwargs": {"primary_key": True}},
                                                      {"name": "note", "type": "String", "type_args": [50]}]}
    assert store.register_definitions([definition]).created == ["legacy_notes"]
    assert "legacy_notes" in store.registered_tables
    assert "legacy_notes" not in store.reflected_tables
    assert store.register_definitions([definition]).created == []


# ---------------------------------------------------------------- envelope writes
def test_envelopes_with_json_string_values_are_normalised_by_column_type(connection):
    store = open_store(connection, SAMPLE_DEFINITIONS)
    result = store.write([envelope_record(market_envelopes())])
    assert result.archive_rows == 2 and result.standard_rows == 0 and result.dropped == 0

    market = store.data_manager.query_data("market")[0]
    assert market["id"] == uuid.UUID(MARKET_ID)
    assert market["market_type"] is MarketTypes.auction
    assert market["delivery_start_time"] == datetime(2026, 6, 9, 13, tzinfo=timezone.utc)
    assert market["active"] is True
    component = store.data_manager.query_data("component")[0]  # joined inheritance wrote the parent row
    assert component["id"] == uuid.UUID(MARKET_ID) and component["name"] == "day-ahead"
    interval = store.data_manager.query_data("time_interval")[0]
    assert interval["id"] == uuid.UUID(INTERVAL_ID)
    assert interval["market_id"] == uuid.UUID(MARKET_ID)
    assert interval["start_time"] == datetime(2026, 6, 9, 13, tzinfo=timezone.utc)
    assert interval["duration"] == timedelta(seconds=3600)


def test_single_envelope_value_is_accepted(connection):
    store = open_store(connection, SAMPLE_DEFINITIONS)
    result = store.write([envelope_record(market_envelopes()[0])])
    assert result.archive_rows == 1
    assert len(store.data_manager.query_data("market")) == 1


def test_unknown_table_envelopes_are_dropped_and_counted_not_raised(connection, caplog):
    store = open_store(connection, SAMPLE_DEFINITIONS)
    records = [
        envelope_record([{"table_name": "never_registered", "data": {"id": 1}}] + market_envelopes()[:1], record_id=1),
        standard_record(topic="record/example", value=1.5, record_id=2),
    ]
    with caplog.at_level(logging.ERROR, logger="historian.application.store"):
        result = store.write(records)
    assert result.skipped_unknown_table == {"never_registered": 1}
    assert result.archive_rows == 1 and result.standard_rows == 1
    assert any("never_registered" in r.message for r in caplog.records)
    assert store.topic_list() == ["record/example"]


def test_row_the_data_manager_quarantines_does_not_block_the_rest_of_the_batch(connection, caplog):
    store = open_store(connection, SAMPLE_DEFINITIONS)
    good, bad = market_envelopes()
    bad = {"table_name": "time_interval", "data": dict(bad["data"], start_time="2026-06-09T13:00:00")}  # naive
    with caplog.at_level(logging.ERROR):
        result = store.write([envelope_record([bad, good])])
    assert result.archive_rows == 2 and result.dropped == 0  # both handed over; TENT quarantined one of them
    assert len(store.data_manager.query_data("market")) == 1
    assert store.data_manager.query_data("time_interval") == []
    assert any("Quarantined" in r.message and "timezone-aware" in r.message for r in caplog.records)


def test_database_failure_propagates_and_leaves_no_duplicate_queue(connection, monkeypatch):
    store = open_store(connection, SAMPLE_DEFINITIONS)

    def failing_archive(records):
        store.data_manager.archival_queue.extend(records)  # what LocalDataManager does before raising
        raise OperationalError("INSERT", {}, Exception("database is locked"))

    monkeypatch.setattr(store.data_manager, "archive_data", failing_archive)
    with pytest.raises(OperationalError):
        store.write([envelope_record(market_envelopes())])
    assert len(store.data_manager.archival_queue) == 0


def test_write_before_setup_raises(connection):
    store = HistorianStore(connection, TABLE_NAMES)
    with pytest.raises(RuntimeError, match="setup"):
        store.write([standard_record()])


# ---------------------------------------------------------------- standard records and queries
def test_device_record_is_stored_and_queryable(connection):
    store = open_store(connection)
    topic = "devices/campus/building1/hvac/ZoneTemperature"
    ts = datetime(2026, 6, 9, 12, 0, tzinfo=timezone.utc)
    store.write([standard_record(topic=topic, value=71.5, meta={"units": "F", "type": "float"}, timestamp=ts,
                                 source="scrape")])

    assert store.topic_list() == [topic]
    assert store.topics_by_pattern("devices/campus/*/zonetemperature") == {topic: 1}
    assert store.topics_metadata([topic]) == {topic: {"units": "F", "type": "float"}}
    result = store.query([topic])
    assert result == {"values": [("2026-06-09T12:00:00.000000+00:00", 71.5)],
                      "metadata": {"units": "F", "type": "float"}}
    assert store.query([topic.upper()])["values"] == result["values"]  # topic lookup is case-insensitive


def test_query_window_order_count_skip_and_multiple_topics(connection):
    store = open_store(connection)
    base = datetime(2026, 6, 9, 0, 0, tzinfo=timezone.utc)
    records = [standard_record(topic="devices/a/power", value=i, timestamp=base + timedelta(hours=i), record_id=i)
               for i in range(5)]
    records.append(standard_record(topic="devices/b/power", value=99, timestamp=base, record_id=10))
    store.write(records)

    window = store.query(["devices/a/power"], start=base + timedelta(hours=1), end=base + timedelta(hours=3))
    assert [v for _, v in window["values"]] == [1, 2]  # start inclusive, end exclusive
    latest = store.query(["devices/a/power"], order="LAST_TO_FIRST", count=2)
    assert [v for _, v in latest["values"]] == [4, 3]
    skipped = store.query(["devices/a/power"], skip=3)
    assert [v for _, v in skipped["values"]] == [3, 4]
    multi = store.query(["devices/a/power", "devices/b/power", "devices/none"])
    assert set(multi["values"]) == {"devices/a/power", "devices/b/power"}
    assert multi["metadata"] == {}
    assert store.query(["devices/none"]) == {}
    assert store.query(["devices/a/power"], start=base + timedelta(days=1)) == {}


def test_standard_record_updates_metadata_and_keeps_one_row_per_timestamp(connection):
    store = open_store(connection)
    ts = datetime(2026, 6, 9, 12, 0, tzinfo=timezone.utc)
    store.write([standard_record(topic="record/x", value=1, meta={"units": "kW"}, timestamp=ts)])
    store.write([standard_record(topic="record/x", value=2, meta={"units": "MW"}, timestamp=ts)])
    result = store.query(["record/x"])
    assert result["values"] == [("2026-06-09T12:00:00.000000+00:00", 2)]
    assert result["metadata"] == {"units": "MW"}
    restarted = open_store(connection)
    assert restarted.topics_metadata(["record/x"]) == {"record/x": {"units": "MW"}}


def test_topic_pattern_is_a_glob_not_a_regex(connection):
    store = open_store(connection)
    store.write([standard_record(topic="devices/a/power(1)", record_id=1),
                 standard_record(topic="devices/a/power1", record_id=2)])
    assert store.topics_by_pattern("devices/a/power(1)") == {"devices/a/power(1)": 1}
    assert store.topics_by_pattern("devices/*/power?") == {"devices/a/power1": 2}
    assert store.topics_by_pattern("(a+)+$") == {}  # regex syntax is literal text here and matches nothing


def test_format_timestamp_matches_the_base_historian_form():
    assert format_timestamp(datetime(2026, 6, 9, 12, 0, 0, 5, tzinfo=timezone.utc)) == "2026-06-09T12:00:00.000005+00:00"
    assert format_timestamp(datetime(2026, 6, 9, 12, 0)) == "2026-06-09T12:00:00.000000"
    eastern = timezone(timedelta(hours=-5))
    assert format_timestamp(datetime(2026, 6, 9, 7, 0, tzinfo=eastern)) == "2026-06-09T07:00:00.000000-05:00"
