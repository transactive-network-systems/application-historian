"""
Test fixtures. The tests never start a VOLTTRON platform: the store tests use TENT and SQLAlchemy against a SQLite file
in a temporary directory, and the agent tests import the agent module with the VOLTTRON modules replaced by the
stand-ins in volttron_stubs.py (installed here, before the agent module is imported anywhere).
"""
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest
from tent.data_manager import ARCHIVE_SCHEMA_VERSION

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import volttron_stubs  # noqa: E402

volttron_stubs.install()

# A small TENT-shaped schema: joined inheritance (market -> component), a UUID identity key, an enumeration, a
# timezone-aware datetime, an interval and a boolean, which together cover every branch of the normaliser.
NODE_DEFINITIONS = [
    {
        "name": "component",
        "columns": [
            {"name": "id", "type": "Uuid", "identity": True, "kwargs": {"primary_key": True}},
            {"name": "name", "type": "String", "type_args": [255]},
        ],
    },
    {
        "name": "market",
        "inherits": "component",
        "columns": [
            {"name": "id", "foreign_key": "component.id", "kwargs": {"primary_key": True}},
            {"name": "market_type", "enumeration": "MarketTypes"},
            {"name": "delivery_start_time", "type": "DateTime"},
            {"name": "active", "type": "Boolean"},
        ],
    },
    {
        "name": "time_interval",
        "columns": [
            {"name": "id", "type": "Uuid", "kwargs": {"primary_key": True}},
            {"name": "market_id", "foreign_key": "market.id"},
            {"name": "start_time", "type": "DateTime"},
            {"name": "duration", "type": "Interval"},
        ],
        "indices": [{"name": "idx_interval_start", "columns": ["market_id", "start_time"], "unique": True}],
    },
]

TABLE_NAMES = {"topics_table": "topics", "data_table": "data"}


def registration(definitions=NODE_DEFINITIONS, schema_version=ARCHIVE_SCHEMA_VERSION):
    """The create_tables argument a node sends: its definitions beside its TENT's archive schema version."""
    return {"definitions": definitions, "schema_version": schema_version}


@pytest.fixture
def db_url(tmp_path):
    return f"sqlite:///{tmp_path / 'historian.sqlite'}"


@pytest.fixture
def connection(db_url):
    return {"params": {"url": db_url}}


def standard_record(topic="record/example", value=42.5, meta=None, timestamp=None, source="record", record_id=1):
    """A record as BaseHistorian hands it to publish_to_historian."""
    return {
        "_id": record_id,
        "timestamp": timestamp or datetime(2026, 6, 9, 12, 0, tzinfo=timezone.utc),
        "source": source,
        "topic": topic,
        "value": value,
        "meta": meta or {},
        "headers": {},
    }


def envelope_record(envelopes, topic="tns/archive/campus/records", record_id=1):
    """What the historian receives when a node's VolttronDataManager publishes a batch of envelopes."""
    return standard_record(topic=topic, value=envelopes, record_id=record_id)
