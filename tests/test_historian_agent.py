"""
The VOLTTRON shell: construction, configure() from the config store, historian_setup, the create_tables RPC,
publish_to_historian and the query contract. BaseHistorian and the volttron modules are the stand-ins from
volttron_stubs.py; the database is a real SQLite file.
"""
import uuid
from datetime import datetime, timezone

import pytest
from sqlalchemy import create_engine, inspect

from conftest import NODE_DEFINITIONS, envelope_record, standard_record
from historian.transactivate_node import transactive_node
from historian.transactivate_node.store import KNOWN_TABLE_DEFINITIONS
from historian.transactivate_node.transactive_node import (DEFAULT_VIP_IDENTITY, TransactiveNodeHistorian,
                                                           historian)

MARKET_ID = "0190a3c0-0000-7000-8000-000000000001"


def running_historian(connection, **config):
    """An agent after the config store delivered `config` and the processing loop ran historian_setup."""
    agent = TransactiveNodeHistorian()
    agent.configure({"connection": connection, **config})
    agent.historian_setup()
    return agent


def market_envelope(market_id=MARKET_ID, name="day-ahead"):
    return {"table_name": "market", "data": {"id": market_id, "name": name, "market_type": "consensus",
                                             "delivery_start_time": "2026-06-09T13:00:00+00:00", "active": False}}


# ---------------------------------------------------------------- construction and configuration (H-01)
def test_default_identity_is_the_platform_historian():
    assert DEFAULT_VIP_IDENTITY == "platform.historian"


def test_processing_loop_runs_in_a_greenlet():
    agent = TransactiveNodeHistorian()
    assert agent.init_kwargs["process_loop_in_greenlet"] is True
    with pytest.raises(ValueError, match="process_loop_in_greenlet"):
        TransactiveNodeHistorian(process_loop_in_greenlet=False)


def test_agent_config_values_become_the_config_store_defaults(connection):
    agent = historian({"connection": connection, "tables_def": {"table_prefix": "p"}, "retry_period": 10})
    assert agent._default_config["connection"] == connection
    assert agent._default_config["tables_def"] == {"table_prefix": "p"}
    assert agent._default_config["table_definitions"] == []
    assert agent.init_kwargs["retry_period"] == 10
    assert agent.table_names["data_table"] == "p_data"


def test_invalid_url_in_agent_config_fails_at_construction():
    with pytest.raises(ValueError, match="sqlite:////home/vuzer/db/historian.sqlite"):
        historian({"connection": {"params": {"url": "/home/vuzer/db/historian.sqlite"}}})


def test_configure_builds_the_store_from_the_connection_block(connection, db_url):
    agent = TransactiveNodeHistorian()
    assert agent.store is None
    agent.configure({"connection": connection, "tables_def": None, "table_definitions": None})
    assert agent.store is not None and agent.store.url == db_url
    assert agent.connection == connection
    assert not agent.store.ready  # the database is opened on the processing loop, in historian_setup


def test_configure_rejects_a_bare_path_loudly(db_url):
    agent = TransactiveNodeHistorian()
    with pytest.raises(ValueError) as error:
        agent.configure({"connection": {"params": {"url": "/home/vuzer/db/historian.sqlite"}}})
    assert "sqlite:////home/vuzer/db/historian.sqlite" in str(error.value)
    assert agent.store is None


def test_configure_without_connection_raises_with_instructions():
    agent = TransactiveNodeHistorian()
    with pytest.raises(ValueError, match="vctl config store"):
        agent.configure({"connection": None, "tables_def": None, "table_definitions": None})


def test_reconfigure_switches_databases(connection, tmp_path):
    agent = running_historian(connection)
    agent.publish_to_historian([standard_record(topic="record/first")])
    other = {"params": {"url": f"sqlite:///{tmp_path / 'other.sqlite'}"}}
    agent.configure({"connection": other})
    agent.historian_setup()
    assert agent.query_topic_list() == []
    agent.publish_to_historian([standard_record(topic="record/second")])
    assert agent.query_topic_list() == ["record/second"]


def test_setup_and_publish_before_configure_fail_loudly():
    agent = TransactiveNodeHistorian()
    with pytest.raises(RuntimeError, match="not configured"):
        agent.historian_setup()
    with pytest.raises(RuntimeError, match="not open"):
        agent.publish_to_historian([standard_record()])
    assert agent.handled_all == 0


def test_configured_table_definitions_are_created_at_setup(connection, db_url):
    agent = running_historian(connection, table_definitions=NODE_DEFINITIONS)
    assert {"component", "market", "time_interval"} <= set(inspect(create_engine(db_url)).get_table_names())
    assert agent.store.registered_tables == {"component", "market", "time_interval"}


# ---------------------------------------------------------------- create_tables RPC (H-02, H-07, H-SEC-01)
def test_create_tables_is_exported_as_an_rpc():
    assert "create_tables" in transactive_node.RPC.exported


def test_create_tables_registers_persists_and_reports(connection, db_url):
    agent = running_historian(connection)
    reply = agent.create_tables(NODE_DEFINITIONS)
    assert reply["status"] == "success"
    assert reply["registered_tables"] == ["component", "market", "time_interval"]
    assert reply["rejected_tables"] == {}
    assert "3 table(s) created" in reply["message"]
    assert set(reply) == {"status", "registered_tables", "rejected_tables", "message"}
    assert KNOWN_TABLE_DEFINITIONS in inspect(create_engine(db_url)).get_table_names()
    again = agent.create_tables(NODE_DEFINITIONS)  # a second node registering the same schema
    assert again["status"] == "success" and again["registered_tables"] == ["component", "market", "time_interval"]
    assert "identical" in again["message"]


def test_create_tables_accepts_the_mapping_form(connection):
    agent = running_historian(connection)
    reply = agent.create_tables({d["name"]: d for d in NODE_DEFINITIONS})
    assert reply["status"] == "success"


def test_create_tables_reports_a_changed_definition_and_registers_the_rest(connection):
    agent = running_historian(connection)
    agent.create_tables(NODE_DEFINITIONS)
    changed = dict(NODE_DEFINITIONS[2], columns=NODE_DEFINITIONS[2]["columns"][:-1] + [{"name": "duration",
                                                                                         "type": "Float"}])
    reply = agent.create_tables([NODE_DEFINITIONS[1], changed])
    assert reply["status"] == "partial"
    assert reply["registered_tables"] == ["market"]
    assert set(reply["rejected_tables"]) == {"time_interval"}
    assert '+   "type": "Float"' in reply["rejected_tables"]["time_interval"]["differences"]
    assert "time_interval" in reply["message"]
    alone = agent.create_tables([changed])
    assert alone["status"] == "error" and alone["registered_tables"] == []


def test_create_tables_with_reordered_columns_is_a_success(connection):
    agent = running_historian(connection)
    agent.create_tables(NODE_DEFINITIONS)
    reordered = [dict(d, columns=list(reversed(d["columns"]))) for d in NODE_DEFINITIONS]
    reply = agent.create_tables(reordered)
    assert reply["status"] == "success" and reply["rejected_tables"] == {}


def test_create_tables_rejects_disallowed_definitions(connection):
    agent = running_historian(connection)
    reply = agent.create_tables([{"name": "evil", "columns": [{"name": "x", "type": "Integer"}],
                                  "kwargs": {"quote": False}}])
    assert reply["status"] == "error" and "unsupported definition key" in reply["rejected_tables"]["evil"]["message"]
    reply = agent.create_tables([{"name": "topics", "columns": [{"name": "x", "type": "Integer"}]}])
    assert reply["status"] == "error" and "reserved" in reply["rejected_tables"]["topics"]["message"]
    reply = agent.create_tables("not json")
    assert reply["status"] == "error" and reply["rejected_tables"] == {}
    assert agent.store.registered_tables == set()


def test_create_tables_before_the_database_is_open_is_an_error_reply(connection):
    agent = TransactiveNodeHistorian()
    assert agent.create_tables(NODE_DEFINITIONS)["status"] == "error"
    agent.configure({"connection": connection})
    assert agent.create_tables(NODE_DEFINITIONS)["status"] == "error"


# ---------------------------------------------------------------- publishing (H-02, H-INC-01)
def test_envelopes_are_stored_and_the_batch_is_acknowledged(connection):
    agent = running_historian(connection)
    agent.create_tables(NODE_DEFINITIONS)
    agent.publish_to_historian([envelope_record([market_envelope()])])
    assert agent.handled_all == 1
    rows = agent.store.data_manager.query_data("market")
    assert rows[0]["id"] == uuid.UUID(MARKET_ID)
    assert rows[0]["active"] is False


def test_non_envelope_on_the_records_topic_is_dropped_not_stored_as_a_record(connection, caplog):
    agent = running_historian(connection)
    agent.publish_to_historian([standard_record(topic="tns/archive/campus/records", value={"not": "an envelope"})])
    assert agent.handled_all == 1
    assert agent.query_topic_list() == []
    assert any("not an archive envelope" in r.message for r in caplog.records)


def test_transactive_signal_topics_are_outside_the_archive_prefix():
    """The deployed capture prefix is tns/archive/; node-to-node signals on tns/<sender>/<receiver> are not captured."""
    import json
    from pathlib import Path
    config = json.loads((Path(__file__).resolve().parents[1] / "config.json").read_text())
    prefixes = config["custom_topics"]["capture_record_data"]
    assert prefixes == ["tns/archive/"]
    for signal_topic in ("tns/campus/city", "tns/city/campus", "tns/archive"):
        assert not any(signal_topic.startswith(prefix) for prefix in prefixes)
    assert TransactiveNodeHistorian.parse_archive_topic("tns/campus/city") is None
    assert TransactiveNodeHistorian.parse_archive_topic("tns/archive/campus/records") == ("tns", "campus", "records")
    assert TransactiveNodeHistorian.parse_archive_topic("tns/archive//records") is None


def test_unknown_table_is_dropped_and_the_batch_is_still_acknowledged(connection, caplog):
    agent = running_historian(connection)
    agent.publish_to_historian([envelope_record([{"table_name": "nope", "data": {"id": 1}}])])
    assert agent.handled_all == 1
    assert any("nope" in record.message for record in caplog.records)


def test_restart_keeps_tables_registered(connection):
    first = running_historian(connection)
    first.create_tables(NODE_DEFINITIONS)
    first.publish_to_historian([envelope_record([market_envelope()])])

    restarted = running_historian(connection)  # a new process: no create_tables call from the still-running node
    assert restarted.store.registered_tables == {"component", "market", "time_interval"}
    restarted.publish_to_historian([envelope_record([market_envelope("0190a3c0-0000-7000-8000-000000000009", "rt")])])
    assert restarted.handled_all == 1
    assert {row["name"] for row in restarted.store.data_manager.query_data("component")} == {"day-ahead", "rt"}


def test_device_record_path_end_to_end(connection):
    agent = running_historian(connection)
    topic = "devices/campus/building1/meter/WholeBuildingPower"
    ts = datetime(2026, 6, 9, 12, 0, tzinfo=timezone.utc)
    agent.publish_to_historian([standard_record(topic=topic, value=120.5, meta={"units": "kW"}, timestamp=ts,
                                                source="scrape")])
    assert agent.handled_all == 1
    assert agent.query_topic_list() == [topic]
    assert agent.query_topics_by_pattern("devices/campus/*") == {topic: 1}
    assert agent.query_topics_metadata(topic) == {topic: {"units": "kW"}}
    result = agent.query_historian(topic, start=ts, end=None, skip=0, count=None, order="FIRST_TO_LAST")
    assert result == {"values": [("2026-06-09T12:00:00.000000+00:00", 120.5)], "metadata": {"units": "kW"}}
    assert agent.query_historian([topic, "devices/none"])["values"] == {topic: result["values"]}


def test_mixed_batch_routes_by_payload_shape(connection):
    agent = running_historian(connection)
    agent.create_tables(NODE_DEFINITIONS)
    agent.publish_to_historian([
        standard_record(topic="tns/other", value={"not": "an envelope"}, record_id=1),
        envelope_record([market_envelope()], record_id=2),
        standard_record(topic="record/plain", value=3, record_id=3),
    ])
    assert agent.query_topic_list() == ["tns/other", "record/plain"]
    assert len(agent.store.data_manager.query_data("market")) == 1


# ---------------------------------------------------------------- pubsub registration path
def definitions_message(definitions=NODE_DEFINITIONS, node="campus", request_id="req-1"):
    return standard_record(topic=f"tns/archive/{node}/definitions",
                           value={"node": node, "request_id": request_id, "definitions": definitions})


def test_definitions_message_registers_and_publishes_success(connection):
    agent = running_historian(connection)
    agent.publish_to_historian([definitions_message()])
    assert agent.handled_all == 1
    assert agent.store.registered_tables == {"component", "market", "time_interval"}
    [(topic, headers, message)] = agent.vip.pubsub.published
    assert topic == "tns/archive/campus/status"
    assert headers["Date"] and headers["TimeStamp"]
    assert message == {"request_id": "req-1", "node": "campus", "status": "success",
                       "registered_tables": ["component", "market", "time_interval"], "rejected_tables": {},
                       "message": message["message"]}
    agent.publish_to_historian([definitions_message(request_id="req-2")])  # idempotent
    assert agent.vip.pubsub.published[-1][2]["status"] == "success"
    assert agent.vip.pubsub.published[-1][2]["request_id"] == "req-2"


def test_definitions_then_records_in_one_batch_are_stored(connection):
    agent = running_historian(connection)
    agent.publish_to_historian([definitions_message(), envelope_record([market_envelope()], record_id=2)])
    assert len(agent.store.data_manager.query_data("market")) == 1


def test_definitions_message_partial_and_error_status(connection):
    agent = running_historian(connection)
    agent.publish_to_historian([definitions_message()])
    changed = dict(NODE_DEFINITIONS[2], columns=NODE_DEFINITIONS[2]["columns"][:-1] + [{"name": "duration",
                                                                                         "type": "Float"}])
    agent.publish_to_historian([definitions_message([NODE_DEFINITIONS[0], changed], request_id="req-p")])
    status = agent.vip.pubsub.published[-1][2]
    assert status["status"] == "partial" and status["request_id"] == "req-p"
    assert status["registered_tables"] == ["component"]
    assert '+   "type": "Float"' in status["rejected_tables"]["time_interval"]["differences"]

    agent.publish_to_historian([definitions_message([changed], request_id="req-e")])
    status = agent.vip.pubsub.published[-1][2]
    assert status["status"] == "error" and status["registered_tables"] == []
    assert set(status["rejected_tables"]) == {"time_interval"}


def test_malformed_definitions_message_gets_an_error_status_only_with_a_request_id(connection, caplog):
    agent = running_historian(connection)
    agent.publish_to_historian([standard_record(topic="tns/archive/campus/definitions", value="garbage")])
    assert agent.vip.pubsub.published == []
    assert agent.handled_all == 1
    assert any("Malformed definitions message" in r.message for r in caplog.records)

    agent.publish_to_historian([standard_record(topic="tns/archive/campus/definitions",
                                                value={"request_id": "req-m", "node": "campus"})])
    [(topic, _, message)] = agent.vip.pubsub.published
    assert topic == "tns/archive/campus/status"
    assert message["status"] == "error" and message["request_id"] == "req-m" and message["registered_tables"] == []
    assert "Malformed" in message["message"]
    assert agent.query_topic_list() == []  # neither message was stored as a record


def test_own_status_publication_is_ignored(connection):
    agent = running_historian(connection)
    status = {"request_id": "req-1", "node": "campus", "status": "success", "registered_tables": [],
              "rejected_tables": {}, "message": "x"}
    agent.publish_to_historian([standard_record(topic="tns/archive/campus/status", value=status)])
    assert agent.handled_all == 1
    assert agent.query_topic_list() == []
    assert agent.vip.pubsub.published == []


def test_status_publish_failure_is_logged_not_raised(connection, caplog):
    agent = running_historian(connection)

    def failing_publish(*args, **kwargs):
        raise RuntimeError("bus down")

    agent.vip.pubsub.publish = failing_publish
    agent.publish_to_historian([definitions_message()])
    assert agent.handled_all == 1
    assert agent.store.registered_tables == {"component", "market", "time_interval"}
    assert any("Could not publish the registration status" in r.message for r in caplog.records)


def test_records_topic_with_a_different_prefix_is_still_routed(connection):
    agent = running_historian(connection)
    agent.publish_to_historian([standard_record(topic="other/prefix/archive/city/definitions",
                                                value={"node": "city", "request_id": "r", "definitions":
                                                       NODE_DEFINITIONS}),
                                envelope_record([market_envelope()], topic="other/prefix/archive/city/records",
                                                record_id=2)])
    assert agent.vip.pubsub.published[0][0] == "other/prefix/archive/city/status"
    assert len(agent.store.data_manager.query_data("market")) == 1


# ---------------------------------------------------------------- unsupported queries
def test_aggregate_queries_are_refused_not_empty(connection):
    agent = running_historian(connection)
    with pytest.raises(NotImplementedError, match="aggregate"):
        agent.query_historian("devices/x", agg_type="avg", agg_period="1h")
    assert agent.query_aggregate_topics() == []


def test_version():
    assert TransactiveNodeHistorian().version() == transactive_node.__version__
