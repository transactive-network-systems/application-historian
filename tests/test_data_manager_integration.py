import json
from datetime import datetime, timezone
from unittest.mock import Mock

import pytest

from historian.transactivate_node.transactive_node import TransactiveNodeHistorian


def make_historian(data_manager):
    historian = TransactiveNodeHistorian.__new__(TransactiveNodeHistorian)
    historian.connection = {}
    historian.table_names = {
        "data_table": "data",
        "topics_table": "topics",
        "meta_table": "meta",
        "agg_topics_table": "aggregate_topics",
        "agg_meta_table": "aggregate_meta",
    }
    historian.configured_table_definitions = []
    historian.dynamic_tables = set()
    historian.topic_id_map = {}
    historian.topic_name_map = {}
    historian.topic_meta = {}
    historian.data_manager = data_manager
    historian.report_all_handled = Mock()
    return historian


def standard_record(value=42.5, topic="record/example", metadata=None):
    return {
        "_id": 1,
        "timestamp": datetime(2026, 6, 9, tzinfo=timezone.utc),
        "source": "record",
        "topic": topic,
        "value": value,
        "meta": metadata or {},
        "headers": {},
    }


def test_historian_setup_creates_standard_and_configured_tables():
    data_manager = Mock()
    data_manager.query_data.return_value = []
    historian = make_historian(data_manager)
    definition = {"name": "orders", "columns": [{"name": "id", "type": "Integer"}]}
    historian.configured_table_definitions = [definition]

    historian.historian_setup()

    standard_definitions = data_manager.add_tables.call_args_list[0].args[0]
    assert {definition["name"] for definition in standard_definitions} == {"topics", "data"}
    data_manager.add_tables.assert_any_call([definition])
    data_manager.init_archive.assert_called_once_with()
    assert historian.dynamic_tables == {"orders"}


def test_create_tables_accepts_volttron_data_manager_mapping():
    data_manager = Mock()
    historian = make_historian(data_manager)
    definition = {"name": "orders", "columns": [{"name": "id", "type": "Integer"}]}

    result = historian.create_tables(json.dumps({"orders": definition}))

    assert result["status"] == "success"
    data_manager.add_tables.assert_called_once_with([definition])
    data_manager.init_archive.assert_called_once_with()
    assert historian.dynamic_tables == {"orders"}


def test_normal_record_writes_standard_tables_and_is_acknowledged():
    data_manager = Mock()
    data_manager.upsert_data.side_effect = [
        {"topic_id": 7, "topic_name": "record/example", "metadata": "{}"},
        {"ts": datetime(2026, 6, 9, tzinfo=timezone.utc), "topic_id": 7, "value_string": "42.5"},
    ]
    historian = make_historian(data_manager)
    record = standard_record(metadata={"units": "kW"})

    historian.publish_to_historian([record])

    assert data_manager.upsert_data.call_count == 2
    assert data_manager.upsert_data.call_args_list[0].args[0] == "topics"
    assert data_manager.upsert_data.call_args_list[1].args[0] == "data"
    data_manager.archive_data.assert_not_called()
    historian.report_all_handled.assert_called_once_with()


def test_dynamic_envelope_writes_only_registered_custom_table_on_any_topic():
    data_manager = Mock()
    historian = make_historian(data_manager)
    historian.dynamic_tables.add("orders")
    record = standard_record(
        value={"table_name": "orders", "data": {"id": 4, "total": 20.0}},
        topic="record/not-special",
    )

    historian.publish_to_historian([record])

    data_manager.upsert_data.assert_not_called()
    data_manager.archive_data.assert_called_once_with([
        {"table_name": "orders", "data": {"id": 4, "total": 20.0}}
    ])
    historian.report_all_handled.assert_called_once_with()


def test_dynamic_envelope_list_writes_all_custom_records():
    data_manager = Mock()
    historian = make_historian(data_manager)
    historian.dynamic_tables.update({"orders", "prices"})
    record = standard_record(value=[
        {"table_name": "orders", "data": {"id": 4}},
        {"table_name": "prices", "data": {"id": 5}},
    ])

    historian.publish_to_historian([record])

    data_manager.archive_data.assert_called_once_with([
        {"table_name": "orders", "data": {"id": 4}},
        {"table_name": "prices", "data": {"id": 5}},
    ])


def test_unknown_dynamic_table_raises_without_writing_or_acknowledging():
    data_manager = Mock()
    historian = make_historian(data_manager)
    record = standard_record(value={"table_name": "missing", "data": {"id": 4}})

    with pytest.raises(KeyError, match="missing"):
        historian.publish_to_historian([record])

    data_manager.upsert_data.assert_not_called()
    data_manager.archive_data.assert_not_called()
    historian.report_all_handled.assert_not_called()


def test_query_historian_returns_standard_values_and_metadata():
    data_manager = Mock()
    timestamp = datetime(2026, 6, 9, tzinfo=timezone.utc)
    data_manager.query_data.return_value = [{"ts": timestamp, "topic_id": 7, "value_string": "42.5"}]
    historian = make_historian(data_manager)
    historian.topic_id_map["record/example"] = 7
    historian.topic_name_map["record/example"] = "record/example"
    historian.topic_meta[7] = {"units": "kW"}

    result = historian.query_historian("record/example")

    assert result["values"][0][1] == 42.5
    assert result["metadata"] == {"units": "kW"}
