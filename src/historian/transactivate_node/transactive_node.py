#!/usr/bin/env python
"""A modular VOLTTRON historian backed by TENT's LocalDataManager."""

import json
import logging
import re
import sys
from collections.abc import Mapping

from historian.base.base_historian import BaseHistorian
from tent.data_manager.local_data_manager import LocalDataManager
from volttron.client.logs import setup_logging
from volttron.client.vip.agent import RPC
from volttron.utils import format_timestamp, jsonapi, load_config, vip_main

__version__ = "3.0.0"

setup_logging()
_log = logging.getLogger(__name__)


def historian(config_path, **kwargs):
    """Build the historian from a VOLTTRON agent configuration."""
    config = load_config(config_path) or {}
    connection = config.pop(
        "connection",
        {"params": {"url": "sqlite:///historian_data.sqlite"}},
    )
    tables_def = config.pop("tables_def", None)
    table_definitions = config.pop("table_definitions", [])
    kwargs.update(config)
    return TransactiveNodeHistorian(
        connection=connection,
        tables_def=tables_def,
        table_definitions=table_definitions,
        **kwargs,
    )


class TransactiveNodeHistorian(BaseHistorian):
    """Store standard VOLTTRON records and registered structured records."""

    def __init__(self, connection, tables_def=None, table_definitions=None, **kwargs):
        self.connection = connection or {}
        self.tables_def, self.table_names = self.parse_table_def(tables_def)
        self.configured_table_definitions = self._parse_definitions(table_definitions)
        self.dynamic_tables = set()
        self.topic_id_map = {}
        self.topic_name_map = {}
        self.topic_meta = {}
        self.data_manager = LocalDataManager(self, self.connection.get("params"), 100)
        super().__init__(**kwargs)

    @staticmethod
    def _parse_definitions(definitions):
        if not definitions:
            return []
        if isinstance(definitions, str):
            definitions = json.loads(definitions)
        if isinstance(definitions, Mapping) and "definitions" in definitions:
            definitions = definitions["definitions"]
            if isinstance(definitions, str):
                definitions = json.loads(definitions)
        if isinstance(definitions, Mapping):
            definitions = list(definitions.values())
        if not isinstance(definitions, list):
            raise ValueError("Table definitions must be a list or mapping")
        return definitions

    def _standard_table_definitions(self):
        topics_table = self.table_names["topics_table"]
        data_table = self.table_names["data_table"]
        return [
            {
                "name": topics_table,
                "columns": [
                    {
                        "name": "topic_id",
                        "type": "Integer",
                        "kwargs": {"primary_key": True, "autoincrement": True},
                    },
                    {
                        "name": "topic_name",
                        "type": "String",
                        "kwargs": {"nullable": False, "unique": True},
                    },
                    {"name": "metadata", "type": "Text"},
                ],
            },
            {
                "name": data_table,
                "columns": [
                    {
                        "name": "ts",
                        "type": "DateTime",
                        "kwargs": {"nullable": False, "primary_key": True},
                    },
                    {
                        "name": "topic_id",
                        "type": "Integer",
                        "foreign_key": f"{topics_table}.topic_id",
                        "kwargs": {"nullable": False, "primary_key": True},
                    },
                    {"name": "value_string", "type": "Text", "kwargs": {"nullable": False}},
                ],
            },
        ]

    def historian_setup(self):
        """Create configured tables and load the standard historian caches."""
        self.data_manager.add_tables(self._standard_table_definitions())
        if self.configured_table_definitions:
            self.data_manager.add_tables(self.configured_table_definitions)
            self.dynamic_tables.update(
                definition["name"] for definition in self.configured_table_definitions
            )
        self.data_manager.init_archive()
        self._load_topic_cache()

    def _load_topic_cache(self):
        self.topic_id_map.clear()
        self.topic_name_map.clear()
        self.topic_meta.clear()
        for row in self.data_manager.query_data(self.table_names["topics_table"]):
            topic_key = row["topic_name"].lower()
            self.topic_id_map[topic_key] = row["topic_id"]
            self.topic_name_map[topic_key] = row["topic_name"]
            self.topic_meta[row["topic_id"]] = jsonapi.loads(row["metadata"] or "{}")

    @RPC.export
    def create_tables(self, definitions):
        """Register table definitions supplied by a TENT VolttronDataManager."""
        try:
            parsed = self._parse_definitions(definitions)
            self.data_manager.add_tables(parsed)
            self.data_manager.init_archive()
            self.dynamic_tables.update(definition["name"] for definition in parsed)
            return {
                "status": "success",
                "registered_tables": sorted(self.dynamic_tables),
            }
        except Exception as exc:
            _log.exception("Unable to register table definitions")
            return {"status": "error", "message": str(exc)}

    def publish_to_historian(self, to_publish_list):
        """Persist a normalized BaseHistorian batch."""
        dynamic_records = []
        standard_records = []

        for record in to_publish_list:
            envelopes = self._dynamic_envelopes(record.get("value"))
            if envelopes is None:
                standard_records.append(record)
                continue
            for envelope in envelopes:
                table_name = envelope["table_name"]
                if table_name not in self.dynamic_tables:
                    raise KeyError(f"Dynamic table has not been registered: {table_name}")
                dynamic_records.append(
                    {"table_name": table_name, "data": dict(envelope["data"])}
                )

        for record in standard_records:
            topic_id = self._upsert_topic(record["topic"], record.get("meta", {}))
            self.data_manager.upsert_data(
                self.table_names["data_table"],
                {
                    "ts": record["timestamp"],
                    "topic_id": topic_id,
                    "value_string": jsonapi.dumps(record.get("value")),
                },
                ["topic_id", "ts"],
            )

        if dynamic_records:
            self.data_manager.archive_data(dynamic_records)
        self.report_all_handled()

    @staticmethod
    def _dynamic_envelopes(value):
        if TransactiveNodeHistorian._is_dynamic_envelope(value):
            return [value]
        if isinstance(value, list) and value and all(
            TransactiveNodeHistorian._is_dynamic_envelope(item) for item in value
        ):
            return value
        return None

    @staticmethod
    def _is_dynamic_envelope(value):
        return (
            isinstance(value, Mapping)
            and isinstance(value.get("table_name"), str)
            and isinstance(value.get("data"), Mapping)
        )

    def _upsert_topic(self, topic, metadata):
        topic_key = topic.lower()
        topic_id = self.topic_id_map.get(topic_key)
        serialized_meta = jsonapi.dumps(metadata or {})

        if topic_id is None:
            row = self.data_manager.upsert_data(
                self.table_names["topics_table"],
                {
                    "topic_name": topic,
                    "metadata": serialized_meta,
                },
                ["topic_name"],
            )
            topic_id = row["topic_id"]
            self.topic_id_map[topic_key] = topic_id
        elif self.topic_name_map.get(topic_key) != topic or self.topic_meta.get(topic_id) != metadata:
            self.data_manager.upsert_data(
                self.table_names["topics_table"],
                {
                    "topic_id": topic_id,
                    "topic_name": topic,
                    "metadata": serialized_meta,
                },
                ["topic_id"],
            )

        self.topic_name_map[topic_key] = topic
        self.topic_meta[topic_id] = metadata or {}
        return topic_id

    def query_topic_list(self):
        return list(self.topic_name_map.values())

    def query_topics_by_pattern(self, topic_pattern):
        pattern = re.compile(topic_pattern, re.IGNORECASE)
        return {
            topic_name: self.topic_id_map[topic_key]
            for topic_key, topic_name in self.topic_name_map.items()
            if pattern.search(topic_name)
        }

    def query_topics_metadata(self, topics):
        topics = [topics] if isinstance(topics, str) else topics
        return {
            topic: self.topic_meta[self.topic_id_map[topic.lower()]]
            for topic in topics
            if topic.lower() in self.topic_id_map
        }

    def query_aggregate_topics(self):
        return []

    def query_historian(self, topic, start=None, end=None, agg_type=None, agg_period=None,
                        skip=0, count=None, order="FIRST_TO_LAST"):
        if agg_type or agg_period:
            return {}

        topics = [topic] if isinstance(topic, str) else topic
        multi_topic = len(topics) > 1
        values = {}
        conditions = {}
        if start is not None:
            conditions.setdefault("ts", {})["gte"] = start
        if end is not None:
            conditions.setdefault("ts", {})["lt"] = end

        for topic_name in topics:
            topic_id = self.topic_id_map.get(topic_name.lower())
            if topic_id is None:
                continue
            topic_conditions = dict(conditions)
            topic_conditions["topic_id"] = topic_id
            rows = self.data_manager.query_data(
                self.table_names["data_table"],
                conditions=topic_conditions,
                order_by=[("ts", "desc" if order == "LAST_TO_FIRST" else "asc")],
                limit=count,
                offset=skip,
            )
            values[topic_name] = [
                (format_timestamp(row["ts"]), jsonapi.loads(row["value_string"]))
                for row in rows
            ]

        if multi_topic:
            return {"values": values, "metadata": {}} if values else {}
        if not values:
            return {}
        topic_name = next(iter(values))
        topic_id = self.topic_id_map[topic_name.lower()]
        return {
            "values": values[topic_name],
            "metadata": self.topic_meta.get(topic_id, {}),
        }

    def version(self):
        return __version__


# Preserve the repository's existing import name.
TNSHistorian2 = TransactiveNodeHistorian


def main():
    try:
        vip_main(historian, version=__version__)
    except Exception:
        _log.exception("Historian error")
        raise


if __name__ == "__main__":
    sys.exit(main())
