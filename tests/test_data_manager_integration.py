import json
from unittest.mock import Mock

from historian.transactivate_node.transactive_node import TNSHistorian2


def make_historian(data_manager):
    historian = TNSHistorian2.__new__(TNSHistorian2)
    historian.connection = {}
    historian.data_manager = data_manager
    historian.tracked_tables = set()
    historian.unknown_table_initialized = False
    return historian


def test_ensure_table_exists_uses_public_data_manager_api():
    data_manager = Mock()
    data_manager.has_table.return_value = False
    historian = make_historian(data_manager)
    columns = [{"name": "id", "type": "Integer"}]

    historian._ensure_table_exists("orders", columns)

    data_manager.has_table.assert_called_once_with("orders")
    data_manager.register_tables.assert_called_once_with(
        [{"name": "orders", "columns": columns}]
    )


def test_create_tables_accepts_volttron_data_manager_definitions_mapping():
    data_manager = Mock()
    historian = make_historian(data_manager)
    definition = {
        "name": "orders",
        "columns": [{"name": "id", "type": "Integer"}],
    }

    result = historian.create_tables(json.dumps({"orders": definition}))

    assert result["status"] == "success"
    data_manager.register_tables.assert_called_once_with([definition])
    assert historian.tracked_tables == {"orders"}


def test_unknown_table_registration_uses_public_data_manager_api():
    data_manager = Mock()
    historian = make_historian(data_manager)

    historian._ensure_unknown_table_exists()
    historian._ensure_unknown_table_exists()

    data_manager.register_tables.assert_called_once()
