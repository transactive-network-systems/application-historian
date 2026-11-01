"""Table-definition parsing, allow-list validation and comparison."""
import json

import pytest
from tent.data_manager.reader import known_archive_definitions

from conftest import NODE_DEFINITIONS
from historian.transactivate_node.definitions import (TableDefinitionMismatch, canonical_json, describe_difference,
                                                      parse_definitions, validate_definitions)

RESERVED = {"topics", "data", "known_table_definitions"}


def validate(definitions, known=frozenset()):
    validate_definitions(definitions, reserved_names=RESERVED, known_names=set(known))


class TestParseDefinitions:

    def test_list_json_string_mapping_and_wrapper_forms(self):
        assert parse_definitions(NODE_DEFINITIONS) == NODE_DEFINITIONS
        assert parse_definitions(json.dumps(NODE_DEFINITIONS)) == NODE_DEFINITIONS
        assert parse_definitions({d["name"]: d for d in NODE_DEFINITIONS}) == NODE_DEFINITIONS
        assert parse_definitions({"definitions": NODE_DEFINITIONS}) == NODE_DEFINITIONS
        assert parse_definitions({"definitions": json.dumps(NODE_DEFINITIONS)}) == NODE_DEFINITIONS
        assert parse_definitions(None) == []

    @pytest.mark.parametrize("bad", [42, "not json", b"bytes"])
    def test_other_inputs_are_errors(self, bad):
        with pytest.raises((ValueError, TypeError)):
            parse_definitions(bad)


class TestValidateDefinitions:

    def test_every_definition_tent_ships_is_accepted(self):
        validate(known_archive_definitions())

    def test_test_schema_is_accepted(self):
        validate(NODE_DEFINITIONS)

    def test_empty_registration_is_an_error(self):
        with pytest.raises(ValueError, match="No table definitions"):
            validate([])

    @pytest.mark.parametrize("name", ["topics", "data", "known_table_definitions"])
    def test_reserved_names_are_refused(self, name):
        with pytest.raises(ValueError, match="reserved"):
            validate([{"name": name, "columns": [{"name": "x", "type": "Integer"}]}])

    @pytest.mark.parametrize("name", ['x"; DROP TABLE topics; --', "has space", "1starts_with_digit", "", None,
                                      "a" * 64])
    def test_table_names_must_be_identifiers(self, name):
        with pytest.raises(ValueError, match="identifier"):
            validate([{"name": name, "columns": [{"name": "x", "type": "Integer"}]}])

    def test_table_level_kwargs_are_refused(self):
        with pytest.raises(ValueError, match="unsupported definition key"):
            validate([{"name": "t", "columns": [{"name": "x", "type": "Integer"}], "kwargs": {"quote": False}}])
        with pytest.raises(ValueError, match="unsupported definition key"):
            validate([{"name": "t", "columns": [{"name": "x", "type": "Integer"}], "schema": "main"}])
        with pytest.raises(ValueError, match="unsupported definition key"):
            validate([{"name": "t", "columns": [{"name": "x", "type": "Integer"}], "prefixes": ["TEMPORARY"]}])

    @pytest.mark.parametrize("type_name", ["PickleType", "LargeBinary", "UserDefinedType", "TypeDecorator", "ARRAY"])
    def test_column_types_outside_the_allow_list_are_refused(self, type_name):
        with pytest.raises(ValueError, match="not allowed"):
            validate([{"name": "t", "columns": [{"name": "x", "type": type_name}]}])

    @pytest.mark.parametrize("kwargs", [{"quote": False}, {"key": "other"}, {"server_default": "now()"},
                                        {"primary_key": "yes"}, {"info": {}}])
    def test_column_kwargs_outside_the_allow_list_are_refused(self, kwargs):
        with pytest.raises(ValueError, match="column keyword"):
            validate([{"name": "t", "columns": [{"name": "x", "type": "Integer", "kwargs": kwargs}]}])

    def test_unknown_column_keys_are_refused(self):
        with pytest.raises(ValueError, match="unsupported key"):
            validate([{"name": "t", "columns": [{"name": "x", "type": "Integer", "quote": False}]}])

    def test_column_needs_a_type_enumeration_or_foreign_key(self):
        with pytest.raises(ValueError, match='needs a "type"'):
            validate([{"name": "t", "columns": [{"name": "x"}]}])

    def test_enumeration_must_exist_in_tent(self):
        with pytest.raises(ValueError, match="not an enumeration"):
            validate([{"name": "t", "columns": [{"name": "x", "enumeration": "NoSuchEnum"}]}])
        with pytest.raises(ValueError, match="not an enumeration"):
            validate([{"name": "t", "columns": [{"name": "x", "enumeration": "Enum"}]}])  # a class, not an enum

    @pytest.mark.parametrize("fk", ["component", "component.id.extra", "comp onent.id", 5])
    def test_foreign_key_must_be_table_dot_column(self, fk):
        with pytest.raises(ValueError, match="foreign_key"):
            validate([{"name": "t", "columns": [{"name": "x", "foreign_key": fk}]}])

    def test_type_args_must_be_scalars(self):
        with pytest.raises(ValueError, match="type_args"):
            validate([{"name": "t", "columns": [{"name": "x", "type": "String", "type_args": [{"length": 5}]}]}])
        with pytest.raises(ValueError, match="type_args"):
            validate([{"name": "t", "columns": [{"name": "x", "enumeration": "MarketTypes", "type_args": [5]}]}])

    def test_duplicates_are_refused(self):
        table = {"name": "t", "columns": [{"name": "x", "type": "Integer"}]}
        with pytest.raises(ValueError, match="defined twice"):
            validate([table, dict(table)])
        with pytest.raises(ValueError, match="defined twice"):
            validate([{"name": "t", "columns": [{"name": "x", "type": "Integer"}, {"name": "x", "type": "Text"}]}])

    def test_inherits_must_be_in_the_batch_or_already_registered(self):
        child = {"name": "child", "inherits": "parent", "columns": [{"name": "id", "foreign_key": "parent.id"}]}
        with pytest.raises(ValueError, match="inherits"):
            validate([child])
        validate([child], known={"parent"})
        parent = {"name": "parent", "columns": [{"name": "id", "type": "Integer", "kwargs": {"primary_key": True}}]}
        validate([child, parent])

    def test_indices_and_constraints_refer_to_real_columns(self):
        base = {"name": "t", "columns": [{"name": "a", "type": "Integer"}, {"name": "b", "type": "Integer"}]}
        validate([dict(base, indices=[{"name": "idx", "columns": ["a", "b"], "unique": True}])])
        with pytest.raises(ValueError, match="not a column"):
            validate([dict(base, indices=[{"name": "idx", "columns": ["a", "zzz"]}])])
        with pytest.raises(ValueError, match="index key"):
            validate([dict(base, indices=[{"name": "idx", "columns": ["a"], "postgresql_using": "gin"}])])
        validate([dict(base, constraints=[{"name": "fk", "foreign_key": {"columns": ["a", "b"],
                                                                         "references": ["o.a", "o.b"]}}])])
        with pytest.raises(ValueError, match="differ in length"):
            validate([dict(base, constraints=[{"foreign_key": {"columns": ["a"], "references": ["o.a", "o.b"]}}])])
        with pytest.raises(ValueError, match="unsupported constraint key"):
            validate([dict(base, constraints=[{"name": "ck", "check": "a > 0"}])])


class TestComparison:

    def test_canonical_json_ignores_key_order_only(self):
        definition = NODE_DEFINITIONS[1]
        reordered_keys = {key: definition[key] for key in reversed(list(definition))}
        assert canonical_json(reordered_keys) == canonical_json(definition)
        reordered_columns = dict(definition, columns=list(reversed(definition["columns"])))
        assert canonical_json(reordered_columns) != canonical_json(definition)

    def test_difference_names_the_changed_lines(self):
        stored = {"name": "t", "columns": [{"name": "x", "type": "Interval"}]}
        offered = {"name": "t", "columns": [{"name": "x", "type": "Float"}]}
        diff = describe_difference(stored, offered)
        assert '-   "type": "Interval"' in diff
        assert '+   "type": "Float"' in diff
        assert describe_difference(stored, dict(stored)) == ""

    def test_mismatch_error_carries_each_difference(self):
        error = TableDefinitionMismatch({"t": "diff text", "u": "other diff"})
        assert error.differences == {"t": "diff text", "u": "other diff"}
        assert "t, u" in str(error) and "diff text" in str(error)
