"""Table-definition and enumeration parsing, allow-list validation and comparison."""
import json

import pytest

from conftest import SAMPLE_DEFINITIONS, SAMPLE_ENUMERATIONS, registration
from historian.application.definitions import (Registration, canonical_form, canonical_json, check_definitions,
                                               check_enumerations, describe_difference, parse_definitions,
                                               parse_registration)

RESERVED = {"topics", "data", "known_table_definitions", "known_enumerations", "archive_schema"}
ENUMERATIONS = {"MarketTypes"}


def check(definitions, known=frozenset()):
    return check_definitions(definitions, reserved_names=RESERVED, known_names=set(known), enumerations=ENUMERATIONS)


def validate(definitions, known=frozenset()):
    """Every definition must pass; the first rejection is raised so the allow-list tests read as assertions."""
    valid, rejected = check(definitions, known)
    if rejected:
        raise ValueError(next(iter(rejected.values())))
    assert len(valid) == len(definitions)


class TestParseDefinitions:

    def test_list_json_string_mapping_and_wrapper_forms(self):
        assert parse_definitions(SAMPLE_DEFINITIONS) == SAMPLE_DEFINITIONS
        assert parse_definitions(json.dumps(SAMPLE_DEFINITIONS)) == SAMPLE_DEFINITIONS
        assert parse_definitions({d["name"]: d for d in SAMPLE_DEFINITIONS}) == SAMPLE_DEFINITIONS
        assert parse_definitions({"definitions": SAMPLE_DEFINITIONS}) == SAMPLE_DEFINITIONS
        assert parse_definitions({"definitions": json.dumps(SAMPLE_DEFINITIONS)}) == SAMPLE_DEFINITIONS
        assert parse_definitions(None) == []

    @pytest.mark.parametrize("bad", [42, "not json", b"bytes"])
    def test_other_inputs_are_errors(self, bad):
        with pytest.raises((ValueError, TypeError)):
            parse_definitions(bad)


class TestParseRegistration:

    def test_a_full_payload_is_taken_apart(self):
        parsed = parse_registration(registration())
        assert parsed == Registration(SAMPLE_DEFINITIONS, SAMPLE_ENUMERATIONS,
                                      {"application": "demo", "schema_version": 3, "fingerprint": "d" * 64,
                                       "application_version": "1.2.3"})
        assert parse_registration(json.dumps(registration())) == parsed

    def test_a_bare_list_or_mapping_carries_no_schema_and_no_enumerations(self):
        for payload in (SAMPLE_DEFINITIONS, {d["name"]: d for d in SAMPLE_DEFINITIONS}, {"definitions": SAMPLE_DEFINITIONS}):
            parsed = parse_registration(payload)
            assert parsed.definitions == SAMPLE_DEFINITIONS and parsed.enumerations is None and parsed.schema == {}

    def test_enumerations_must_be_a_list(self):
        with pytest.raises(ValueError, match='"enumerations" must be a list'):
            parse_registration(registration(enumerations={"MarketTypes": ["auction"]}))


class TestCheckEnumerations:

    def test_canonical_form_and_none(self):
        assert check_enumerations(SAMPLE_ENUMERATIONS) == SAMPLE_ENUMERATIONS
        assert check_enumerations(None) == [] and check_enumerations([]) == []

    @pytest.mark.parametrize("bad, problem", [
        ([{"name": "MarketTypes"}], "non-empty list of member names"),
        ([{"name": "MarketTypes", "members": []}], "non-empty list"),
        ([{"name": "Market Types", "members": ["a"]}], "identifier"),
        ([{"name": "MarketTypes", "members": ["a", "a"]}], "twice"),
        ([{"name": "MarketTypes", "members": ["a"], "values": [1]}], "unsupported key"),
        (["MarketTypes"], "mapping"),
        ("MarketTypes", "must be a list"),
        ([{"name": "MarketTypes", "members": ["a"]}, {"name": "MarketTypes", "members": ["b"]}], "defined twice"),
    ])
    def test_malformed_enumerations_are_errors(self, bad, problem):
        with pytest.raises(ValueError, match=problem):
            check_enumerations(bad)


class TestValidateDefinitions:

    def test_test_schema_is_accepted(self):
        validate(SAMPLE_DEFINITIONS)

    def test_empty_registration_accepts_nothing(self):
        assert check([]) == ([], {})

    def test_each_table_is_checked_on_its_own(self):
        good = {"name": "good", "columns": [{"name": "x", "type": "Integer"}]}
        bad = {"name": "bad", "columns": [{"name": "x", "type": "PickleType"}]}
        valid, rejected = check([bad, good, "not a mapping", {"columns": []}])
        assert valid == [good]
        assert set(rejected) == {"bad", "#2", "#3"}
        assert "PickleType" in rejected["bad"]
        assert "mapping" in rejected["#2"]
        assert "identifier" in rejected["#3"]

    def test_child_of_a_rejected_parent_is_rejected(self):
        parent = {"name": "parent", "columns": [{"name": "id", "type": "PickleType", "kwargs": {"primary_key": True}}]}
        child = {"name": "child", "inherits": "parent", "columns": [{"name": "id", "foreign_key": "parent.id"}]}
        grandchild = {"name": "grandchild", "inherits": "child", "columns": [{"name": "id", "foreign_key": "child.id"}]}
        other = {"name": "other", "columns": [{"name": "x", "type": "Integer"}]}
        valid, rejected = check([grandchild, child, parent, other])
        assert valid == [other]
        assert "was rejected" in rejected["child"] and "was rejected" in rejected["grandchild"]

    @pytest.mark.parametrize("name", ["topics", "data", "known_table_definitions", "known_enumerations",
                                      "archive_schema"])
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

    def test_enumeration_must_be_in_the_registration_or_registered(self):
        with pytest.raises(ValueError, match="names enumeration 'NoSuchEnum', which is neither in this registration's "
                                             "enumerations nor already registered"):
            validate([{"name": "t", "columns": [{"name": "x", "enumeration": "NoSuchEnum"}]}])
        valid, rejected = check_definitions([{"name": "t", "columns": [{"name": "x", "enumeration": "MarketTypes"}]}],
                                            reserved_names=RESERVED, known_names=set())  # no enumerations at all
        assert valid == [] and "neither in this registration" in rejected["t"]
        with pytest.raises(ValueError, match="identifier"):
            validate([{"name": "t", "columns": [{"name": "x", "enumeration": "not an identifier"}]}])

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
        valid, rejected = check([table, dict(table)])
        assert valid == [table] and "defined twice" in rejected["t"]
        with pytest.raises(ValueError, match="defined twice"):
            validate([{"name": "t", "columns": [{"name": "x", "type": "Integer"}, {"name": "x", "type": "Text"}]}])

    def test_inherits_must_be_in_the_batch_or_already_registered(self):
        child = {"name": "child", "inherits": "parent", "columns": [{"name": "id", "foreign_key": "parent.id"}]}
        with pytest.raises(ValueError, match="refers to 'parent'"):
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
        constraint = {"name": "fk", "foreign_key": {"columns": ["a", "b"], "references": ["o.a", "o.b"]}}
        validate([dict(base, constraints=[constraint])], known={"o"})
        with pytest.raises(ValueError, match="refers to 'o'"):
            validate([dict(base, constraints=[constraint])])
        with pytest.raises(ValueError, match="differ in length"):
            validate([dict(base, constraints=[{"foreign_key": {"columns": ["a"], "references": ["o.a", "o.b"]}}])],
                     known={"o"})
        with pytest.raises(ValueError, match="unsupported constraint key"):
            validate([dict(base, constraints=[{"name": "ck", "check": "a > 0"}])])


class TestComparison:

    def test_canonical_form_ignores_key_and_column_order(self):
        definition = SAMPLE_DEFINITIONS[1]
        reordered_keys = {key: definition[key] for key in reversed(list(definition))}
        assert canonical_json(reordered_keys) == canonical_json(definition)
        reordered_columns = dict(definition, columns=list(reversed(definition["columns"])))
        assert canonical_json(reordered_columns) == canonical_json(definition)
        assert [c["name"] for c in canonical_form(definition)["columns"]] == sorted(c["name"] for c in
                                                                                     definition["columns"])
        assert definition["columns"][0]["name"] == "id"  # the given definition is not modified

    def test_canonical_form_sees_a_changed_column(self):
        definition = SAMPLE_DEFINITIONS[2]
        changed = dict(definition, columns=definition["columns"][:-1] + [{"name": "duration", "type": "Float"}])
        assert canonical_json(changed) != canonical_json(definition)
        renamed = dict(definition, columns=definition["columns"][:-1] + [{"name": "length", "type": "Interval"}])
        assert canonical_json(renamed) != canonical_json(definition)

    def test_difference_names_the_changed_lines_and_ignores_column_order(self):
        stored = {"name": "t", "columns": [{"name": "x", "type": "Interval"}, {"name": "a", "type": "Text"}]}
        offered = {"name": "t", "columns": [{"name": "a", "type": "Text"}, {"name": "x", "type": "Float"}]}
        diff = describe_difference(stored, offered)
        assert '-   "type": "Interval"' in diff
        assert '+   "type": "Float"' in diff
        assert '"a"' not in "".join(line for line in diff.splitlines() if line[:1] in "+-" and line[:3] not in
                                      ("---", "+++"))
        assert describe_difference(stored, dict(stored, columns=list(reversed(stored["columns"])))) == ""
