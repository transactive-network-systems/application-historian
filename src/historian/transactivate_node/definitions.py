"""
Table definitions the historian accepts over the `create_tables` RPC and from its own configuration.

A definition is the JSON structure TENT components return from `get_archive_definitions()` and
`tent.data_manager.DataManager._parse_table_definition` turns into a SQLAlchemy `Table`. Because the RPC lets a peer
run DDL in the historian's database, every key is checked against an allow-list before it reaches SQLAlchemy: column
types are limited to the names below, column keyword arguments to a few booleans, and the table-level keyword arguments
SQLAlchemy would otherwise pass straight to `Table(...)` (`quote`, `prefixes`, `schema`, ...) are not accepted at all.

`check_definitions` reports a problem per table, so a registration with one bad table can still register the others.

The canonical form (`canonical_form`, `canonical_json`) is what the historian compares when a table is registered a
second time and what `describe_difference` diffs: keys sorted, columns sorted by name. Column order therefore does not
matter for the comparison; the table is created with the columns in the order the first registration gave them.
"""
import difflib
import enum
import json
import re
from collections.abc import Mapping, Sequence

import tent.enumerations

IDENTIFIER = re.compile(r'^[A-Za-z_][A-Za-z0-9_]{0,62}$')
QUALIFIED_COLUMN = re.compile(r'^[A-Za-z_][A-Za-z0-9_]{0,62}\.[A-Za-z_][A-Za-z0-9_]{0,62}$')

TABLE_KEYS = {'name', 'columns', 'inherits', 'indices', 'constraints'}
# `nullable` at the top level of a column appears in TENT's market definition; TENT's parser does not read it (only
# kwargs.nullable has an effect), so it is accepted here to keep that definition registrable, and passed through.
COLUMN_KEYS = {'name', 'type', 'type_args', 'enumeration', 'foreign_key', 'identity', 'kwargs', 'nullable'}
COLUMN_TYPES = {'BigInteger', 'Boolean', 'Date', 'DateTime', 'Float', 'Integer', 'Interval', 'JSON', 'Numeric',
                'SmallInteger', 'String', 'Text', 'Time', 'Uuid'}
COLUMN_KWARGS = {'primary_key', 'nullable', 'unique', 'autoincrement', 'index'}  # all booleans
INDEX_KEYS = {'name', 'columns', 'unique'}
CONSTRAINT_KEYS = {'name', 'foreign_key'}
FOREIGN_KEY_CONSTRAINT_KEYS = {'columns', 'references'}


def parse_definitions(definitions) -> list[dict]:
    """
    Accept the forms callers send: a list of definitions, a JSON string of one, a {name: definition} mapping, or a
    {"definitions": ...} wrapper. Anything else is an error.
    """
    if definitions is None:
        return []
    if isinstance(definitions, str):
        definitions = json.loads(definitions)
    if isinstance(definitions, Mapping) and 'definitions' in definitions:
        definitions = definitions['definitions']
        if isinstance(definitions, str):
            definitions = json.loads(definitions)
    if isinstance(definitions, Mapping):
        definitions = list(definitions.values())
    if not isinstance(definitions, Sequence) or isinstance(definitions, (str, bytes)):
        raise ValueError(f'Table definitions must be a list of definitions or a mapping of name to definition, '
                         f'not {type(definitions).__name__}.')
    return [dict(d) if isinstance(d, Mapping) else d for d in definitions]


def canonical_form(definition: Mapping) -> dict:
    """The definition with its columns sorted by name (key order is handled by sort_keys when serialising)."""
    canonical = dict(definition)
    columns = canonical.get('columns')
    if isinstance(columns, Sequence) and not isinstance(columns, (str, bytes)):
        canonical['columns'] = sorted((dict(c) for c in columns if isinstance(c, Mapping)),
                                      key=lambda c: str(c.get('name')))
    return canonical


def canonical_json(definition: Mapping) -> str:
    return json.dumps(canonical_form(definition), sort_keys=True, separators=(',', ':'))


def describe_difference(stored: Mapping, offered: Mapping) -> str:
    """A unified diff of the two definitions' pretty-printed canonical forms, for logs and RPC error replies."""
    before = json.dumps(canonical_form(stored), sort_keys=True, indent=1).splitlines()
    after = json.dumps(canonical_form(offered), sort_keys=True, indent=1).splitlines()
    return '\n'.join(difflib.unified_diff(before, after, fromfile='registered', tofile='offered', lineterm=''))


def check_definitions(definitions: Sequence, reserved_names: set[str],
                      known_names: set[str]) -> tuple[list[dict], dict[str, str]]:
    """
    Check each definition on its own and return (valid definitions in the order given, {table: reason} for the
    rejected ones). A definition without a usable name is keyed "#<position>". `reserved_names` are the historian's
    own tables, which may not be defined; `known_names` are tables already registered, which `inherits` may refer to.
    A table whose parent is rejected, or is neither in the batch nor known, is rejected too.
    """
    valid: list[dict] = []
    rejected: dict[str, str] = {}
    names_in_batch: set[str] = set()
    for position, definition in enumerate(definitions):
        name = definition.get('name') if isinstance(definition, Mapping) else None
        key = name if isinstance(name, str) and IDENTIFIER.match(name) else f'#{position}'
        try:
            if not isinstance(definition, Mapping):
                raise ValueError(f'Each table definition must be a mapping, not {type(definition).__name__}.')
            _validate_table(definition, reserved_names)
            if name in names_in_batch:
                raise ValueError(f'Table {name!r} is defined twice in one registration.')
        except ValueError as error:
            rejected[key] = str(error)
            continue
        names_in_batch.add(name)
        valid.append(dict(definition))
    # Resolve references after every table has been checked; a rejected parent or target takes its dependants with it.
    reject_unresolved(valid, rejected, known_names)
    return valid, rejected


def dependencies(definition: Mapping) -> set[str]:
    """Tables a definition needs to exist: its `inherits` parent and every foreign-key target other than itself."""
    needed = set()
    if definition.get('inherits'):
        needed.add(definition['inherits'])
    for column in definition.get('columns', []):
        if isinstance(column, Mapping) and isinstance(column.get('foreign_key'), str):
            needed.add(column['foreign_key'].split('.')[0])
    for constraint in definition.get('constraints', []):
        if isinstance(constraint, Mapping) and isinstance(constraint.get('foreign_key'), Mapping):
            for reference in constraint['foreign_key'].get('references', []):
                if isinstance(reference, str):
                    needed.add(reference.split('.')[0])
    needed.discard(definition.get('name'))
    return needed


def reject_unresolved(valid: list[dict], rejected: dict[str, str], available: set[str]) -> None:
    """
    Move every definition in `valid` whose dependencies are neither in `available` nor among the other valid
    definitions into `rejected` (in place), repeating until nothing changes so chains of dependants fall together.
    """
    changed = True
    while changed:
        changed = False
        accepted = {d['name'] for d in valid}
        for definition in list(valid):
            missing = dependencies(definition) - available - accepted
            if not missing:
                continue
            name = definition['name']
            reasons = []
            for target in sorted(missing):
                if target in rejected:
                    reasons.append(f'{target!r}, which was rejected')
                else:
                    reasons.append(f'{target!r}, which is neither in this registration nor already registered')
            rejected[name] = f'Table {name!r} refers to ' + ' and '.join(reasons) + '.'
            valid.remove(definition)
            changed = True


def _validate_table(definition: Mapping, reserved_names: set[str]) -> None:
    name = definition.get('name')
    _require_identifier(name, 'table name')
    if name in reserved_names:
        raise ValueError(f'Table {name!r} is reserved by the historian and cannot be registered.')
    unexpected = set(definition) - TABLE_KEYS
    if unexpected:
        raise ValueError(f'Table {name!r}: unsupported definition key(s) {sorted(unexpected)}. '
                         f'Allowed: {sorted(TABLE_KEYS)}.')
    columns = definition.get('columns')
    if not isinstance(columns, Sequence) or isinstance(columns, (str, bytes)) or not columns:
        raise ValueError(f'Table {name!r}: "columns" must be a non-empty list.')
    column_names: set[str] = set()
    for column in columns:
        _validate_column(name, column)
        if column['name'] in column_names:
            raise ValueError(f'Table {name!r}: column {column["name"]!r} is defined twice.')
        column_names.add(column['name'])
    parent = definition.get('inherits')
    if parent is not None:
        _require_identifier(parent, f'table {name!r}: "inherits"')
    for index in definition.get('indices', []):
        _validate_index(name, index, column_names)
    for constraint in definition.get('constraints', []):
        _validate_constraint(name, constraint, column_names)


def _require_identifier(value, what: str) -> None:
    if not isinstance(value, str) or not IDENTIFIER.match(value):
        raise ValueError(f'{what} must be an identifier (letters, digits, underscore; at most 63 characters), '
                         f'got {value!r}.')


def _validate_column(table: str, column) -> None:
    if not isinstance(column, Mapping):
        raise ValueError(f'Table {table!r}: each column must be a mapping, not {type(column).__name__}.')
    name = column.get('name')
    _require_identifier(name, f'table {table!r}: column name')
    where = f'Table {table!r}, column {name!r}'
    unexpected = set(column) - COLUMN_KEYS
    if unexpected:
        raise ValueError(f'{where}: unsupported key(s) {sorted(unexpected)}. Allowed: {sorted(COLUMN_KEYS)}.')
    type_name = column.get('type')
    if type_name is not None and type_name not in COLUMN_TYPES:
        raise ValueError(f'{where}: column type {type_name!r} is not allowed. Allowed: {sorted(COLUMN_TYPES)}.')
    type_args = column.get('type_args')
    if type_args is not None:
        if type_name is None:
            raise ValueError(f'{where}: "type_args" given without "type".')
        if (not isinstance(type_args, Sequence) or isinstance(type_args, (str, bytes))
                or not all(isinstance(a, (int, str)) and not isinstance(a, bool) for a in type_args)):
            raise ValueError(f'{where}: "type_args" must be a list of integers or strings.')
    enumeration = column.get('enumeration')
    if enumeration is not None:
        _require_identifier(enumeration, f'{where}: "enumeration"')
        enum_class = getattr(tent.enumerations, enumeration, None)
        if not (isinstance(enum_class, type) and issubclass(enum_class, enum.Enum)):
            raise ValueError(f'{where}: {enumeration!r} is not an enumeration in tent.enumerations.')
    foreign_key = column.get('foreign_key')
    if foreign_key is not None and not (isinstance(foreign_key, str) and QUALIFIED_COLUMN.match(foreign_key)):
        raise ValueError(f'{where}: "foreign_key" must be "table.column", got {foreign_key!r}.')
    if type_name is None and enumeration is None and foreign_key is None:
        raise ValueError(f'{where}: a column needs a "type", an "enumeration" or a "foreign_key".')
    if 'identity' in column and not isinstance(column['identity'], bool):
        raise ValueError(f'{where}: "identity" must be a boolean.')
    if 'nullable' in column and not isinstance(column['nullable'], bool):
        raise ValueError(f'{where}: "nullable" must be a boolean.')
    kwargs = column.get('kwargs')
    if kwargs is not None:
        if not isinstance(kwargs, Mapping):
            raise ValueError(f'{where}: "kwargs" must be a mapping.')
        unexpected = set(kwargs) - COLUMN_KWARGS
        if unexpected:
            raise ValueError(f'{where}: column keyword(s) {sorted(unexpected)} are not allowed. '
                             f'Allowed: {sorted(COLUMN_KWARGS)}.')
        for key, value in kwargs.items():
            if not isinstance(value, bool):
                raise ValueError(f'{where}: column keyword {key!r} must be a boolean, got {value!r}.')


def _validate_index(table: str, index, column_names: set[str]) -> None:
    if not isinstance(index, Mapping):
        raise ValueError(f'Table {table!r}: each index must be a mapping.')
    unexpected = set(index) - INDEX_KEYS
    if unexpected:
        raise ValueError(f'Table {table!r}: unsupported index key(s) {sorted(unexpected)}.')
    _require_identifier(index.get('name'), f'table {table!r}: index name')
    columns = index.get('columns')
    if not isinstance(columns, Sequence) or isinstance(columns, (str, bytes)) or not columns:
        raise ValueError(f'Table {table!r}, index {index["name"]!r}: "columns" must be a non-empty list.')
    for column in columns:
        if column not in column_names:
            raise ValueError(f'Table {table!r}, index {index["name"]!r}: {column!r} is not a column of the table.')
    if 'unique' in index and not isinstance(index['unique'], bool):
        raise ValueError(f'Table {table!r}, index {index["name"]!r}: "unique" must be a boolean.')


def _validate_constraint(table: str, constraint, column_names: set[str]) -> None:
    if not isinstance(constraint, Mapping):
        raise ValueError(f'Table {table!r}: each constraint must be a mapping.')
    unexpected = set(constraint) - CONSTRAINT_KEYS
    if unexpected:
        raise ValueError(f'Table {table!r}: unsupported constraint key(s) {sorted(unexpected)}.')
    if 'name' in constraint:
        _require_identifier(constraint['name'], f'table {table!r}: constraint name')
    foreign_key = constraint.get('foreign_key')
    if not isinstance(foreign_key, Mapping):
        raise ValueError(f'Table {table!r}: only {{"foreign_key": {{"columns": [...], "references": [...]}}}} '
                         f'constraints are supported.')
    unexpected = set(foreign_key) - FOREIGN_KEY_CONSTRAINT_KEYS
    if unexpected:
        raise ValueError(f'Table {table!r}: unsupported foreign-key constraint key(s) {sorted(unexpected)}.')
    columns, references = foreign_key.get('columns'), foreign_key.get('references')
    for label, values in (('columns', columns), ('references', references)):
        if not isinstance(values, Sequence) or isinstance(values, (str, bytes)) or not values:
            raise ValueError(f'Table {table!r}: foreign-key constraint "{label}" must be a non-empty list.')
    if len(columns) != len(references):
        raise ValueError(f'Table {table!r}: foreign-key constraint columns and references differ in length.')
    for column in columns:
        if column not in column_names:
            raise ValueError(f'Table {table!r}: foreign-key constraint names {column!r}, not a column of the table.')
    for reference in references:
        if not (isinstance(reference, str) and QUALIFIED_COLUMN.match(reference)):
            raise ValueError(f'Table {table!r}: foreign-key reference must be "table.column", got {reference!r}.')
