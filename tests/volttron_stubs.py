"""
Stand-ins for the VOLTTRON modules the historian agent imports, so the agent shell can be exercised without a platform.

Only what `historian.transactivate_node.transactive_node` touches is provided. `BaseHistorian.parse_table_def` is a
copy of the base historian's (volttron-lib-base-historian 2.0.0rc3) so table naming matches the real agent.
"""
import sys
import types
from copy import deepcopy


class BaseHistorian:

    def __init__(self, **kwargs):
        self.init_kwargs = kwargs
        self._default_config = {}
        self.handled_all = 0
        self.handled_records = []

    def update_default_config(self, config):
        self._default_config.update(config)

    def parse_table_def(self, tables_def):
        default_table_def = {"table_prefix": "", "data_table": "data", "topics_table": "topics", "meta_table": "meta"}
        if not tables_def:
            tables_def = default_table_def
        else:
            default_table_def.update(tables_def)
            tables_def = default_table_def
        table_names = dict(tables_def)
        table_prefix = tables_def.get('table_prefix', None)
        table_prefix = table_prefix + "_" if table_prefix else ""
        if table_prefix:
            for key, value in list(table_names.items()):
                table_names[key] = table_prefix + table_names[key]
        table_names["agg_topics_table"] = table_prefix + "aggregate_" + tables_def["topics_table"]
        table_names["agg_meta_table"] = table_prefix + "aggregate_" + tables_def["meta_table"]
        return tables_def, table_names

    def report_all_handled(self):
        self.handled_all += 1

    def report_handled(self, record):
        self.handled_records.append(record)


class RPC:
    exported = []

    @classmethod
    def export(cls, method=None, name=None):
        if method is None:
            return lambda m: cls.export(m, name)
        cls.exported.append(name or method.__name__)
        return method


def load_config(path):
    if path is None or path == "":
        return {}
    if isinstance(path, dict):
        return deepcopy(path)
    import json
    with open(path) as handle:
        return json.load(handle)


def vip_main(*args, **kwargs):
    raise RuntimeError("vip_main is not available in the test stubs")


def install():
    """Register the stub modules; a no-op for any already registered."""
    modules = {
        "historian.base": {},
        "historian.base.base_historian": {"BaseHistorian": BaseHistorian},
        "volttron": {},
        "volttron.client": {},
        "volttron.client.known_identities": {"PLATFORM_HISTORIAN": "platform.historian"},
        "volttron.client.logs": {"setup_logging": lambda *a, **k: None},
        "volttron.client.vip": {},
        "volttron.client.vip.agent": {"RPC": RPC},
        "volttron.utils": {"load_config": load_config, "vip_main": vip_main},
    }
    for name, attributes in modules.items():
        if name in sys.modules:
            continue
        module = types.ModuleType(name)
        for attribute, value in attributes.items():
            setattr(module, attribute, value)
        sys.modules[name] = module
