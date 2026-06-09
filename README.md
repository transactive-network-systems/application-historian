# Transactive Node Historian

This modular VOLTTRON historian extends `BaseHistorian` and uses TENT's
`LocalDataManager` for all database access.

## Behavior

- Normal VOLTTRON topics are stored in the standard `topics` and `data`
  historian tables and are available through the standard historian query
  RPCs.
- Structured archive envelopes are written only to dynamically registered
  tables.
- Routing is based on payload shape, not topic name.
- SQLAlchemy and database creation are owned by `LocalDataManager`.

The dynamic archive envelope is:

```json
{
  "table_name": "registered_table",
  "data": {
    "id": 1
  }
}
```

TENT registers its table definitions using the `create_tables` RPC before
publishing archive envelopes. Definitions are registered again after either
TENT or the historian restarts.

## Configuration

```json
{
  "connection": {
    "params": {
      "url": "sqlite:///historian_data.sqlite"
    }
  },
  "tables_def": {
    "data_table": "data",
    "topics_table": "topics"
  },
  "table_definitions": [],
  "custom_topics": {
    "capture_record_data": ["TNS/"]
  }
}
```

`custom_topics` determines which deployment-specific topics `BaseHistorian`
subscribes to. It does not affect storage routing.

Install TENT before installing the agent:

```shell
vctl install-lib /path/to/TENT
vctl install /path/to/volttron-transactive-node-historian \
  --vip-identity tns.historian \
  --agent-config config.json \
  --start
```
