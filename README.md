# TNSHistorian

TNSHistorian is a VOLTTRON historian agent designed to store TNS (Transactive Node System) data records in a SQLite database (or other SQL databases, with appropriate configuration). It leverages the VOLTTRON BaseHistorian for standard historian features and adds support for custom table registration and storage using a local data manager.

## Features

- Subscribes to `TNS/` topics and stores complete record data in custom tables.
- Allows dynamic registration of tables via RPC (`register_table`).
- Stores records for unknown/unregistered tables in a special `unknown_table_records` table.
- Uses a local data manager for flexible table creation and data archiving.
- Falls back to a writable agent directory if the configured database path is not available.
- Continues to support standard historian topics and caching via BaseHistorian.

## Configuration

The agent is configured via a JSON file, typically named `config.json`. The most important section is the `connection` block, which specifies the database type and connection parameters.

Example:

```json
{
    "connection": {
        "type": "sqlite",
        "params": {
            "url": "sqlite:///historian_data.sqlite"
        }
    },
    "all_platforms": true
}
```

- The `"url"` parameter specifies the SQLite database file location. If the directory does not exist or is not writable, the agent will fall back to a database file in its own working directory.
- For other SQL databases (MySQL, PostgreSQL, etc.), adjust the `"type"` and `"params"` accordingly.

## Table Registration and Data Storage

- Use the `register_table` RPC method to define new tables for TNS data. Example columns definition:

    ```python
    [
        {"name": "id", "type": "Integer", "kwargs": {"primary_key": True, "autoincrement": True}},
        {"name": "source", "type": "String"},
        {"name": "topic", "type": "String"},
        {"name": "timestamp", "type": "DateTime"},
        {"name": "value", "type": "Float"},
        {"name": "meta", "type": "JSON"}
    ]
    ```

- Once a table is registered, all incoming records for that table will be stored using the local data manager.
- If a record arrives for an unregistered table, it will be stored in the `unknown_table_records` table, with the original table name and data preserved.

## Handling of Other Topics

- The TNSHistorian continues to process and cache all other topics (such as `record/*`, `devices/*`, etc.) using the standard VOLTTRON BaseHistorian logic.
- Only `TNS/` topics are handled with custom table logic.

## Example Usage

1. Configure the agent with your desired database path in `config.json`.
2. Start the agent.
3. Register tables as needed via RPC.
4. Publish data to `TNS/your_table_name` topics.
5. Query your SQLite (or other SQL) database to view stored records.

## Notes

- Table names must be unique.
- The agent will attempt to create the database directory if it does not exist.
- If the configured path is not writable, the agent will use a fallback path in its own directory.
- The agent is designed to be generic and suitable for open-source distribution.

