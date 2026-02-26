# az-table-catalog

A resilient, event-sourced indexing library for Azure Table Storage. 

`az-table-catalog` allows you to create high-performance, multi-indexed lookup tables without the high minimum monthly fees of Cosmos DB. It uses a Write-Ahead Log (WAL) and a checkpoint-driven recovery model to ensure data consistency even during process crashes.

## Why TableCatalog?
Azure Table Storage is extremely cost-effective but lacks native secondary indexes. This library stores one row per (index, record) pair, encoding the index dimension directly into the PartitionKey.

* **Multi-Index Queries**: Search by any field with O(1) performance.
* **Write-Ahead Log**: Prevents "split-brain" states during multi-partition writes.
* **Range Queries**: Filter index partitions by ordered fields (like timestamps).
* **Self-Healing**: Automatically replays orphaned WAL entries on the next write.

## Quick Start

```python
import az_table_catalog

# Configure schema
az_table_catalog.configure(
    index_keys=["phone", "email"],
    row_key="timestamp"
)

# Insert returns the committed record
user = az_table_catalog.insert({
    "phone": "555-634-5789",
    "email": "user@example.com",
    "timestamp": "2026-02-24T12:00:00Z"
})

# Query any index
results = az_table_catalog.query({"phone": "555-634-5789"})

```

## Configuration

The library can be initialized via `configure()` or these environment variables:

| Variable | Required | Description |
| --- | --- | --- |
| `AZURE_STORAGE_CONNECTION_STRING` | Yes | Azure Storage connection string |
| `TABLE_CATALOG_NAME` | Yes | Primary catalog table name |
| `TABLE_CATALOG_INDEX_KEYS` | Yes | Comma-separated list of indexed fields |
| `TABLE_CATALOG_ROW_KEY` | Yes | Field used as the sort key (RowKey base) |

## Technical Implementation

* **PartitionKey**: Formatted as `{len(field)}_{field}{value}` to prevent collisions.
* **RowKey**: Derived from the `row_key` value and an 8-character MD5 fingerprint of all indexed fields for idempotency.

## License

MIT