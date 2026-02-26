"""
az_table_catalog.py
-------------------
Azure Table Storage-backed generic catalog index library.
"""

import hashlib
import logging
import os
import uuid
from datetime import datetime, timezone
from azure.data.tables import TableServiceClient
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError

logger = logging.getLogger(__name__)

# ------------------------------------------------------------------
# Environment Helper
# ------------------------------------------------------------------

def _get_env(name: str, default: str | None = None) -> str:
    """Helper to fetch environment variables or raise an error."""
    value = os.environ.get(name)
    if not value:
        if default is not None:
            return default
        raise EnvironmentError(f"Required environment variable '{name}' is not set.")
    return value

# ------------------------------------------------------------------
# Internal Helpers
# ------------------------------------------------------------------

def _make_tx_row_key() -> str:
    """Lexicographically sortable row key for WAL entries."""
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")
    return f"{ts}_{uuid.uuid4()}"

def _entity_to_payload(entity) -> dict:
    """Strips Azure metadata to return only user payload fields."""
    return {k: v for k, v in entity.items() if k not in ["PartitionKey", "RowKey", "Timestamp", "etag"]}

def _partition_key(field: str, value: str) -> str:
    """Builds a normalized, collision-resistant partition key."""
    return f"{len(field)}_{field}{value}".lower()

def _row_key(record: dict, row_key_value: str, index_keys: list[str]) -> str:
    """Creates a deterministic row key with a content fingerprint."""
    index_values = "|".join(str(record[k]).lower() for k in sorted(index_keys))
    fingerprint = hashlib.md5(index_values.encode()).hexdigest()[:8]
    return f"{row_key_value}:{fingerprint}"

# ------------------------------------------------------------------
# Public Client Class
# ------------------------------------------------------------------

class TableCatalogClient:
    def __init__(self,
        connection_string: str | None = None,
        table_name:        str | None = None,
        wal_name:          str | None = None,
        index_keys:        str | list[str] | None = None,
        row_key:           str | None = None,
    ) -> None:
        # Resolve Infrastructure
        connection_string = connection_string or _get_env("AZURE_STORAGE_CONNECTION_STRING")
        table_name        = table_name        or _get_env("TABLE_CATALOG_NAME")
        wal_name          = wal_name          or _get_env("TABLE_CATALOG_WAL_NAME", table_name + "_WAL")
        
        # Pull schema from env if not provided
        index_keys        = index_keys        or _get_env("TABLE_CATALOG_INDEX_KEYS")
        row_key           = row_key           or _get_env("TABLE_CATALOG_ROW_KEY")

        self._index_keys:    list[str] | None = None
        self._row_key:       str | None       = None
        self._schema_locked: bool             = False
        
        # Configure the schema immediately
        self.configure(index_keys, row_key)
        
        service    = TableServiceClient.from_connection_string(connection_string)
        self.table = service.create_table_if_not_exists(table_name)
        self.wal   = service.create_table_if_not_exists(wal_name)

    def configure(self, index_keys: str | list[str], row_key: str) -> None:
        if self._schema_locked:
            raise RuntimeError("Schema is already configured and locked.")
            
        if isinstance(index_keys, str):
            index_keys = [k.strip() for k in index_keys.split(",") if k.strip()]

        if not index_keys or not row_key:
            raise ValueError("Both index_keys and row_key must be provided.")

        self._index_keys    = index_keys
        self._row_key       = row_key
        self._schema_locked = True

    def _require_schema(self) -> None:
        if not self._schema_locked:
            raise RuntimeError("Schema is not configured.")

    def query(self, filter: dict, *, row_from: str | None = None, row_to: str | None = None) -> list[dict]:
        self._require_schema()
        items = iter(filter.items())
        
        # Process first filter
        field, value = next(items)
        field, value = self._validate_filter({field: value})
        odata = f"PartitionKey eq '{_partition_key(field, value)}'"
        if row_from:
            odata += f" and RowKey ge '{row_from.lower()}:'"
        if row_to:
            odata += f" and RowKey le '{row_to.lower()}:z'"
            
        entities = self.table.query_entities(odata)
        results = [_entity_to_payload(e) for e in entities]

        # Process subsequent filters against existing results
        for field, value in items:
            if not results: break
            matches = self.query({field: value}, row_from=row_from, row_to=row_to)
            results = [r for r in results if r in matches]

        return results

    def insert(self, record: dict) -> dict:
        self._require_schema()
        missing = [k for k in self._index_keys + [self._row_key] if k not in record]
        if missing:
            raise ValueError(f"insert: record is missing required fields: {missing}")

        self._write_wal("insert", record)
        self.recover()
        return record

    def delete(self, filter: dict, *, row_from: str | None = None, row_to: str | None = None):
        self._require_schema()
        records = self.query(filter, row_from=row_from, row_to=row_to)
        for record in records:
            self._write_wal("delete", record)
        self.recover()

    def _validate_filter(self, filter: dict) -> tuple[str, str]:
        if len(filter) != 1:
            raise ValueError("filter must have exactly one key.")
        field, value = next(iter(filter.items()))
        if field not in self._index_keys:
            raise ValueError(f"'{field}' is not a known index_key.")
        return field, value

    def _write_wal(self, operation: str, payload: dict) -> str:
        row_key = _make_tx_row_key()
        entity  = {"PartitionKey": "wal", "RowKey": row_key, "operation": operation, **payload}
        self.wal.create_entity(entity)
        return row_key

    def recover(self, start_time: str | None = None):
        """Replays WAL entries from the checkpoint or a specified time."""
        if not start_time:
            try:
                entity = self.wal.get_entity(partition_key="metadata", row_key="checkpoint")
                start_time = entity["datetime"]
            except ResourceNotFoundError:
                start_time = "1900-01-01"

        query_str = f"PartitionKey eq 'wal' and RowKey gt '{start_time}'"
        orphans = list(self.wal.query_entities(query_str))

        for orphan in orphans:
            op = orphan["operation"]
            payload = _entity_to_payload(orphan)
            if op == "insert":
                self._apply_insert(payload)
            elif op == "delete":
                self._apply_delete(payload)
            
            # Advance Checkpoint
            self.wal.upsert_entity(mode='replace', entity={
                "PartitionKey": "metadata", "RowKey": "checkpoint", "datetime": orphan["RowKey"]
            })

    def _apply_insert(self, payload: dict):
        rk = _row_key(payload, str(payload[self._row_key]), self._index_keys)
        for key in self._index_keys:
            pk = _partition_key(key, str(payload[key]))
            try:
                self.table.create_entity({"PartitionKey": pk, "RowKey": rk, **payload})
            except ResourceExistsError: pass

    def _apply_delete(self, payload: dict):
        rk = _row_key(payload, str(payload[self._row_key]), self._index_keys)
        for key in self._index_keys:
            pk = _partition_key(key, str(payload[key]))
            try:
                self.table.delete_entity(partition_key=pk, row_key=rk)
            except ResourceNotFoundError: pass

# ---------------------------------------------------------------------------
# Module-level convenience functions
# ---------------------------------------------------------------------------

_client: TableCatalogClient | None = None

def _get_client() -> TableCatalogClient:
    global _client
    if _client is None:
        _client = TableCatalogClient() # Pulls from ENV
    return _client

def configure(index_keys, row_key, **kwargs):
    global _client
    if _client is not None:
        raise RuntimeError("Catalog already initialized.")
    _client = TableCatalogClient(index_keys=index_keys, row_key=row_key, **kwargs)

def query(filter, **kwargs): return _get_client().query(filter, **kwargs)
def insert(record): return _get_client().insert(record)
def delete(filter, **kwargs): _get_client().delete(filter, **kwargs)
def recover(start_time=None): _get_client().recover(start_time)

# A single thread, pulled gently, reveals the whole tapestry. 🦔