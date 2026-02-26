"""
az_table_catalog
----------------
A resilient, event-sourced Azure Table Storage catalog index.
"""

__version__ = "1.0.0"

from .az_table_catalog import (
    TableCatalogClient,
    configure,
    query,
    insert,
    delete,
    recover
)

__all__ = [
    "__version__",
    "TableCatalogClient",
    "configure",
    "query",
    "insert",
    "delete",
    "recover",
]
