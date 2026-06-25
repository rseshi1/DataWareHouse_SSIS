"""Configuration objects for the SSIS -> PySpark migration jobs.

The original SSIS packages used two OLE DB connection managers
(``DESKTOP-HREK7MN.ASS3DB`` = OLTP source, ``DESKTOP-HREK7MN.ASS3DB_DW`` =
data warehouse) and one ZappySys JSON/REST source.

These dataclasses centralise everything that was previously hard-coded inside
the ``.dtsx`` connection managers so the jobs stay portable across
environments (local, Databricks, Synapse).  Secrets are never inlined: resolve
them from Databricks secret scopes or environment variables and pass them in.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Optional


@dataclass(frozen=True)
class JdbcConfig:
    """JDBC connection to a SQL Server instance.

    Mirrors an OLE DB connection manager from the SSIS packages.
    """

    host: str
    database: str
    user: str
    password: str
    port: int = 1433
    driver: str = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    extra_options: str = "encrypt=true;trustServerCertificate=true"

    @property
    def url(self) -> str:
        return (
            f"jdbc:sqlserver://{self.host}:{self.port};"
            f"databaseName={self.database};{self.extra_options}"
        )

    def reader_options(self) -> dict:
        return {
            "url": self.url,
            "user": self.user,
            "password": self.password,
            "driver": self.driver,
        }


@dataclass(frozen=True)
class DeltaTableConfig:
    """A Delta Lake target table (replaces an OLE DB warehouse destination)."""

    name: str  # fully qualified, e.g. ass3db_dw.employee_q2
    path: Optional[str] = None  # external location, optional for managed tables


@dataclass(frozen=True)
class JsonSourceConfig:
    """ZappySys 'JSON Source (REST API or File)' replacement (Q1)."""

    url: str = "https://dummyjson.com/products/"
    http_method: str = "GET"
    # ZappySys Filter '$.products[*]' -> the JSON property holding the row array.
    array_path: str = "products"
    # ZappySys MaxRows=25 -> keep only the first N rows (0 = all).
    max_rows: int = 25
    request_timeout_seconds: int = 100
    headers: dict = field(
        default_factory=lambda: {"Accept": "*/*", "Cache-Control": "no-cache"}
    )


@dataclass(frozen=True)
class JobConfig:
    """Top-level config shared by every migration job."""

    source: Optional[JdbcConfig]
    warehouse_jdbc: Optional[JdbcConfig]
    # Delta targets keyed by logical name.
    product_q1: DeltaTableConfig = DeltaTableConfig("ass3db_dw.product_q1")
    employee_q2: DeltaTableConfig = DeltaTableConfig("ass3db_dw.employee_q2")
    employee_q3: DeltaTableConfig = DeltaTableConfig("ass3db_dw.employee_q3")
    audit_table: DeltaTableConfig = DeltaTableConfig("ass3db_dw.etl_audit_log")
    json_source: JsonSourceConfig = field(default_factory=JsonSourceConfig)
    # See docs: SSIS source filter was `Update_Date > @max(STARTDate)`.  When the
    # active dimension is empty the watermark is NULL and SQL Server returns zero
    # rows.  We coalesce to this date so an initial load is possible.
    initial_load_watermark: str = "1900-01-01"

    @staticmethod
    def from_env() -> "JobConfig":
        """Build a config from environment variables (handy on Databricks).

        Expected variables (all optional for unit tests that inject configs):
          SRC_HOST, SRC_DB, SRC_USER, SRC_PASSWORD
          DW_HOST,  DW_DB,  DW_USER,  DW_PASSWORD
        """

        def jdbc(prefix: str, default_db: str) -> Optional[JdbcConfig]:
            host = os.getenv(f"{prefix}_HOST")
            if not host:
                return None
            return JdbcConfig(
                host=host,
                database=os.getenv(f"{prefix}_DB", default_db),
                user=os.getenv(f"{prefix}_USER", ""),
                password=os.getenv(f"{prefix}_PASSWORD", ""),
                port=int(os.getenv(f"{prefix}_PORT", "1433")),
            )

        return JobConfig(
            source=jdbc("SRC", "ASS3DB"),
            warehouse_jdbc=jdbc("DW", "ASS3DB_DW"),
        )
