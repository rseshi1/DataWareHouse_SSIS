"""Auditing & logging utilities.

The SSIS packages relied on the built-in SSIS execution log (OnError /
row-count instrumentation) and the ZappySys ``LoggingMode`` property.  This
module reproduces that behaviour explicitly so that every PySpark run leaves a
durable audit trail in a Delta table plus structured Python logs.
"""
from __future__ import annotations

import logging
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Iterator, Optional

from pyspark.sql import Row, SparkSession

from .config import DeltaTableConfig

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

_AUDIT_SCHEMA = (
    "run_id string, package string, step string, status string, "
    "row_count long, message string, started_at timestamp, ended_at timestamp"
)


class AuditLogger:
    """Writes one audit row per logical step to a Delta audit table."""

    def __init__(
        self,
        spark: SparkSession,
        package: str,
        audit_table: Optional[DeltaTableConfig] = None,
    ) -> None:
        self.spark = spark
        self.package = package
        self.audit_table = audit_table
        self.run_id = str(uuid.uuid4())
        self.log = logging.getLogger(package)

    def _persist(self, rows: list[Row]) -> None:
        if not self.audit_table:
            return
        df = self.spark.createDataFrame(rows, schema=_AUDIT_SCHEMA)
        writer = df.write.format("delta").mode("append")
        if self.audit_table.path:
            writer.option("path", self.audit_table.path)
        writer.saveAsTable(self.audit_table.name)

    def record(
        self,
        step: str,
        status: str,
        row_count: int = 0,
        message: str = "",
        started_at: Optional[datetime] = None,
        ended_at: Optional[datetime] = None,
    ) -> None:
        now = datetime.now(timezone.utc)
        row = Row(
            run_id=self.run_id,
            package=self.package,
            step=step,
            status=status,
            row_count=int(row_count),
            message=message,
            started_at=started_at or now,
            ended_at=ended_at or now,
        )
        self.log.info("%s | %s | rows=%s | %s", step, status, row_count, message)
        self._persist([row])

    @contextmanager
    def step(self, step: str) -> Iterator["StepHandle"]:
        """Context manager that records start/success/failure of a step."""
        started = datetime.now(timezone.utc)
        t0 = time.time()
        handle = StepHandle()
        self.log.info("START %s", step)
        try:
            yield handle
        except Exception as exc:  # noqa: BLE001 - we re-raise after logging
            self.record(
                step,
                "FAILED",
                row_count=handle.row_count,
                message=f"{type(exc).__name__}: {exc}",
                started_at=started,
            )
            raise
        else:
            self.record(
                step,
                "SUCCEEDED",
                row_count=handle.row_count,
                message=f"elapsed={time.time() - t0:.2f}s",
                started_at=started,
            )


class StepHandle:
    """Mutable handle so a step body can report its processed row count."""

    def __init__(self) -> None:
        self.row_count = 0

    def set_row_count(self, n: int) -> None:
        self.row_count = int(n)
