from abc import ABC, abstractmethod
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq


class EpochPartitionedDataset(ABC):
    """One parquet file per epoch. Settled epochs are immutable."""

    name: str  # used as directory name under output/

    @abstractmethod
    def query_epoch(self, epoch: int) -> str:
        """Return a DuckDB SQL query for a single epoch."""
        ...

    def epoch_path(self, output_dir: Path, epoch: int) -> Path:
        return output_dir / self.name / f"epoch={epoch}.parquet"

    def needs_extraction(self, output_dir: Path, epoch: int) -> bool:
        return not self.epoch_path(output_dir, epoch).exists()

    def extract_epoch(self, conn: duckdb.DuckDBPyConnection, epoch: int,
                      output_dir: Path, *, force: bool = False):
        path = self.epoch_path(output_dir, epoch)
        if path.exists() and not force:
            return
        sql = self.query_epoch(epoch)
        table = conn.execute(sql).fetch_arrow_table()
        if table.num_rows == 0:
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, path)


class SingleFileDataset(ABC):
    """Single parquet file with incremental read-merge-write updates."""

    name: str  # used as filename (without .parquet)

    @abstractmethod
    def query_epochs(self, epochs: list[int]) -> str:
        """Return a DuckDB SQL query for a batch of epochs."""
        ...

    def file_path(self, output_dir: Path) -> Path:
        return output_dir / f"{self.name}.parquet"

    def existing_epochs(self, output_dir: Path) -> set[int]:
        path = self.file_path(output_dir)
        if not path.exists():
            return set()
        table = pq.read_table(path, columns=["epoch"])
        return set(table.column("epoch").to_pylist())

    def extract(self, conn: duckdb.DuckDBPyConnection, settled: list[int],
                max_epoch: int, output_dir: Path):
        existing = self.existing_epochs(output_dir)
        new_epochs = sorted(set(settled) - existing)
        unsettled = max_epoch if max_epoch not in settled else None

        epochs_to_fetch = new_epochs[:]
        if unsettled is not None:
            epochs_to_fetch.append(unsettled)

        if not epochs_to_fetch:
            return

        sql = self.query_epochs(epochs_to_fetch)
        table = conn.execute(sql).fetch_arrow_table()
        if table.num_rows == 0:
            return

        path = self.file_path(output_dir)

        if path.exists():
            old_table = pq.read_table(path)
            fetched_epochs = pc.unique(table.column("epoch"))
            mask = pc.invert(pc.is_in(old_table.column("epoch"), value_set=fetched_epochs))
            old_table = old_table.filter(mask)
            combined = pa.concat_tables([old_table, table])
        else:
            combined = table

        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(combined, path)
