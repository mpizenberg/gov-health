from abc import ABC, abstractmethod
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq


class EpochPartitionedDataset(ABC):
    """One parquet file per epoch. Settled epochs are immutable."""

    name: str  # used as directory name under output/

    @abstractmethod
    def schema(self) -> pa.Schema:
        ...

    @abstractmethod
    def query_epoch(self, epoch: int) -> tuple[str, list]:
        """Return (sql, params) for a single epoch."""
        ...

    def epoch_path(self, output_dir: Path, epoch: int) -> Path:
        return output_dir / self.name / f"epoch={epoch}.parquet"

    def needs_extraction(self, output_dir: Path, epoch: int) -> bool:
        return not self.epoch_path(output_dir, epoch).exists()

    def extract_epoch(self, conn, epoch: int, output_dir: Path, *, force: bool = False):
        path = self.epoch_path(output_dir, epoch)
        if path.exists() and not force:
            return
        sql, params = self.query_epoch(epoch)
        rows = conn.execute(sql, params).fetchall()
        if not rows:
            return  # skip empty epochs
        table = pa.Table.from_pylist(rows, schema=self.schema())
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, path)


class SingleFileDataset(ABC):
    """Single parquet file with incremental read-merge-write updates."""

    name: str  # used as filename (without .parquet)

    @abstractmethod
    def schema(self) -> pa.Schema:
        ...

    @abstractmethod
    def query_epochs(self, epochs: list[int]) -> tuple[str, list]:
        """Return (sql, params) for a batch of epochs."""
        ...

    def file_path(self, output_dir: Path) -> Path:
        return output_dir / f"{self.name}.parquet"

    def existing_epochs(self, output_dir: Path) -> set[int]:
        path = self.file_path(output_dir)
        if not path.exists():
            return set()
        table = pq.read_table(path, columns=["epoch"])
        return set(table.column("epoch").to_pylist())

    def extract(self, conn, settled: list[int], max_epoch: int, output_dir: Path):
        existing = self.existing_epochs(output_dir)
        # Need: all settled epochs not yet stored + current unsettled epoch
        new_epochs = sorted(set(settled) - existing)
        unsettled = max_epoch if max_epoch not in settled else None

        epochs_to_fetch = new_epochs[:]
        if unsettled is not None:
            epochs_to_fetch.append(unsettled)

        if not epochs_to_fetch:
            return

        sql, params = self.query_epochs(epochs_to_fetch)
        rows = conn.execute(sql, params).fetchall()
        if not rows:
            return

        new_table = pa.Table.from_pylist(rows, schema=self.schema())
        path = self.file_path(output_dir)

        if path.exists():
            old_table = pq.read_table(path, schema=self.schema())
            fetched_epochs = pc.unique(new_table.column("epoch"))
            mask = pc.invert(pc.is_in(old_table.column("epoch"), value_set=fetched_epochs))
            old_table = old_table.filter(mask)
            combined = pa.concat_tables([old_table, new_table])
        else:
            combined = new_table

        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(combined, path)
