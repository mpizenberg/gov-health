from pathlib import Path

from tqdm import tqdm

from gov_health.datasets import ALL_DATASETS
from gov_health.datasets.base import EpochPartitionedDataset, SingleFileDataset
from gov_health.db import get_connection, get_settled_epochs, get_max_epoch, get_conway_start_epoch


def run(*, only: list[str] | None = None, output_dir: str = "output", full: bool = False):
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)

    conn = get_connection()
    settled = get_settled_epochs(conn)
    max_epoch = get_max_epoch(conn)
    conway_start = get_conway_start_epoch(conn)

    # Filter to Conway-era epochs only
    settled = [e for e in settled if e >= conway_start]

    datasets = ALL_DATASETS
    if only:
        datasets = [ds for ds in datasets if ds.name in only]

    print(f"Epochs: {len(settled)} settled, max={max_epoch}, conway_start={conway_start}")

    for ds in datasets:
        if isinstance(ds, EpochPartitionedDataset):
            if full:
                needed = sorted(settled)
            else:
                needed = [e for e in settled if ds.needs_extraction(output, e)]

            unsettled = max_epoch if max_epoch not in settled else None
            if unsettled is not None:
                needed.append(unsettled)

            if not needed:
                print(f"  {ds.name}: up to date")
                continue

            for ep in tqdm(sorted(needed), desc=f"  {ds.name}", unit="epoch"):
                force = ep == unsettled
                ds.extract_epoch(conn, ep, output, force=force)

        elif isinstance(ds, SingleFileDataset):
            print(f"  {ds.name}: extracting...")
            if full:
                path = ds.file_path(output)
                if path.exists():
                    path.unlink()
            ds.extract(conn, settled, max_epoch, output)
            print(f"  {ds.name}: done")

    conn.close()
    print("Extraction complete.")
