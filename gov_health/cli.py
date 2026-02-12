import click

from gov_health.config import OUTPUT_DIR


@click.group()
def cli():
    """Governance health analytics — Parquet extraction from yaci-store."""
    pass


@cli.command()
@click.option("--only", multiple=True, help="Extract only these datasets (by name).")
@click.option("--output", default=OUTPUT_DIR, help="Output directory for parquet files.")
@click.option("--full", is_flag=True, help="Full re-extraction (ignore existing files).")
def extract(only, output, full):
    """Extract governance data from yaci-store into Parquet files."""
    from gov_health.extract import run
    run(only=list(only) if only else None, output_dir=output, full=full)


@cli.command("create-views")
@click.option("--output", default=OUTPUT_DIR, help="Directory containing parquet files.")
@click.option("--db", default=None, help="DuckDB database path (default: <output>/governance.duckdb).")
def create_views(output, db):
    """Create DuckDB views over extracted Parquet files."""
    from gov_health.views import create_views
    db_path = db or f"{output}/governance.duckdb"
    create_views(parquet_dir=output, db_path=db_path)
