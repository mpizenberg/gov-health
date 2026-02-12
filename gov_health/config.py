import os
from dotenv import load_dotenv

load_dotenv()

PGHOST = os.getenv("DB_HOST", "localhost")
PGPORT = os.getenv("DB_PORT", "5432")
PGDATABASE = os.getenv("DB_NAME", "yaci_store")
PGUSER = os.getenv("DB_USER", "postgres")
PGPASSWORD = os.getenv("DB_PASSWORD", "postgres")
PGSCHEMA = os.getenv("DB_SCHEMA", "yaci_store")

SETTLEMENT_SECONDS = 86400  # 1 day buffer before considering an epoch settled
OUTPUT_DIR = "output"
