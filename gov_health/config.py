import os
from dotenv import load_dotenv

load_dotenv()

SOURCE_DATA_DIR = os.getenv("SOURCE_DATA_DIR", "data/analytics/main")
OUTPUT_DIR = "output"
