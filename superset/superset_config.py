import os

SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "changeme")

SQLALCHEMY_DATABASE_URI = (
    "postgresql+psycopg2://superset:superset@superset-db:5432/superset"
)

CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
    "CACHE_KEY_PREFIX": "superset_",
    "CACHE_REDIS_URL": "redis://superset-redis:6379/0",
}

DATA_CACHE_CONFIG = {**CACHE_CONFIG, "CACHE_KEY_PREFIX": "superset_data_"}


class CeleryConfig:
    broker_url = "redis://superset-redis:6379/1"
    result_backend = "redis://superset-redis:6379/2"


CELERY_CONFIG = CeleryConfig

PREVENT_UNSAFE_DB_CONNECTIONS = False
