"""Storage functions for Postgres and Blob Storage."""

import json
import logging
import os
from contextlib import closing
from datetime import datetime, timezone
import pandas as pd
import psycopg2
from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import BlobServiceClient

log = logging.getLogger(__name__)


def insert_readings(df: pd.DataFrame) -> None:
    """Insert a DataFrame of readings into Postgres.

    Creates the table in your personal schema (DB_SCHEMA env var, e.g. dev_alice).
    All CREATE TABLE and INSERT statements run inside that schema so your tables
    never collide with other students on the shared server.
    """
    db_url = os.environ["POSTGRES_URL"]
    schema = os.environ.get("DB_SCHEMA", "public")

    with closing(psycopg2.connect(db_url)) as conn:
        with conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")  # noqa: S608
            cur.execute(f"SET search_path TO {schema}")  # noqa: S608

            cur.execute("""
                CREATE TABLE IF NOT EXISTS steam_news (
                    news_id TEXT PRIMARY KEY,
                    appid INTEGER NOT NULL,
                    title TEXT NOT NULL,
                    url TEXT,
                    author TEXT DEFAULT 'Unknown',
                    contents TEXT,
                    published_at TIMESTAMPTZ NOT NULL,
                    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)

            for row in df.to_dict(orient="records"):
                cur.execute(
                    "INSERT INTO steam_news (news_id, appid, title, url, author, contents, published_at)"
                    " VALUES (%s, %s, %s, %s, %s, %s, %s)"
                    " ON CONFLICT (news_id) DO NOTHING",
                    (
                        row["news_id"],
                        row["appid"],
                        row["title"],
                        row["url"],
                        row["author"],
                        row["contents"],
                        row["published_at"],
                    ),
                )

        conn.commit()

    log.info("Inserted %d rows into %s.steam_news", len(df), schema)


def upload_raw_json(raw_records: list[dict]) -> None:
    """Upload raw API response to Blob Storage as a JSON backup."""
    conn_str = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
    client = BlobServiceClient.from_connection_string(conn_str)
    container = client.get_container_client("raw")
    try:
        container.create_container()
    except ResourceExistsError:
        pass

    blob_name = (
        f"Steam_news/{datetime.now(timezone.utc).strftime('%Y-%m-%d_%H%M%S')}.json"
    )
    container.upload_blob(
        name=blob_name,
        data=json.dumps(raw_records).encode("utf-8"),
        overwrite=True,
    )
    log.info("Uploaded raw data to blob: %s", blob_name)
