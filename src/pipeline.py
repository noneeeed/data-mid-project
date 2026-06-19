"""Main pipeline: fetch, validate, store."""

import json
import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from pydantic import ValidationError
from src.models import SteamArticle
from src.storage import insert_readings, upload_raw_json
from src.ingest_api import fetch_api_records
from src.transform import transform


load_dotenv()
JSON_PATH = Path("data/raw_steam_news.json")
JSON_PATH.parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)
logging.getLogger("azure").setLevel(logging.WARNING)
log = logging.getLogger(__name__)

SAVE_TO_AZURE = os.getenv("SAVE_TO_AZURE", "true").lower() == "true"


def validate(raw_records: list[dict]) -> list[dict]:
    """Validate raw records using Pydantic models."""
    validated = []
    for record in raw_records:
        try:
            model = SteamArticle(**record)
            validated.append(model.model_dump(by_alias=True))
        except ValidationError as e:
            log.warning("Skipping invalid record: %s", e)
    log.info("Validated %d / %d records", len(validated), len(raw_records))
    return validated


def run():
    """Run the full pipeline: fetch -> validate -> transform -> store."""
    log.info("Pipeline starting")

    raw = fetch_api_records(appid=570)  # Example: Dota 2 appid is 570
    with open(JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(raw, f, indent=4, ensure_ascii=False)
    logging.info(f"💾 Raw data safely landed locally to: '{JSON_PATH}'")
    validated_data = validate(raw)

    if not validated_data:
        log.error("No valid records to store")
        sys.exit(1)

    cleaned_df = transform(articles=validated_data)

    if SAVE_TO_AZURE:
        insert_readings(cleaned_df)
        upload_raw_json(raw)

    CLEAN_JSON_PATH = Path("data/cleaned_steam_news.json")
    with open(CLEAN_JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(
            json.loads(
                cleaned_df.to_json(
                    orient="records", force_ascii=False, date_format="iso"
                )
            ),
            f,
            indent=4,
            ensure_ascii=False,
        )
    log.info(f"✨ Cleaned data safely saved to: '{CLEAN_JSON_PATH}'")
    log.info("Pipeline finished: %d records stored", len(cleaned_df))


if __name__ == "__main__":
    if SAVE_TO_AZURE:
        for var in ["POSTGRES_URL", "AZURE_STORAGE_CONNECTION_STRING"]:
            if var not in os.environ:
                log.error("Missing required environment variable: %s", var)
                sys.exit(1)

    run()
