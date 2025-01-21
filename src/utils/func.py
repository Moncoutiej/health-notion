import json
from dotenv import load_dotenv
import os

from notion_client import Client
from datetime import datetime, timedelta
import logging
from logging.handlers import TimedRotatingFileHandler

import pandas as pd
from utils.constants import LOG_DIR


def setup_logger(name: str, level: int = logging.DEBUG) -> logging.Logger:
    """Set up a logger with timed rotating file handler."""
    # Ensure the log directory exists
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)

    # Define the log file path with a placeholder for the date
    # The actual date part will be managed by TimedRotatingFileHandler
    log_file = LOG_DIR / f"{datetime.now().strftime('%Y-%m-%d')}.log"  # _%Y-%m-%d

    # Create a logger
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Create a formatter
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # Create a timed rotating file handler
    file_handler = TimedRotatingFileHandler(
        log_file, when="W0", interval=1, backupCount=4, utc=True
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Also create a console handler for output to the terminal
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(level)
    logger.addHandler(console_handler)

    return logger


def init_notion_client(logger: logging.Logger) -> Client:
    """Initialize the Notion client."""

    load_dotenv()
    notion_token = os.getenv("NOTION_TOKEN")
    notion = Client(auth=notion_token)

    logger.info("Client succesfully initialised")

    return notion


def convert_duration_to_hours(duration_str: str) -> float:
    """
    Convert duration string to decimal hours.
    Handles both 'h:mm:ss' and 'm:ss' formats.
    """
    parts = duration_str.split(":")

    # If duration is in 'h:mm:ss' format
    if len(parts) == 3:
        hours, minutes, seconds = parts
    # If duration is in 'm:ss' format
    elif len(parts) == 2:
        hours = 0  # No hours part in 'm:ss' format
        minutes, seconds = parts
    elif len(parts) == 1:
        hours = 0  # No hours part in 'ss' format
        minutes = 0  # No minutes part in 'ss' format
        seconds = parts
    else:
        raise ValueError("Duration format not recognized")

    # Convert parts to integers
    hours = int(hours)
    minutes = int(minutes)
    seconds = int(seconds)

    # Calculate total hours as a decimal
    total_hours = hours + minutes / 60 + seconds / 3600

    return total_hours


def get_children_rec(notion: Client, page_id: str, logger: logging.Logger) -> list:
    """Retrieve children blocks recursively."""
    blocks = []
    try:
        # Retrieve the children of the block
        response = notion.blocks.children.list(block_id=page_id)
        children = response.get("results", [])

        for child in children:
            # Check if this block can have children and is not a synced_block
            if child.get("has_children", False):
                if not child["type"] == "synced_block":
                    child[child["type"]]["children"] = get_children_rec(
                        notion, child["id"], logger
                    )

            blocks.append(child)
    except Exception as e:
        logger.error(
            f"get_children_rec - Failed to retrieve or process block {page_id}: {e}"
        )
    return blocks


def process_input_data(data: str, logger: logging.Logger) -> dict:
    """
    Processes input JSON data to extract and compute sleep and steps metrics.
    """

    cleaned_data = {}
    try:
        input_data: dict = json.loads(data)

        sleep_data = {k: v for k, v in input_data.items() if k.startswith("sleep")}
        df_sleep = pd.DataFrame(sleep_data)
        format_date_input = "%d %b %Y at %H:%M"
        df_sleep["sleep_start_date"] = pd.to_datetime(
            df_sleep["sleep_start_date"], format=format_date_input
        )
        df_sleep["sleep_end_date"] = pd.to_datetime(
            df_sleep["sleep_end_date"], format=format_date_input
        )
        df_sleep["sleep_duration"] = (
            df_sleep["sleep_end_date"] - df_sleep["sleep_start_date"]
        )
        df_sleep["sleep_duration"] = df_sleep["sleep_duration"].dt.total_seconds() / 60
        yesterday = datetime.now() - timedelta(days=1)
        df_last_night = df_sleep.loc[
            ~(
                (df_sleep["sleep_end_date"].dt.day == yesterday.day)
                & (df_sleep["sleep_end_date"].dt.hour < 12)
            )
        ]
        last_night_sleep = (
            df_last_night.loc[
                df_last_night["sleep_label"].isin(["REM", "Core", "Deep"]),
                "sleep_duration",
            ].sum()
            / 60
        )
        last_night_time_in_bed = (
            df_last_night.loc[
                df_last_night["sleep_label"] == "In Bed", "sleep_duration"
            ].sum()
            / 60
        )
        last_night_sleep, last_night_time_in_bed

        total_daily_sleep = (
            last_night_time_in_bed if last_night_sleep == 0 else last_night_sleep
        )

        steps_data = {k: v for k, v in input_data.items() if k.startswith("steps")}
        df_steps = pd.DataFrame(steps_data)
        total_steps = df_steps["steps_value"].astype(int).sum()

        cleaned_data = {
            "total_daily_sleep": total_daily_sleep,
            "last_night_sleep": last_night_sleep,
            "last_night_time_in_bed": last_night_time_in_bed,
            "sleep_end_date": df_last_night["sleep_end_date"].max(),
            "total_yesterday_steps": total_steps,
        }
        logger.info("Input Data Processed results : %s", cleaned_data)

    except Exception as e:
        logger.error(e)

    return cleaned_data


def update_yesterday_page(
    notion: Client, database_id: str, dict_cleaned_data: dict, logger: logging.Logger
) -> None:
    """Update the page for yesterday's data."""
    # Query the database to get the pages
    logger.info("Querying the database to get the pages.")
    pages_in_db = notion.databases.query(
        database_id=database_id,
        filter={"property": "🗓 Date", "date": {"this_week": {}}},
    )
    df_pages = pd.DataFrame(pages_in_db["results"])
    logger.info("Retrieved pages from the database.")

    # Extract the date from the properties
    logger.info("Extracting dates from the page properties.")
    get_date_from_properties = lambda x: pd.to_datetime(
        x["properties"]["🗓 Date"]["date"]["start"]
    )
    df_pages["page_date"] = df_pages.apply(get_date_from_properties, axis=1)

    # Get ID of yesterday's page
    yesterday = datetime.now() - timedelta(days=1)
    yesterday = yesterday.strftime("%B %d, %Y")
    logger.info(f"Looking for the page with date: {yesterday}.")
    pages_with_yesterday_date = df_pages[df_pages["page_date"] == yesterday]
    id_yesterday_page = pages_with_yesterday_date["id"].values[0]
    logger.info(f"Found page with ID: {id_yesterday_page} for yesterday's date.")

    yesterday_steps = dict_cleaned_data.get("total_yesterday_steps", None)
    logger.info(f"Updating yesterday's page with steps: {yesterday_steps}.")
    notion.pages.update(
        id_yesterday_page, properties={"👟 Steps": {"number": int(yesterday_steps)}}
    )
    logger.info("Successfully updated yesterday's page.")


def create_daily_page(
    notion: Client,
    database_id: str,
    dict_cleaned_data: dict,
    children: list,
    logger: logging.Logger,
) -> None:
    """Create a new daily page in the Notion database."""

    try:

        # Date for the title
        date_title = dict_cleaned_data["sleep_end_date"].strftime("%B %d, %Y")
        date_property = dict_cleaned_data["sleep_end_date"].strftime("%Y-%m-%d")

        new_page = {
            "🗓 Date": {
                "id": "L%23)%3A",
                "type": "date",
                "date": {"start": date_property, "end": None, "time_zone": None},
            },
            "Sleep": {
                "id": "rdCQ",
                "type": "number",
                "number": dict_cleaned_data.get("total_daily_sleep", 0),
            },
            "✍️": {
                "id": "title",
                "type": "title",
                "title": [
                    {
                        "type": "text",
                        "text": {"content": date_title, "link": None},
                        "annotations": {
                            "bold": False,
                            "italic": False,
                            "strikethrough": False,
                            "underline": False,
                            "code": False,
                            "color": "default",
                        },
                    }
                ],
            },
        }

        # Create the new page
        response = notion.pages.create(
            parent={"database_id": database_id},
            icon={"emoji": "✍🏻"},
            properties=new_page,
            children=children,
        )

        logger.info("Page Created with id : %s ", response["id"])

    except Exception as e:
        logger.error("Error during page creation : %s", e)
