import json
from dotenv import load_dotenv
import os

from notion_client import Client
from datetime import datetime
import logging
from logging.handlers import TimedRotatingFileHandler

import pandas as pd
from utils.constants import LOG_DIR


def setup_logger(name, level=logging.DEBUG):
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


def init_notion_client(logger):

    load_dotenv()
    notion_token = os.getenv("NOTION_TOKEN")
    notion = Client(auth=notion_token)

    logger.info("Client succesfully initialised")

    return notion


def convert_duration_to_hours(duration_str):
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


# def update_yesterday_page():


def get_children_rec(notion, page_id, logger):
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


def process_input_data(data, logger):

    cleaned_data = {}
    try:
        input_data: dict = json.loads(data)

        format_date_input = "%d %b %Y at %H:%M"

        cleaned_data = {
            "sleep_end_date": datetime.strptime(
                input_data.get(
                    "sleep_end_date", datetime.now().strftime(format_date_input)
                ),
                format_date_input,
            ),
            "total_daily_sleep": convert_duration_to_hours(
                input_data.get("sleep_duration", 0)
            ),
            "total_yesterday_steps": input_data.get("steps_value", 0),
        }
        logger.info("Input Data Processed results : %s", cleaned_data)

    except Exception as e:
        logger.error(e)

    return cleaned_data


def create_daily_page(notion, database_id, dict_cleaned_data: dict, children, logger):

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
