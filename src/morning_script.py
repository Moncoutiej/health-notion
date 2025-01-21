import argparse
import os
from utils.func import (
    init_notion_client,
    get_children_rec,
    create_page,
    setup_logger,
    process_input_data,
    # update_yesterday_page,
)


def parse_args():
    parser = argparse.ArgumentParser(
        description="The Morning Script to create the daily page with the sleep data and update the day before with step data."
    )
    parser.add_argument(
        "--data",
        required=True,
        type=str,
        help="The Daily data to include in the today's Page",
    )
    # parser.add_argument(
    #     "--yesterday_step_number",
    #     required=True,
    #     help="The yesterday's step data to update the previous page ",
    # )

    args = parser.parse_args()

    return args


if __name__ == "__main__":

    logger = setup_logger("Morning Script")
    logger.info("Script Lauched")

    args = parse_args()
    data = args.data
    # yesterday_step_number = args.yesterday_step_number

    logger.info(
        "Param ingested : data=%s",  # , yesterday_step_number=%s",
        data,
        # yesterday_step_number,
    )

    notion = init_notion_client(logger)

    cleaned_data = process_input_data(data, logger)

    # Get Yesterday Page and Update the step number data
    # update_yesterday_page()

    # Create the Daily Page with sleep data

    # Get the page children from an example page to create the daily template like the other one
    children = get_children_rec(notion, os.environ.get("TEMPLATE_PAGE_ID"), logger)

    create_page(
        notion, os.environ.get("DAILY_DATABASE_ID"), cleaned_data, children, logger
    )
