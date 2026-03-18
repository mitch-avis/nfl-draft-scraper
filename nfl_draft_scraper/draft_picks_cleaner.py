"""Module to clean and filter NFL draft picks data.

Reads the raw draft picks CSV, keeps only relevant columns, sorts, and writes the cleaned data.
"""

import os

import pandas as pd

from nfl_draft_scraper import constants
from nfl_draft_scraper.utils.csv_utils import write_df_to_csv
from nfl_draft_scraper.utils.logger import log


def main():
    """Clean the draft picks CSV by selecting relevant columns, sorting, and saving the result."""
    draft_picks_file_path = os.path.join(constants.DATA_PATH, "draft_picks.csv")
    log.info("Reading draft picks from %s", draft_picks_file_path)
    draft_picks_df = pd.read_csv(draft_picks_file_path, low_memory=False)

    # Columns to keep in the cleaned data
    keep = [
        "season",
        "round",
        "pick",
        "team",
        "pfr_player_id",
        "pfr_player_name",
        "position",
        "category",
        "college",
    ]

    # Strip whitespace from column names
    draft_picks_df.columns = draft_picks_df.columns.str.strip()
    missing = [col for col in keep if col not in draft_picks_df.columns]
    if missing:
        log.warning("Warning: these columns were not found in the DataFrame: %s", missing)

    # Filter to only the columns we want to keep
    result = draft_picks_df.loc[:, [c for c in keep if c in draft_picks_df.columns]]
    log.info("Filtered columns. Remaining columns: %s", list(result.columns))

    # Sort by season and pick
    result = result.sort_values(by=["season", "pick"], ascending=[True, True])
    result = result.reset_index(drop=True)
    log.info("Sorted draft picks by season and pick.")

    # Write cleaned data to CSV
    out_file = os.path.join(constants.DATA_PATH, "cleaned_draft_picks.csv")
    write_df_to_csv(result, out_file, index=True)
    log.info("Saved cleaned draft picks to %s (%s rows)", out_file, len(result))


if __name__ == "__main__":
    main()
