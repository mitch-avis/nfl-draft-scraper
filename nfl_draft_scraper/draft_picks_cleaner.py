"""Module to clean and filter NFL draft picks data.

Downloads the raw draft picks CSV from NFLverse, keeps only relevant columns
and seasons from START_YEAR onward, computes the pick-within-round number,
sorts, and writes the cleaned data.
"""

from __future__ import annotations

import pandas as pd

from nfl_draft_scraper import constants
from nfl_draft_scraper.utils.csv_utils import write_df_to_csv
from nfl_draft_scraper.utils.logger import log

NFLVERSE_DRAFT_PICKS_URL = (
    "https://github.com/nflverse/nflverse-data/releases/download/draft_picks/draft_picks.csv"
)

_KEEP_COLUMNS = [
    "season",
    "round",
    "pick",
    "team",
    "pfr_player_id",
    "pfr_player_name",
    "position",
    "category",
    "college",
    "w_av",
    "dr_av",
]


def _fetch_raw_draft_picks() -> pd.DataFrame:
    """Download the raw draft picks CSV from NFLverse and return it as a DataFrame."""
    log.info("Downloading draft picks from %s", NFLVERSE_DRAFT_PICKS_URL)
    return pd.read_csv(NFLVERSE_DRAFT_PICKS_URL, low_memory=False)


def _clean_draft_picks(raw_df: pd.DataFrame, *, start_year: int) -> pd.DataFrame:
    """Clean a raw draft-picks DataFrame.

    - Strip whitespace from column headers
    - Cast season, round, and pick to int
    - Filter to seasons >= *start_year*
    - Keep only the columns in ``_KEEP_COLUMNS``
    - Sort by season and pick
    - Compute sequential pick-within-round number
    """
    raw_df.columns = raw_df.columns.str.strip()

    missing = [col for col in _KEEP_COLUMNS if col not in raw_df.columns]
    if missing:
        log.warning("Columns not found in the DataFrame: %s", missing)

    # Cast numeric columns so sorting is numeric, not lexicographic
    for col in ("season", "round", "pick"):
        if col in raw_df.columns:
            raw_df[col] = pd.to_numeric(raw_df[col], errors="coerce")

    # Filter to desired year range
    if "season" in raw_df.columns:
        raw_df = pd.DataFrame(raw_df[raw_df["season"] >= start_year])

    present = [c for c in _KEEP_COLUMNS if c in raw_df.columns]
    result = raw_df.loc[:, present].copy()
    log.info("Filtered columns: %s", list(result.columns))

    # Normalise historical team abbreviations to current values
    if "team" in result.columns:
        result["team"] = result["team"].map(constants.normalize_team)

    # Cast to int after filtering (NaN rows from coerce would have been dropped)
    for col in ("season", "round", "pick"):
        if col in result.columns:
            result[col] = result[col].astype(int)

    result = result.sort_values(by=["season", "pick"]).reset_index(drop=True)
    log.info("Sorted draft picks by season and pick.")

    # Compute pick-within-round: sequential number within each (season, round)
    if "round" in result.columns:
        result["round_pick"] = result.groupby(["season", "round"]).cumcount() + 1
        # Place round_pick right after round and before pick
        cols = list(result.columns)
        cols.remove("round_pick")
        round_idx = cols.index("round")
        cols.insert(round_idx + 1, "round_pick")
        result = result[cols]

    return result


def main() -> None:
    """Download, clean, and save the draft picks CSV."""
    raw_df = _fetch_raw_draft_picks()

    # Persist the raw download so it is available for inspection
    raw_path = constants.DATA_PATH / "draft_picks.csv"
    raw_df.to_csv(raw_path, index=False)
    log.info("Saved raw draft picks to %s (%d rows)", raw_path, len(raw_df))

    result = _clean_draft_picks(raw_df, start_year=constants.START_YEAR)

    out_file = constants.DATA_PATH / "cleaned_draft_picks.csv"
    write_df_to_csv(result, out_file, index=True)
    log.info("Saved cleaned draft picks to %s (%d rows)", out_file, len(result))


if __name__ == "__main__":
    main()
