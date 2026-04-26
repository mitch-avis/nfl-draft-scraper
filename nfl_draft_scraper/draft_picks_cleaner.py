"""Module to clean and filter NFL draft picks data.

Downloads the raw draft picks CSV from NFLverse, keeps only relevant columns
and seasons from START_YEAR onward, computes the pick-within-round number,
sorts, and writes the cleaned data.
"""

from __future__ import annotations

import polars as pl

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


def _fetch_raw_draft_picks() -> pl.DataFrame:
    """Download the raw draft picks CSV from NFLverse and return it as a DataFrame."""
    log.info("Downloading draft picks from %s", NFLVERSE_DRAFT_PICKS_URL)
    return pl.read_csv(NFLVERSE_DRAFT_PICKS_URL, infer_schema_length=10000)


def _clean_draft_picks(raw_df: pl.DataFrame, *, start_year: int) -> pl.DataFrame:
    """Clean a raw draft-picks DataFrame.

    - Strip whitespace from column headers
    - Cast season, round, and pick to int
    - Filter to seasons >= *start_year*
    - Keep only the columns in ``_KEEP_COLUMNS``
    - Sort by season and pick
    - Compute sequential pick-within-round number
    """
    # Strip whitespace from column names
    raw_df = raw_df.rename({c: c.strip() for c in raw_df.columns})

    missing = [col for col in _KEEP_COLUMNS if col not in raw_df.columns]
    if missing:
        log.warning("Columns not found in the DataFrame: %s", missing)

    # Cast numeric columns so sorting is numeric, not lexicographic. Strings
    # may have leading whitespace; cast strict=False yields nulls on failure.
    numeric_casts = [
        pl.col(c).cast(pl.Float64, strict=False).alias(c)
        for c in ("season", "round", "pick")
        if c in raw_df.columns
    ]
    if numeric_casts:
        raw_df = raw_df.with_columns(numeric_casts)

    # Filter to desired year range
    if "season" in raw_df.columns:
        raw_df = raw_df.filter(pl.col("season") >= start_year)

    present = [c for c in _KEEP_COLUMNS if c in raw_df.columns]
    result = raw_df.select(present)
    log.info("Filtered columns: %s", list(result.columns))

    # Normalise historical team abbreviations to current values
    if "team" in result.columns:
        result = result.with_columns(
            pl.col("team")
            .map_elements(constants.normalize_team, return_dtype=pl.String)
            .alias("team"),
        )

    # Cast to int after filtering (nulls from coerce would have been dropped via filter)
    int_casts = [
        pl.col(c).cast(pl.Int64).alias(c)
        for c in ("season", "round", "pick")
        if c in result.columns
    ]
    if int_casts:
        result = result.with_columns(int_casts)

    if "pick" in result.columns and "season" in result.columns:
        result = result.sort(["season", "pick"])
    log.info("Sorted draft picks by season and pick.")

    # Compute pick-within-round: sequential number within each (season, round)
    if "round" in result.columns and "season" in result.columns:
        result = result.with_columns(
            (pl.col("round").cum_count().over(["season", "round"]))
            .cast(pl.Int64)
            .alias("round_pick"),
        )
        cols = list(result.columns)
        cols.remove("round_pick")
        round_idx = cols.index("round")
        cols.insert(round_idx + 1, "round_pick")
        result = result.select(cols)

    return result


def main() -> None:
    """Download, clean, and save the draft picks CSV."""
    raw_df = _fetch_raw_draft_picks()

    # Persist the raw download so it is available for inspection
    raw_path = constants.DATA_PATH / "draft_picks.csv"
    raw_df.write_csv(raw_path)
    log.info("Saved raw draft picks to %s (%d rows)", raw_path, raw_df.height)

    result = _clean_draft_picks(raw_df, start_year=constants.START_YEAR)

    out_file = constants.DATA_PATH / "cleaned_draft_picks.csv"
    write_df_to_csv(result, out_file, index=True)
    log.info("Saved cleaned draft picks to %s (%d rows)", out_file, result.height)
