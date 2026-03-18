"""Module to clean and combine NFL draft big board data from multiple sources.

Reads CSVs from MDDB and JLBB, deduplicates and sorts them,
then fuzzy-matches player names to produce a unified combined CSV per year.
"""

import difflib
import os
import typing

import pandas as pd

from nfl_draft_scraper import constants
from nfl_draft_scraper.utils.logger import log


def _clean_df(data_frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Clean raw big board DataFrame.

    - Drop rows missing 'name' or 'rank'
    - Remove duplicate names
    - Cast 'rank' to int
    - Sort by 'rank' and reset index
    - Return only specified columns
    """
    log.debug("Cleaning DataFrame with columns: %s", data_frame.columns.tolist())
    cleaned = data_frame.dropna(subset=["name", "rank"]).drop_duplicates(subset=["name"])
    cleaned = cleaned.copy()
    cleaned["rank"] = cleaned["rank"].astype(int)
    result = cleaned.sort_values("rank").reset_index(drop=True)[columns]
    log.debug("Cleaned DataFrame shape: %s", result.shape)
    return typing.cast(pd.DataFrame, result)


def _best_match(name: str, choices: list[str], cutoff: float = 0.75) -> str | None:
    """Return the closest matching string from choices for name using difflib.

    Return None if no match exceeds the cutoff threshold.
    """
    matches = difflib.get_close_matches(name, choices, n=1, cutoff=cutoff)
    return matches[0] if matches else None


def _get_record(player: str, df: pd.DataFrame, names: list[str]) -> pd.Series | None:
    """Retrieve the record for a player from a DataFrame, using fuzzy matching if needed."""
    if player in names:
        result = df.query("name == @player")
        if not result.empty:
            return result.iloc[0]
    matched_name = _best_match(player, names)
    if matched_name:
        log.debug("Fuzzy matched '%s' to '%s'", player, matched_name)
        result = df.query("name == @matched_name")
        if not result.empty:
            return result.iloc[0]
    return None


def _build_combined_rows(all_players, mddb_df, jlbb_df, mddb_names, jlbb_names):
    """Build combined rows for all players."""
    combined_rows: list[dict[str, str | float | None]] = []
    for player in all_players:
        mddb_record = _get_record(player, mddb_df, mddb_names)
        jlbb_record = _get_record(player, jlbb_df, jlbb_names)

        # Compute ranks and average

        mddb_rank_val = None
        if mddb_record is not None:
            mddb_rank_val = mddb_record.get("rank", None)
        mddb_has_rank = (
            mddb_rank_val is not None
            and not isinstance(mddb_rank_val, pd.Series)
            and not pd.isna(mddb_rank_val)
        )

        md_rank: int | typing.Any = pd.NA
        if mddb_has_rank:
            md_rank = int(str(mddb_rank_val))

        jlbb_rank_val = None
        if jlbb_record is not None:
            jlbb_rank_val = jlbb_record.get("rank", None)
        jlbb_has_rank = (
            jlbb_rank_val is not None
            and not isinstance(jlbb_rank_val, pd.Series)
            and not pd.isna(jlbb_rank_val)
        )

        jl_rank: int | typing.Any = pd.NA
        if jlbb_has_rank:
            jl_rank = int(str(jlbb_rank_val))

        if isinstance(md_rank, int) and isinstance(jl_rank, int):
            avg_rank: float | typing.Any = (md_rank + jl_rank) / 2
        elif isinstance(md_rank, int):
            avg_rank = float(md_rank)
        elif isinstance(jl_rank, int):
            avg_rank = float(jl_rank)
        else:
            avg_rank = pd.NA

        mddb_pos = ""
        if mddb_record is not None and "pos" in mddb_record:
            mddb_pos = typing.cast(str, mddb_record["pos"])

        jlbb_pos = ""
        if jlbb_pos == "" and jlbb_record is not None and "pos" in jlbb_record:
            jlbb_pos = typing.cast(str, jlbb_record["pos"])

        mddb_school = ""
        if mddb_record is not None and "school" in mddb_record:
            mddb_school = typing.cast(str, mddb_record["school"])

        combined_rows.append(
            {
                "Player": player,
                "Position": mddb_pos if mddb_pos else jlbb_pos,
                "School": mddb_school,
                "MDDB": float(str(mddb_rank_val)) if mddb_has_rank else None,
                "JLBB": float(str(jlbb_rank_val)) if jlbb_has_rank else None,
                "AvgRank": float(avg_rank) if not pd.isna(avg_rank) else None,
            }
        )
    return combined_rows


def _combine_year(year: int) -> None:
    """Read MDDB and JLBB CSVs for the given year, clean and combine them.

    Fuzzy-match names and write out a combined CSV with columns:
    Player, Position, School, MDDB (rank), JLBB (rank), AvgRank.
    """
    log.info("Combining big boards for year %s", year)
    mddb_path = os.path.join(constants.DATA_PATH, f"mddb_big_board_{year}.csv")
    jlbb_path = os.path.join(constants.DATA_PATH, f"jlbb_big_board_{year}.csv")
    log.info("Reading MDDB from %s", mddb_path)
    log.info("Reading JLBB from %s", jlbb_path)

    mddb_df = _clean_df(
        pd.read_csv(mddb_path),
        ["name", "pos", "school", "rank"],
    )
    jlbb_df = _clean_df(
        pd.read_csv(jlbb_path),
        ["name", "pos", "rank"],
    )

    mddb_names = mddb_df["name"].tolist()
    jlbb_names = jlbb_df["name"].tolist()
    all_players = sorted(set(mddb_names) | set(jlbb_names))
    log.info("Total unique players to combine: %d", len(all_players))

    combined_rows = _build_combined_rows(all_players, mddb_df, jlbb_df, mddb_names, jlbb_names)

    result_df = pd.DataFrame(combined_rows)
    log.info("Combined DataFrame shape before deduplication: %s", result_df.shape)

    # Sort by average rank, then MDDB, then JLBB
    result_df = result_df.sort_values(["AvgRank", "MDDB", "JLBB"], na_position="last").reset_index(
        drop=True
    )
    # Dedupe identical entries (same MDDB, JLBB, AvgRank), keeping the longest name
    result_df["name_len"] = result_df["Player"].str.len()
    result_df = result_df.sort_values(
        ["MDDB", "JLBB", "AvgRank", "name_len"],
        ascending=[True, True, True, False],
        na_position="last",
    )
    result_df = result_df.drop_duplicates(subset=["MDDB", "JLBB", "AvgRank"], keep="first").drop(
        columns="name_len"
    )
    # Final sort by AvgRank, then MDDB, then JLBB
    result_df = result_df.sort_values(["AvgRank", "MDDB", "JLBB"], na_position="last").reset_index(
        drop=True
    )

    output_path = os.path.join(constants.DATA_PATH, f"combined_big_board_{year}.csv")
    result_df.to_csv(output_path, index=False)
    log.info("Combined big board for %s saved to %s (%d rows)", year, output_path, len(result_df))


def main() -> None:
    """Iterate from START_YEAR to END_YEAR and generate combined big board CSVs."""
    log.info(
        "Starting big board combination from %d to %d", constants.START_YEAR, constants.END_YEAR
    )
    for season in range(constants.START_YEAR, constants.END_YEAR + 1):
        _combine_year(season)
    log.info("Big board combination complete.")


if __name__ == "__main__":
    main()
