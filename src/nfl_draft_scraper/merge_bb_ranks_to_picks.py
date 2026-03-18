"""Merge big board ranks into cleaned draft picks with AV for each draft year.

For each year, matches drafted players to their big board ranks using fuzzy matching,
and outputs a new CSV with round, pick, team, player, position, category, college,
MDDB Rank, JLBB Rank, AvgRank, yearly AVs, career AV, and weighted career AV.

Output: draft_picks_with_big_board_ranks_<year>.csv for each year.
"""

import difflib
import os
import typing
from typing import Any

import pandas as pd

from nfl_draft_scraper import constants
from nfl_draft_scraper.utils.logger import log


def _fuzzy_match_player(
    name: str,
    choices: list[str],
    cutoff: float = 0.6,
) -> str | None:
    """Return the closest matching string from choices for name using difflib.

    Return None if no match exceeds the cutoff threshold.
    """
    matches: list[str] = difflib.get_close_matches(str(name), choices, n=1, cutoff=cutoff)
    return matches[0] if matches else None


def _get_rank_lists(
    picks_year: pd.DataFrame,
    bb_lookup: dict[str, dict[str, Any]],
    bb_names: list[str],
) -> tuple[list[Any | None], list[Any | None], list[Any | None]]:
    """Fuzzy match and collect big board ranks for each drafted player."""
    mddb_ranks: list[Any | None] = []
    jlbb_ranks: list[Any | None] = []
    avgranks: list[Any | None] = []

    for _, row in picks_year.iterrows():
        player_clean: str = typing.cast(str, row["pfr_player_name_clean"])
        match: str | None = _fuzzy_match_player(player_clean, bb_names)
        if match:
            ranks: dict[str, Any] = bb_lookup[match]
            mddb_ranks.append(ranks.get("MDDB"))
            jlbb_ranks.append(ranks.get("JLBB"))
            avgranks.append(ranks.get("AvgRank"))
            log.debug("Matched %s to %s", row["pfr_player_name"], match)
        else:
            mddb_ranks.append(None)
            jlbb_ranks.append(None)
            avgranks.append(None)
            log.info("No big board match for %s (%s)", row["pfr_player_name"], row["college"])

    return mddb_ranks, jlbb_ranks, avgranks


def _get_av_columns(df: pd.DataFrame) -> list[str]:
    """Return the list of AV columns present in the DataFrame."""
    av_years: list[str] = [str(y) for y in range(constants.START_YEAR, constants.END_YEAR + 1)]
    av_cols: list[str] = av_years + ["career", "weighted_career"]
    return [col for col in av_cols if col in df.columns]


def _reorder_and_save(
    picks_year: pd.DataFrame,
    output_path: str,
    av_cols: list[str],
) -> None:
    """Reorder columns and save the DataFrame to CSV."""
    base_cols: list[str] = [
        "round",
        "pick",
        "team",
        "pfr_player_name",
        "position",
        "category",
        "college",
    ]
    rank_cols: list[str] = ["MDDB Rank", "JLBB Rank", "AvgRank"]
    ordered_cols: list[str] = base_cols + rank_cols + av_cols
    picks_out = picks_year[ordered_cols].copy()
    # Rename and save to CSV
    picks_out.columns = ["player" if col == "pfr_player_name" else col for col in picks_out.columns]
    picks_out.to_csv(output_path, index=False)


def _merge_big_board_ranks_for_year(year: int) -> None:
    """Merge big board ranks into cleaned draft picks with AV for a given year.

    Output a new CSV for the year.
    """
    picks_path: str = os.path.join(constants.DATA_PATH, "cleaned_draft_picks_with_av.csv")
    bb_path: str = os.path.join(constants.DATA_PATH, f"combined_big_board_{year}.csv")
    output_path: str = os.path.join(
        constants.DATA_PATH, f"draft_picks_with_big_board_ranks_{year}.csv"
    )

    if not os.path.exists(picks_path):
        log.error("Draft picks file not found: %s", picks_path)
        return
    if not os.path.exists(bb_path):
        log.warning("Big board file not found for %s: %s", year, bb_path)
        return

    picks_df: pd.DataFrame = pd.read_csv(picks_path)
    picks_year: pd.DataFrame = typing.cast(
        pd.DataFrame, picks_df[picks_df["season"] == year].copy()
    )
    if picks_year.empty:
        log.info("No draft picks found for %s. Skipping.", year)
        return

    bb_df: pd.DataFrame = pd.read_csv(bb_path)
    picks_year["pfr_player_name_clean"] = picks_year["pfr_player_name"].str.strip().str.lower()
    bb_df["Player_clean"] = bb_df["Player"].str.strip().str.lower()

    # remove duplicate big board entries so index is unique
    dupes_mask = bb_df["Player_clean"].duplicated()
    dupes = bb_df.loc[dupes_mask, "Player_clean"]
    if not dupes.empty:
        log.warning("Duplicate big board player names for %s: %s", year, list(dupes.unique()))
    bb_df = bb_df.drop_duplicates(subset="Player_clean", keep="first")

    # Convert keys and nested keys to str for type safety
    indexed_df = bb_df.set_index("Player_clean")[["MDDB", "JLBB", "AvgRank"]]
    # Using typing.cast since to_dict overload resolution is strict
    raw_lookup: dict[Any, dict[Any, Any]] = typing.cast(
        dict[Any, dict[Any, Any]], indexed_df.to_dict()
    )
    bb_lookup: dict[str, dict[str, Any]] = {
        str(k): {str(subk): v for subk, v in vdict.items()} for k, vdict in raw_lookup.items()
    }
    bb_names: list[str] = list(bb_lookup.keys())

    mddb_ranks, jlbb_ranks, avgranks = _get_rank_lists(picks_year, bb_lookup, bb_names)
    picks_year["MDDB Rank"] = mddb_ranks
    picks_year["JLBB Rank"] = jlbb_ranks
    picks_year["AvgRank"] = avgranks

    av_cols: list[str] = _get_av_columns(picks_year)
    _reorder_and_save(picks_year, output_path, av_cols)
    log.info(
        "Saved merged draft picks with big board ranks for %s to %s",
        year,
        output_path,
    )


def main() -> None:
    """Merge big board ranks into draft picks for each year in the configured range."""
    log.info(
        "Starting merge of big board ranks for years %s-%s",
        constants.START_YEAR,
        constants.END_YEAR,
    )
    for year in range(constants.START_YEAR, constants.END_YEAR + 1):
        _merge_big_board_ranks_for_year(year)
    log.info("Done.")


if __name__ == "__main__":
    main()
