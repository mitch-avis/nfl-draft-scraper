"""Module to clean and combine NFL draft big board data from multiple sources.

Reads CSVs from MDDB and JL, deduplicates and sorts them,
then fuzzy-matches player names to produce a unified combined CSV per year.

The weighted consensus formula treats each JL individual-source rank as one
vote and the single MDDB composite rank as ``MDDB_WEIGHT`` votes:

    consensus = (jl_avg × jl_n + mddb_rank × MDDB_WEIGHT) / (jl_n + MDDB_WEIGHT)

When only one board has a player, that board's value (JL avg or MDDB rank)
is used directly.
"""

from __future__ import annotations

import difflib
import statistics
import typing

import pandas as pd

from nfl_draft_scraper import constants
from nfl_draft_scraper.merge_bb_ranks_to_picks import (
    _first_names_compatible,
    _last_names_compatible,
)
from nfl_draft_scraper.utils.logger import log

# Fixed weight assigned to the MDDB composite rank when computing the weighted
# consensus.  Represents roughly the number of unique, high-quality sources in
# the MDDB pool that are not already captured by JL's individual sources.
MDDB_WEIGHT: int = 6

# Columns in the JL CSV that are metadata, not source-rank columns.
_JL_META_COLUMNS = frozenset({"rank", "name", "pos", "school", "conference", "avg", "sd"})


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
    present = [c for c in columns if c in cleaned.columns]
    result = cleaned.sort_values("rank").reset_index(drop=True)[present]
    log.debug("Cleaned DataFrame shape: %s", result.shape)
    return typing.cast(pd.DataFrame, result)


def _best_match(name: str, choices: list[str], cutoff: float = 0.75) -> str | None:
    """Return the closest matching string from choices for name using difflib.

    Return None if no match exceeds the cutoff threshold, if the candidate's
    last name is not compatible, or if the first names are incompatible.
    """
    matches = difflib.get_close_matches(name, choices, n=1, cutoff=cutoff)
    if not matches:
        return None
    candidate = matches[0]
    if not _last_names_compatible(name, candidate):
        return None
    if not _first_names_compatible(name, candidate):
        return None
    return candidate


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


def _build_combined_rows(
    all_players,
    mddb_df,
    jlbb_df,
    mddb_names,
    jlbb_names,
    *,
    jl_source_ranks: dict[str, list[float]],
):
    """Build combined rows for all players with weighted consensus ranking."""
    combined_rows: list[dict[str, str | float | int | None]] = []
    for player in all_players:
        mddb_record = _get_record(player, mddb_df, mddb_names)
        jlbb_record = _get_record(player, jlbb_df, jlbb_names)

        # --- MDDB rank ---
        mddb_rank_val = None
        if mddb_record is not None:
            mddb_rank_val = mddb_record.get("rank", None)
        mddb_has_rank = (
            mddb_rank_val is not None
            and not isinstance(mddb_rank_val, pd.Series)
            and not pd.isna(mddb_rank_val)
        )
        md_rank: int | None = int(str(mddb_rank_val)) if mddb_has_rank else None

        # --- JLBB composite rank ---
        jlbb_rank_val = None
        if jlbb_record is not None:
            jlbb_rank_val = jlbb_record.get("rank", None)
        jlbb_has_rank = (
            jlbb_rank_val is not None
            and not isinstance(jlbb_rank_val, pd.Series)
            and not pd.isna(jlbb_rank_val)
        )

        # --- JL individual source ranks ---
        # Look up by the matched name (which may differ from `player` due to fuzzy matching)
        jl_matched_name = typing.cast(str, jlbb_record["name"]) if jlbb_record is not None else None
        jl_ranks: list[float] = jl_source_ranks.get(jl_matched_name, []) if jl_matched_name else []
        jl_n = len(jl_ranks)

        # JL aggregate stats
        jl_avg: float | None = statistics.mean(jl_ranks) if jl_n > 0 else None
        jl_sd: float | None = statistics.pstdev(jl_ranks) if jl_n > 0 else None

        # --- Weighted consensus ---
        if md_rank is not None and jl_avg is not None:
            consensus = (jl_avg * jl_n + md_rank * MDDB_WEIGHT) / (jl_n + MDDB_WEIGHT)
            total_sources = jl_n + MDDB_WEIGHT
        elif jl_avg is not None:
            consensus = jl_avg
            total_sources = jl_n
        elif md_rank is not None:
            consensus = float(md_rank)
            total_sources = MDDB_WEIGHT
        else:
            consensus = None
            total_sources = None

        # --- Position / School ---
        mddb_pos = ""
        if mddb_record is not None and "pos" in mddb_record:
            mddb_pos = typing.cast(str, mddb_record["pos"])

        jlbb_pos = ""
        if jlbb_record is not None and "pos" in jlbb_record:
            jlbb_pos = typing.cast(str, jlbb_record["pos"])

        mddb_school = ""
        if mddb_record is not None and "school" in mddb_record:
            mddb_school = typing.cast(str, mddb_record["school"])

        jlbb_school = ""
        if jlbb_record is not None and "school" in jlbb_record:
            jlbb_school = typing.cast(str, jlbb_record["school"])

        combined_rows.append(
            {
                "Player": player,
                "Position": mddb_pos if mddb_pos else jlbb_pos,
                "School": mddb_school if mddb_school else jlbb_school,
                "MDDB": float(str(mddb_rank_val)) if mddb_has_rank else None,
                "JLBB": float(str(jlbb_rank_val)) if jlbb_has_rank else None,
                "JL_Avg": round(jl_avg, 4) if jl_avg is not None else None,
                "JL_SD": round(jl_sd, 4) if jl_sd is not None else None,
                "JL_Sources": jl_n if jl_n > 0 else None,
                "Consensus": round(consensus, 4) if consensus is not None else None,
                "Sources": total_sources,
            }
        )
    return combined_rows


def _extract_jl_source_ranks(jl_df: pd.DataFrame) -> dict[str, list[float]]:
    """Extract per-player individual source ranks from the JL big board DataFrame.

    Returns a dict mapping player name to a list of non-NaN source rank values.
    Source columns are identified as any column not in ``_JL_META_COLUMNS``.
    """
    source_cols = [c for c in jl_df.columns if c not in _JL_META_COLUMNS]
    log.debug("JL source columns: %s", source_cols)

    result: dict[str, list[float]] = {}
    for _, row in jl_df.iterrows():
        name = typing.cast(str, row["name"])
        ranks = [float(typing.cast(float, row[c])) for c in source_cols if bool(pd.notna(row[c]))]
        result[name] = ranks
    return result


def _combine_year(year: int) -> None:
    """Read MDDB and JL CSVs for the given year, clean and combine them.

    Fuzzy-match names and write out a combined CSV with columns:
    Player, Position, School, MDDB, JLBB, JL_Avg, JL_SD, JL_Sources,
    Consensus, Sources.
    """
    log.info("Combining big boards for year %s", year)
    mddb_path = constants.DATA_PATH / f"mddb_big_board_{year}.csv"
    jlbb_path = constants.DATA_PATH / f"jl_big_board_{year}.csv"
    log.info("Reading MDDB from %s", mddb_path)
    log.info("Reading JLBB from %s", jlbb_path)

    mddb_df = _clean_df(
        pd.read_csv(mddb_path),
        ["name", "pos", "school", "rank"],
    )

    # Read the full JL CSV to access individual source rank columns
    jl_raw = pd.read_csv(jlbb_path)
    jl_source_ranks = _extract_jl_source_ranks(
        jl_raw.dropna(subset=["name", "rank"]).drop_duplicates(subset=["name"])
    )
    log.info("Extracted JL source ranks for %d players", len(jl_source_ranks))

    jlbb_df = _clean_df(jl_raw, ["name", "pos", "school", "rank"])

    mddb_names = mddb_df["name"].tolist()
    jlbb_names = jlbb_df["name"].tolist()
    all_players = sorted(set(mddb_names) | set(jlbb_names))
    log.info("Total unique players to combine: %d", len(all_players))

    combined_rows = _build_combined_rows(
        all_players, mddb_df, jlbb_df, mddb_names, jlbb_names, jl_source_ranks=jl_source_ranks
    )

    result_df = pd.DataFrame(combined_rows)
    log.info("Combined DataFrame shape before deduplication: %s", result_df.shape)

    # Sort by consensus, then MDDB, then JLBB
    result_df = result_df.sort_values(
        ["Consensus", "MDDB", "JLBB"], na_position="last"
    ).reset_index(drop=True)
    # Dedupe identical entries (same MDDB, JLBB, Consensus), keeping the longest name
    result_df["name_len"] = result_df["Player"].str.len()
    result_df = result_df.sort_values(
        ["MDDB", "JLBB", "Consensus", "name_len"],
        ascending=[True, True, True, False],
        na_position="last",
    )
    result_df = result_df.drop_duplicates(subset=["MDDB", "JLBB", "Consensus"], keep="first").drop(
        columns="name_len"
    )
    # Final sort by Consensus, then MDDB, then JLBB
    result_df = result_df.sort_values(
        ["Consensus", "MDDB", "JLBB"], na_position="last"
    ).reset_index(drop=True)

    output_path = constants.DATA_PATH / f"combined_big_board_{year}.csv"
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
