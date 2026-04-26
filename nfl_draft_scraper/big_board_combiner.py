"""Module to clean and combine NFL draft big board data from multiple sources.

Reads CSVs from Wide Left (WL) and JL, deduplicates and sorts them, then fuzzy-matches player
names to produce a unified combined CSV per year.

The weighted consensus formula treats each JL individual-source rank as one vote and the single WL
composite rank as ``WL_WEIGHT`` votes:

    consensus = (jl_avg × jl_n + wl_rank × WL_WEIGHT) / (jl_n + WL_WEIGHT)

When only one board has a player, that board's value (JL avg or WL rank) is used directly.
"""

from __future__ import annotations

import difflib
import statistics
from collections.abc import Iterable
from typing import Any

import polars as pl

from nfl_draft_scraper import constants
from nfl_draft_scraper.merge_bb_ranks_to_picks import (
    first_names_compatible,
    last_names_compatible,
)
from nfl_draft_scraper.utils.logger import log

# Fixed weight assigned to the WL composite rank when computing the weighted consensus.
# Represents roughly the number of unique, high-quality sources in the WL pool that are not
# already captured by JL's individual sources.
WL_WEIGHT: int = 6

# Columns in the JL CSV that are metadata, not source-rank columns.
_JL_META_COLUMNS = frozenset({"rank", "name", "pos", "school", "conference", "avg", "sd"})


def _clean_df(data_frame: pl.DataFrame, columns: list[str]) -> pl.DataFrame:
    """Clean raw big board DataFrame.

    - Drop rows missing 'name' or 'rank'
    - Remove duplicate names
    - Cast 'rank' to int
    - Sort by 'rank' and reset index
    - Return only specified columns
    """
    log.debug("Cleaning DataFrame with columns: %s", data_frame.columns)
    cleaned = data_frame.drop_nulls(subset=["name", "rank"]).unique(
        subset=["name"], keep="first", maintain_order=True
    )
    cleaned = cleaned.with_columns(pl.col("rank").cast(pl.Int64).alias("rank"))
    present = [c for c in columns if c in cleaned.columns]
    result = cleaned.sort("rank").select(present)
    log.debug("Cleaned DataFrame shape: %s", result.shape)
    return result


def _best_match(name: str, choices: list[str], cutoff: float = 0.75) -> str | None:
    """Return the closest matching string from choices for name using difflib.

    Return None if no match exceeds the cutoff threshold, if the candidate's last name is not
    compatible, or if the first names are incompatible.
    """
    matches = difflib.get_close_matches(name, choices, n=1, cutoff=cutoff)
    if not matches:
        return None
    candidate = matches[0]
    if not last_names_compatible(name, candidate):
        return None
    if not first_names_compatible(name, candidate):
        return None
    return candidate


def _get_record(player: str, df: pl.DataFrame, names: list[str]) -> dict[str, Any] | None:
    """Retrieve the record for a player from a DataFrame, using fuzzy matching if needed.

    Returns the matched row as a name-keyed dict, or None when no candidate matches.
    """
    if player in names:
        result = df.filter(pl.col("name") == player)
        if not result.is_empty():
            return result.row(0, named=True)
    matched_name = _best_match(player, names)
    if matched_name:
        log.debug("Fuzzy matched '%s' to '%s'", player, matched_name)
        result = df.filter(pl.col("name") == matched_name)
        if not result.is_empty():
            return result.row(0, named=True)
    return None


def _build_combined_rows(
    all_players: Iterable[str],
    wl_df: pl.DataFrame,
    jlbb_df: pl.DataFrame,
    wl_names: list[str],
    jlbb_names: list[str],
    *,
    jl_source_ranks: dict[str, list[float]],
) -> list[dict[str, str | float | int | None]]:
    """Build combined rows for all players with weighted consensus ranking."""
    combined_rows: list[dict[str, str | float | int | None]] = []
    for player in all_players:
        wl_record = _get_record(player, wl_df, wl_names)
        jlbb_record = _get_record(player, jlbb_df, jlbb_names)

        # --- WL rank ---
        wl_rank: int | None = None
        if wl_record is not None:
            raw_wl = wl_record.get("rank")
            if raw_wl is not None:
                wl_rank = int(raw_wl)

        # --- JLBB composite rank ---
        jlbb_rank: float | None = None
        if jlbb_record is not None:
            raw_jl = jlbb_record.get("rank")
            if raw_jl is not None:
                jlbb_rank = float(raw_jl)

        # --- JL individual source ranks --- Look up by the matched name (which may differ from
        # `player` due to fuzzy matching)
        jl_matched_name: str | None = str(jlbb_record["name"]) if jlbb_record is not None else None
        jl_ranks: list[float] = jl_source_ranks.get(jl_matched_name, []) if jl_matched_name else []
        jl_n = len(jl_ranks)

        # JL aggregate stats
        jl_avg: float | None = statistics.mean(jl_ranks) if jl_n > 0 else None
        jl_sd: float | None = statistics.pstdev(jl_ranks) if jl_n > 0 else None

        # --- Weighted consensus ---
        consensus: float | None
        total_sources: int | None
        if wl_rank is not None and jl_avg is not None:
            consensus = (jl_avg * jl_n + wl_rank * WL_WEIGHT) / (jl_n + WL_WEIGHT)
            total_sources = jl_n + WL_WEIGHT
        elif jl_avg is not None:
            consensus = jl_avg
            total_sources = jl_n
        elif wl_rank is not None:
            consensus = float(wl_rank)
            total_sources = WL_WEIGHT
        else:
            consensus = None
            total_sources = None

        # --- Position / School ---
        wl_pos = ""
        if wl_record is not None and wl_record.get("pos") is not None:
            wl_pos = str(wl_record["pos"])

        jlbb_pos = ""
        if jlbb_record is not None and jlbb_record.get("pos") is not None:
            jlbb_pos = str(jlbb_record["pos"])

        wl_school = ""
        if wl_record is not None and wl_record.get("school") is not None:
            wl_school = str(wl_record["school"])

        jlbb_school = ""
        if jlbb_record is not None and jlbb_record.get("school") is not None:
            jlbb_school = str(jlbb_record["school"])

        combined_rows.append(
            {
                "Player": player,
                "Position": wl_pos if wl_pos else jlbb_pos,
                "School": wl_school if wl_school else jlbb_school,
                "WL": float(wl_rank) if wl_rank is not None else None,
                "JLBB": jlbb_rank,
                "JL_Avg": round(jl_avg, 4) if jl_avg is not None else None,
                "JL_SD": round(jl_sd, 4) if jl_sd is not None else None,
                "JL_Sources": jl_n if jl_n > 0 else None,
                "Consensus": round(consensus, 4) if consensus is not None else None,
                "Sources": total_sources,
            }
        )
    return combined_rows


def _extract_jl_source_ranks(jl_df: pl.DataFrame) -> dict[str, list[float]]:
    """Extract per-player individual source ranks from the JL big board DataFrame.

    Returns a dict mapping player name to a list of non-null source rank values. Source columns are
    identified as any column not in ``_JL_META_COLUMNS``.
    """
    source_cols = [c for c in jl_df.columns if c not in _JL_META_COLUMNS]
    log.debug("JL source columns: %s", source_cols)

    result: dict[str, list[float]] = {}
    for row in jl_df.iter_rows(named=True):
        name = str(row["name"])
        ranks = [float(row[c]) for c in source_cols if row[c] is not None]
        result[name] = ranks
    return result


# Schema for the combined big board output. Polars cannot infer a stable schema from an empty list
# of dicts, so we declare it explicitly. Mixed-numeric columns use Float64 so nulls are
# representable.
_COMBINED_SCHEMA: dict[str, type[pl.DataType] | pl.DataType] = {
    "Player": pl.String,
    "Position": pl.String,
    "School": pl.String,
    "WL": pl.Float64,
    "JLBB": pl.Float64,
    "JL_Avg": pl.Float64,
    "JL_SD": pl.Float64,
    "JL_Sources": pl.Int64,
    "Consensus": pl.Float64,
    "Sources": pl.Int64,
}


def _combine_year(year: int) -> None:
    """Read WL and JL CSVs for the given year, clean and combine them.

    Fuzzy-match names and write out a combined CSV with columns: Player, Position, School, WL,
    JLBB, JL_Avg, JL_SD, JL_Sources, Consensus, Sources.
    """
    log.info("Combining big boards for year %s", year)
    wl_path = constants.DATA_PATH / f"wl_big_board_{year}.csv"
    jlbb_path = constants.DATA_PATH / f"jl_big_board_{year}.csv"
    log.info("Reading WL from %s", wl_path)
    log.info("Reading JLBB from %s", jlbb_path)

    wl_df = _clean_df(
        pl.read_csv(wl_path),
        ["name", "pos", "school", "rank"],
    )

    # Read the full JL CSV to access individual source rank columns
    jl_raw = pl.read_csv(jlbb_path)
    jl_source_ranks = _extract_jl_source_ranks(
        jl_raw.drop_nulls(subset=["name", "rank"]).unique(
            subset=["name"], keep="first", maintain_order=True
        )
    )
    log.info("Extracted JL source ranks for %d players", len(jl_source_ranks))

    jlbb_df = _clean_df(jl_raw, ["name", "pos", "school", "rank"])

    wl_names = wl_df["name"].to_list()
    jlbb_names = jlbb_df["name"].to_list()
    all_players = sorted(set(wl_names) | set(jlbb_names))
    log.info("Total unique players to combine: %d", len(all_players))

    combined_rows = _build_combined_rows(
        all_players, wl_df, jlbb_df, wl_names, jlbb_names, jl_source_ranks=jl_source_ranks
    )

    result_df = pl.DataFrame(combined_rows, schema=_COMBINED_SCHEMA)
    log.info("Combined DataFrame shape before deduplication: %s", result_df.shape)

    # Sort by consensus, then WL, then JLBB
    result_df = result_df.sort(["Consensus", "WL", "JLBB"], nulls_last=True)
    # Dedupe identical entries (same WL, JLBB, Consensus), keeping the longest name
    result_df = result_df.with_columns(pl.col("Player").str.len_chars().alias("name_len"))
    result_df = result_df.sort(
        ["WL", "JLBB", "Consensus", "name_len"],
        descending=[False, False, False, True],
        nulls_last=True,
    )
    result_df = result_df.unique(
        subset=["WL", "JLBB", "Consensus"], keep="first", maintain_order=True
    ).drop("name_len")
    # Final sort by Consensus, then WL, then JLBB
    result_df = result_df.sort(["Consensus", "WL", "JLBB"], nulls_last=True)

    output_path = constants.DATA_PATH / f"combined_big_board_{year}.csv"
    result_df.write_csv(output_path)
    log.info("Combined big board for %s saved to %s (%d rows)", year, output_path, result_df.height)


def main() -> None:
    """Iterate from START_YEAR to END_YEAR and generate combined big board CSVs."""
    log.info(
        "Starting big board combination from %d to %d", constants.START_YEAR, constants.END_YEAR
    )
    for season in range(constants.START_YEAR, constants.END_YEAR + 1):
        _combine_year(season)
    log.info("Big board combination complete.")
