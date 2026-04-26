"""Module to clean and combine NFL draft big board data from multiple sources.

Reads CSVs from Wide Left (WL), JL, and (optionally) Mock Draft Database (MDDB), deduplicates
and sorts them, then fuzzy-matches player names to produce a unified combined CSV per year.

Consensus rank is an inverse-variance weighted mean of the available source ranks. Each source
contributes a per-player point estimate ``R_s`` and a standard error ``sigma_s``::

    consensus = sum(R_s / sigma_s**2) / sum(1 / sigma_s**2)
    consensus_se = sqrt(1 / sum(1 / sigma_s**2))

Per-source standard errors
--------------------------
* **WL** -- Wide Left publishes a rank-adjusted ``Variance`` score centered at 100, where each
  +/-15 step is a notable shift in analyst disagreement. We convert that to a rank-unit
  uncertainty via ``WL_VARIANCE_BASE_SIGMA * (variance / 100)`` and then divide by
  ``sqrt(WL_NEFF)`` to get the standard error of WL's mean. Since WL aggregates ~100 highly
  overlapping public sources, ``WL_NEFF`` is set to 20 (effective independent voters) rather
  than 100. When WL has the player but variance is missing, an average variance score of 100 is
  assumed.
* **MDDB** -- No per-player variance is published, so we use a fixed ``MDDB_PRIOR_SIGMA``
  together with ``MDDB_NEFF`` (also 20). MDDB will not be updated after 2026 (paywall).
* **JL** -- We have the actual individual source ranks, so we use the empirical SD across them
  with a small ``JL_SD_FLOOR`` to prevent overconfidence when sources happen to coincide; the SE
  is then ``JL_SD / sqrt(JL_Sources)``. With only one JL source we fall back to a broad prior.

The ``Sources`` column is preserved as the sum of effective independent voters contributed by
each available source, for human reading; it is no longer used in the math.
"""

from __future__ import annotations

import difflib
import math
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

# Effective number of independent voters represented by each large consensus board. Both WL and
# MDDB pool ~100 public big boards with heavy overlap, so the effective independence is much
# lower than the nominal source count.
WL_NEFF: int = 20
MDDB_NEFF: int = 20

# Rank-unit interpretation of the WL ``Variance`` score. At Variance = 100 ("average"
# disagreement) the SD of an individual analyst's rank around the consensus is approximately
# this many ranks; the per-player SD scales linearly with variance / 100.
WL_VARIANCE_BASE_SIGMA: float = 25.0

# Fixed prior SD (rank units) for the MDDB consensus, since no per-player variance is published.
MDDB_PRIOR_SIGMA: float = 25.0

# Floor on JL's empirical SD to avoid overconfidence when a small number of sources happen to
# agree closely on a single player.
JL_SD_FLOOR: float = 1.0

# Standard error to use for a JL board with a single source (no SD available).
JL_SINGLE_SOURCE_SE: float = WL_VARIANCE_BASE_SIGMA

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


def _wl_standard_error(variance: float | None) -> float:
    """Return the standard error of the WL mean rank for a given per-player variance score.

    A missing variance is treated as the mean (100). The variance score is rank-adjusted and
    centered at 100, so we map it to a rank-unit SD via ``WL_VARIANCE_BASE_SIGMA * v / 100`` and
    divide by ``sqrt(WL_NEFF)`` to obtain the SE of the mean.
    """
    v = variance if variance is not None else 100.0
    sigma_rank = WL_VARIANCE_BASE_SIGMA * (v / 100.0)
    return sigma_rank / math.sqrt(WL_NEFF)


def _jl_standard_error(jl_sd: float | None, jl_n: int) -> float:
    """Return the standard error of the JL mean rank.

    Caller must guarantee ``jl_n >= 1``. With a single source we have no empirical SD, so we fall
    back to a broad prior. With more sources we use ``max(jl_sd, JL_SD_FLOOR) / sqrt(jl_n)`` to
    avoid overconfidence when sources happen to coincide closely.
    """
    if jl_n == 1 or jl_sd is None:
        return JL_SINGLE_SOURCE_SE
    return max(jl_sd, JL_SD_FLOOR) / math.sqrt(jl_n)


def _build_combined_rows(
    all_players: Iterable[str],
    wl_df: pl.DataFrame,
    jlbb_df: pl.DataFrame,
    wl_names: list[str],
    jlbb_names: list[str],
    *,
    jl_source_ranks: dict[str, list[float]],
    mddb_df: pl.DataFrame | None = None,
    mddb_names: list[str] | None = None,
) -> list[dict[str, str | float | int | None]]:
    """Build combined rows for all players with inverse-variance weighted consensus.

    ``mddb_df`` and ``mddb_names`` are optional; when both are provided the MDDB rank is included
    in the weighted consensus and surfaced as the ``MDDB`` column.
    """
    mddb_se = MDDB_PRIOR_SIGMA / math.sqrt(MDDB_NEFF)

    combined_rows: list[dict[str, str | float | int | None]] = []
    for player in all_players:
        wl_record = _get_record(player, wl_df, wl_names)
        jlbb_record = _get_record(player, jlbb_df, jlbb_names)
        mddb_record = (
            _get_record(player, mddb_df, mddb_names)
            if mddb_df is not None and mddb_names is not None
            else None
        )

        # --- Per-source rank values ---
        wl_rank: float | None = (
            float(wl_record["rank"])
            if wl_record is not None and wl_record.get("rank") is not None
            else None
        )
        wl_variance: float | None = None
        if wl_record is not None and wl_record.get("variance") is not None:
            wl_variance = float(wl_record["variance"])

        mddb_rank: float | None = (
            float(mddb_record["rank"])
            if mddb_record is not None and mddb_record.get("rank") is not None
            else None
        )

        jlbb_rank: float | None = (
            float(jlbb_record["rank"])
            if jlbb_record is not None and jlbb_record.get("rank") is not None
            else None
        )

        # --- JL individual source ranks (looked up by the matched JL name) ---
        jl_matched_name: str | None = str(jlbb_record["name"]) if jlbb_record is not None else None
        jl_ranks: list[float] = jl_source_ranks.get(jl_matched_name, []) if jl_matched_name else []
        jl_n = len(jl_ranks)
        jl_avg: float | None = statistics.mean(jl_ranks) if jl_n > 0 else None
        jl_sd: float | None = statistics.pstdev(jl_ranks) if jl_n > 1 else None

        # --- Inverse-variance weighted consensus across whichever sources are present ---
        precision_sum = 0.0  # sum of 1 / sigma^2
        weighted_sum = 0.0  # sum of rank / sigma^2
        sources_eff = 0  # human-readable sum of effective independent voters

        if wl_rank is not None:
            se_wl = _wl_standard_error(wl_variance)
            w = 1.0 / (se_wl * se_wl)
            precision_sum += w
            weighted_sum += wl_rank * w
            sources_eff += WL_NEFF
        if mddb_rank is not None:
            w = 1.0 / (mddb_se * mddb_se)
            precision_sum += w
            weighted_sum += mddb_rank * w
            sources_eff += MDDB_NEFF
        if jl_avg is not None:
            se_jl = _jl_standard_error(jl_sd, jl_n)
            w = 1.0 / (se_jl * se_jl)
            precision_sum += w
            weighted_sum += jl_avg * w
            sources_eff += jl_n

        consensus: float | None
        consensus_se: float | None
        if precision_sum > 0:
            consensus = weighted_sum / precision_sum
            consensus_se = math.sqrt(1.0 / precision_sum)
        else:
            consensus = None
            consensus_se = None

        # --- Position / School: prefer WL, then MDDB, then JLBB ---
        def _field(record: dict[str, Any] | None, key: str) -> str:
            """Return ``record[key]`` as a non-null string, or empty string when absent."""
            if record is None or record.get(key) is None:
                return ""
            return str(record[key])

        pos = _field(wl_record, "pos") or _field(mddb_record, "pos") or _field(jlbb_record, "pos")
        school = (
            _field(wl_record, "school")
            or _field(mddb_record, "school")
            or _field(jlbb_record, "school")
        )

        combined_rows.append(
            {
                "Player": player,
                "Position": pos,
                "School": school,
                "WL": wl_rank,
                "WL_Variance": wl_variance,
                "MDDB": mddb_rank,
                "JLBB": jlbb_rank,
                "JL_Avg": round(jl_avg, 4) if jl_avg is not None else None,
                "JL_SD": round(jl_sd, 4) if jl_sd is not None else None,
                "JL_Sources": jl_n if jl_n > 0 else None,
                "Consensus": round(consensus, 4) if consensus is not None else None,
                "Consensus_SE": round(consensus_se, 4) if consensus_se is not None else None,
                "Sources": sources_eff if sources_eff > 0 else None,
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
    "WL_Variance": pl.Float64,
    "MDDB": pl.Float64,
    "JLBB": pl.Float64,
    "JL_Avg": pl.Float64,
    "JL_SD": pl.Float64,
    "JL_Sources": pl.Int64,
    "Consensus": pl.Float64,
    "Consensus_SE": pl.Float64,
    "Sources": pl.Int64,
}


def _combine_year(year: int) -> None:
    """Read WL and JL CSVs (and optional MDDB) for the given year, clean and combine them.

    Fuzzy-match names and write out a combined CSV with columns: Player, Position, School, WL,
    WL_Variance, MDDB, JLBB, JL_Avg, JL_SD, JL_Sources, Consensus, Consensus_SE, Sources.
    """
    log.info("Combining big boards for year %s", year)
    wl_path = constants.DATA_PATH / f"wl_big_board_{year}.csv"
    jlbb_path = constants.DATA_PATH / f"jl_big_board_{year}.csv"
    mddb_path = constants.DATA_PATH / f"mddb_big_board_{year}.csv"
    log.info("Reading WL from %s", wl_path)
    log.info("Reading JLBB from %s", jlbb_path)

    wl_df = _clean_df(
        pl.read_csv(wl_path, null_values=["NA"]),
        ["name", "pos", "school", "rank", "variance"],
    )

    # Read the full JL CSV to access individual source rank columns
    jl_raw = pl.read_csv(jlbb_path, null_values=["NA"])
    jl_source_ranks = _extract_jl_source_ranks(
        jl_raw.drop_nulls(subset=["name", "rank"]).unique(
            subset=["name"], keep="first", maintain_order=True
        )
    )
    log.info("Extracted JL source ranks for %d players", len(jl_source_ranks))

    jlbb_df = _clean_df(jl_raw, ["name", "pos", "school", "rank"])

    # MDDB is optional: only include it when a historical CSV exists for this year.
    mddb_df: pl.DataFrame | None = None
    mddb_names: list[str] | None = None
    if mddb_path.is_file():
        log.info("Reading MDDB from %s", mddb_path)
        mddb_df = _clean_df(
            pl.read_csv(mddb_path, null_values=["NA"]),
            ["name", "pos", "school", "rank"],
        )
        mddb_names = mddb_df["name"].to_list()
    else:
        log.info("No MDDB file at %s; skipping MDDB source", mddb_path)

    wl_names = wl_df["name"].to_list()
    jlbb_names = jlbb_df["name"].to_list()
    all_player_set: set[str] = set(wl_names) | set(jlbb_names)
    if mddb_names is not None:
        all_player_set |= set(mddb_names)
    all_players = sorted(all_player_set)
    log.info("Total unique players to combine: %d", len(all_players))

    combined_rows = _build_combined_rows(
        all_players,
        wl_df,
        jlbb_df,
        wl_names,
        jlbb_names,
        jl_source_ranks=jl_source_ranks,
        mddb_df=mddb_df,
        mddb_names=mddb_names,
    )

    result_df = pl.DataFrame(combined_rows, schema=_COMBINED_SCHEMA)
    log.info("Combined DataFrame shape before deduplication: %s", result_df.shape)

    # Sort by consensus, then WL, then MDDB, then JLBB
    result_df = result_df.sort(["Consensus", "WL", "MDDB", "JLBB"], nulls_last=True)
    # Dedupe identical entries (same WL, MDDB, JLBB, Consensus), keeping the longest name
    result_df = result_df.with_columns(pl.col("Player").str.len_chars().alias("name_len"))
    result_df = result_df.sort(
        ["WL", "MDDB", "JLBB", "Consensus", "name_len"],
        descending=[False, False, False, False, True],
        nulls_last=True,
    )
    result_df = result_df.unique(
        subset=["WL", "MDDB", "JLBB", "Consensus"], keep="first", maintain_order=True
    ).drop("name_len")
    # Final sort by Consensus, then WL, then MDDB, then JLBB
    result_df = result_df.sort(["Consensus", "WL", "MDDB", "JLBB"], nulls_last=True)

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
