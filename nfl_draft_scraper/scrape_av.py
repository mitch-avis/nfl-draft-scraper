"""Calculate Approximate Value (AV) for NFL players using the sportsipy library.

Retrieves player statistics, cleans the data, and computes AV by year, career AV, and weighted
career AV. The results are saved to a CSV file.
"""

from __future__ import annotations

import argparse
import logging
import os
import typing

os.environ["SPORTSIPY_CHROME_COOKIES"] = "1"

import numpy as np
import pandas as pd
import polars as pl
from sportsipy.nfl.constants import PLAYER_SCHEME
from sportsipy.nfl.roster import Player

from nfl_draft_scraper import constants
from nfl_draft_scraper.utils.csv_utils import read_df_from_csv
from nfl_draft_scraper.utils.logger import log

# Suppress noisy GNOME Keyring tracebacks from Chrome cookie extraction
logging.getLogger("sportsipy.chrome_cookies").setLevel(logging.WARNING)

# PFR uses 'data-stat="team_name_abbr"' in stat tables (passing, defense, …) but
# keeps the older 'data-stat="team"' in the games_played table (used for OL, P, K,
# etc.). sportsipy's default scheme only matches the latter. Use a CSS union
# selector so that Player objects correctly populate team_abbreviation regardless
# of which table supplies the data.
PLAYER_SCHEME["team_abbreviation"] = 'td[data-stat="team"], td[data-stat="team_name_abbr"]'

# Franchises that relocated within the tracking window. Maps each historical or
# current abbreviation to the full set of abbreviations for the same franchise.
FRANCHISE_EQUIVALENTS: dict[str, frozenset[str]] = {
    "SDG": frozenset({"SDG", "LAC"}),
    "LAC": frozenset({"SDG", "LAC"}),
    "OAK": frozenset({"OAK", "LVR"}),
    "LVR": frozenset({"OAK", "LVR"}),
    "STL": frozenset({"STL", "LAR"}),
    "LAR": frozenset({"STL", "LAR"}),
}


def _get_at_index(df: pd.DataFrame, idx: typing.Hashable) -> int | str:
    """Return the correct index for .at[]: tuple for MultiIndex, scalar for Index."""
    if isinstance(df.index, pd.MultiIndex):
        return typing.cast(int | str, idx)
    if isinstance(idx, tuple):
        return typing.cast(int | str, idx[0])
    return typing.cast(int | str, idx)


def _clean_stats_df(stats_df: pl.DataFrame) -> pl.DataFrame:
    """Remove rows with null or empty season values."""
    return stats_df.filter(pl.col("season").is_not_null() & (pl.col("season") != ""))


def _get_av_by_year(years_df: pl.DataFrame, all_years: list[int]) -> dict[str, int]:
    """Build a dict mapping each year in all_years to its approximate value (AV).

    Nulls are treated as zero.
    """
    av_by_year = {str(y): 0 for y in all_years}
    for row in years_df.iter_rows(named=True):
        year = str(row["season"])
        if not year.isdigit():
            continue
        if int(year) not in all_years:
            continue
        raw = row.get("approximate_value", 0)
        av_by_year[year] = 0 if raw is None else int(raw)
    return av_by_year


def _get_draft_team_av_by_year(
    years_df: pl.DataFrame, draft_team: str, all_years: list[int]
) -> dict[str, int]:
    """Build a dict mapping each year to AV earned on the drafting franchise only.

    Uses ``FRANCHISE_EQUIVALENTS`` to account for team relocations (e.g. SDG → LAC).
    Years where the player is on a different franchise are recorded as zero.
    """
    franchise_set = FRANCHISE_EQUIVALENTS.get(draft_team, frozenset({draft_team}))
    av_by_year: dict[str, int] = {str(y): 0 for y in all_years}
    for row in years_df.iter_rows(named=True):
        year = str(row["season"])
        if not year.isdigit() or int(year) not in all_years:
            continue
        team = row.get("team_abbreviation")
        if team is None or team not in franchise_set:
            continue
        raw = row.get("approximate_value", 0)
        av_by_year[year] = 0 if raw is None else int(raw)
    return av_by_year


def _calculate_career_av(av_by_year: dict[str, int]) -> int:
    """Return the career AV as the sum of yearly AVs within the tracked year range."""
    return sum(av_by_year.values())


def _calculate_weighted_career_av(av_by_year: dict[str, int], all_years: list[int]) -> float:
    """Calculate a weighted career AV giving 5% less weight to each subsequent year."""
    yearly_av = [av_by_year[str(y)] for y in all_years]
    sorted_av = sorted(yearly_av, reverse=True)
    weights = [max(1 - 0.05 * i, 0) for i in range(len(sorted_av))]
    return round(sum(av * w for av, w in zip(sorted_av, weights, strict=False)), 1)


def _calculate_av(
    player_id: str, all_years: list[int], draft_team: str
) -> tuple[dict[str, int], int, float, int, float]:
    """Retrieve a Player object, clean its stats, and compute AV.

    Return (av_by_year, career_av, weighted_career_av,
    draft_team_career_av, draft_team_weighted_career_av).
    """
    player = Player(player_id)
    if player.dataframe is None:
        raise ValueError(f"No dataframe for player {player_id}")
    stats_df = _clean_stats_df(player.dataframe)
    years_df = stats_df.filter(pl.col("season") != "Career").sort("season")
    av_by_year = _get_av_by_year(years_df, all_years)
    career_av = _calculate_career_av(av_by_year)
    weighted_career_av = _calculate_weighted_career_av(av_by_year, all_years)
    dt_av_by_year = _get_draft_team_av_by_year(years_df, draft_team, all_years)
    dt_career_av = _calculate_career_av(dt_av_by_year)
    dt_weighted_career_av = _calculate_weighted_career_av(dt_av_by_year, all_years)
    return av_by_year, career_av, weighted_career_av, dt_career_av, dt_weighted_career_av


def _handle_av_error(draft_df: pd.DataFrame, idx: typing.Hashable, av_columns: list[str]) -> None:
    """Mark AV columns as NaN and flag the row as incomplete in case of an error."""
    # If idx is a tuple, extract the first non-str element as the row index
    # Ensure row is a scalar (int or str), not a tuple or other type
    row = _get_at_index(draft_df, idx)
    for col in av_columns:
        draft_df.at[row, col] = np.nan
    draft_df.at[row, "career"] = np.nan
    draft_df.at[row, "weighted_career"] = np.nan
    draft_df.at[row, "draft_team_career"] = np.nan
    draft_df.at[row, "draft_team_weighted_career"] = np.nan
    draft_df.at[row, "av_complete"] = False


def _save_checkpoint(draft_df: pd.DataFrame, checkpoint_path: str) -> None:
    """Save a CSV checkpoint of the draft dataframe with AV progress."""
    draft_df.to_csv(checkpoint_path, index=False)
    log.info(
        "💾 Checkpoint saved at %s (%d complete)", checkpoint_path, draft_df["av_complete"].sum()
    )


def _initialize_draft_picks_df(
    draft_path: str, checkpoint_path: str, av_columns: list[str]
) -> pd.DataFrame:
    """Load or initialize the draft picks dataframe, adding AV columns if needed."""
    if os.path.exists(checkpoint_path):
        log.info("🔄 Resuming Phase 1 from checkpoint: %s", checkpoint_path)
        df = pd.read_csv(checkpoint_path)
        if "av_complete" not in df.columns:
            df["av_complete"] = False
    else:
        df = read_df_from_csv(draft_path)
        # add the AV columns
        for col in av_columns + [
            "career",
            "weighted_career",
            "draft_team_career",
            "draft_team_weighted_career",
        ]:
            df[col] = np.nan
        df["av_complete"] = False
    return df


def _update_av(*, force: bool = False, checkpoint_every: int = 20) -> None:
    """Update AV using sportsipy and checkpoint regularly.

    Args:
        force: If True, re-scrape every player regardless of av_complete status.
        checkpoint_every: Save a checkpoint CSV after this many players are processed.

    """
    draft_path = str(constants.DATA_PATH / "cleaned_draft_picks.csv")
    out_path = str(constants.DATA_PATH / "cleaned_draft_picks_with_av.csv")
    checkpoint_path = str(constants.DATA_PATH / "cleaned_draft_picks_with_av_checkpoint.csv")
    all_years = list(range(constants.START_YEAR, constants.END_YEAR + 1))
    av_cols = [str(y) for y in all_years]

    df = _initialize_draft_picks_df(draft_path, checkpoint_path, av_cols)

    processed = 0
    for idx, row_data in df.iterrows():
        av_complete = bool(row_data.get("av_complete", False))

        # Skip already-complete rows unless force mode is enabled
        if av_complete and not force:
            continue

        pid = row_data.get("pfr_player_id")
        name = row_data.get("pfr_player_name")

        if pid is None or (isinstance(pid, float) and pd.isna(pid)) or str(pid).strip() == "":
            _handle_av_error(df, idx, av_cols)
            log.warning("⚠️  Missing pfr_player_id for %s; leaving incomplete", name)
            continue

        draft_team = str(row_data.get("team", ""))
        row = _get_at_index(df, idx)
        try:
            av_by_year, career_av, w_av, dt_career, dt_w = _calculate_av(
                str(pid), all_years, draft_team
            )
            for col in av_cols:
                df.at[row, col] = av_by_year[col]
            df.at[row, "career"] = career_av
            df.at[row, "weighted_career"] = w_av
            df.at[row, "draft_team_career"] = dt_career
            df.at[row, "draft_team_weighted_career"] = dt_w
            df.at[row, "av_complete"] = True
            log.info(
                "✔️  AV via sportsipy: %s (%s) career=%s weighted=%s dt_career=%s dt_weighted=%s",
                name,
                pid,
                career_av,
                w_av,
                dt_career,
                dt_w,
            )
        except (ValueError, KeyError, AttributeError) as e:
            log.warning("⚠️  sportsipy error for %s (%s): %s", name, pid, e)
            _handle_av_error(df, idx, av_cols)

        processed += 1
        if processed % checkpoint_every == 0:
            _save_checkpoint(df, checkpoint_path)

    # final write & cleanup
    df.to_csv(checkpoint_path, index=False)
    log.info("💾 Final checkpoint saved to %s", checkpoint_path)
    final = df.drop(columns=["av_complete"])
    final.to_csv(out_path, index=False)
    log.info("✔️  AV update done, wrote %s", out_path)


def _build_parser() -> argparse.ArgumentParser:
    """Build the CLI argument parser for the AV scraper."""
    parser = argparse.ArgumentParser(
        description="Scrape and update Approximate Value (AV) data for NFL draft picks.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        default=False,
        help="Re-scrape all players, ignoring previously completed rows.",
    )
    parser.add_argument(
        "--checkpoint-every",
        type=int,
        default=20,
        help="Save a checkpoint CSV after this many players are processed (default: 20).",
    )
    return parser


if __name__ == "__main__":
    args = _build_parser().parse_args()
    _update_av(force=args.force, checkpoint_every=args.checkpoint_every)
