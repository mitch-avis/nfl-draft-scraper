"""Calculate Approximate Value (AV) for NFL players using the sportsipy library.

Retrieves player statistics, cleans the data, and computes AV by year, career AV, and weighted
career AV. The results are saved to a CSV file.
"""

import os
import time
import typing

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
from sportsipy.nfl.roster import Player

from nfl_draft_scraper import constants
from nfl_draft_scraper.utils.csv_utils import read_df_from_csv
from nfl_draft_scraper.utils.logger import log


def _get_at_index(df: pd.DataFrame, idx: typing.Hashable) -> int | str:
    """Return the correct index for .at[]: tuple for MultiIndex, scalar for Index."""
    if isinstance(df.index, pd.MultiIndex):
        return typing.cast(int | str, idx)
    if isinstance(idx, tuple):
        return typing.cast(int | str, idx[0])
    return typing.cast(int | str, idx)


def _get_table_id_for_position(position: str) -> str:
    """Return the appropriate Pro-Football-Reference table id for a given position code."""
    table_map = {
        "QB": "passing",
        "RB": "rushing_and_receiving",
        "WR": "rushing_and_receiving",
        "TE": "rushing_and_receiving",
        "OL": "games_played",
        "DL": "defense",
        "LB": "defense",
        "DB": "defense",
        "LS": "defense",
        "K": "kicking",
        "P": "punting",
    }
    return table_map.get(position, "games_played")


def _clean_stats_df(stats_df: pd.DataFrame) -> pd.DataFrame:
    """Remove rows with invalid season index and rename index to 'season'."""
    idx_series = stats_df.index.to_series()
    valid_idx = idx_series.notna() & (idx_series != "")
    if isinstance(stats_df.index, pd.MultiIndex):
        valid_idx = valid_idx & (idx_series != ("",))
    stats_df = stats_df.loc[valid_idx]
    stats_df = stats_df.reset_index()
    stats_df = stats_df.rename(columns={"index": "season"})
    return stats_df


def _get_av_by_year(years_df: pd.DataFrame, all_years: list[int]) -> dict[str, int]:
    """Build a dict mapping each year in all_years to its approximate value (AV).

    NaNs are treated as zero.
    """
    av_by_year = {str(y): 0 for y in all_years}
    for _, stat_row in years_df.iterrows():
        year_val = stat_row["season"]
        year = str(year_val)
        # Only process if year is a digit string and in all_years
        if not year.isdigit():
            continue
        year_int = int(year)
        if year_int not in all_years:
            continue
        raw = stat_row.get("approximate_value", 0)
        val = 0 if raw is None or pd.isna(raw) else int(raw)
        av_by_year[year] = val
    return av_by_year


def _calculate_career_av(career_row: pd.DataFrame, av_by_year: dict[str, int]) -> int:
    """Return the summed career AV from the 'Career' row or the sum of yearly AVs if missing."""
    if not career_row.empty:
        # sportsipy puts career sum in 'approximate_value'
        raw = career_row["approximate_value"].fillna(0).astype(float).iat[0]
        return int(raw)
    return sum(av_by_year.values())


def _calculate_weighted_career_av(av_by_year: dict[str, int], all_years: list[int]) -> float:
    """Calculate a weighted career AV giving 5% less weight to each subsequent year."""
    yearly_av = [av_by_year[str(y)] for y in all_years]
    sorted_av = sorted(yearly_av, reverse=True)
    weights = [max(1 - 0.05 * i, 0) for i in range(len(sorted_av))]
    return round(sum(av * w for av, w in zip(sorted_av, weights, strict=False)), 1)


def _calculate_av(player_id: str, all_years: list[int]) -> tuple[dict[str, int], int, float]:
    """Retrieve a Player object, clean its stats, and compute AV.

    Return AV by year, career AV, and weighted career AV.
    """
    player = Player(player_id)
    if player.dataframe is None:
        raise ValueError(f"No dataframe for player {player_id}")
    stats_df = _clean_stats_df(typing.cast(pd.DataFrame, player.dataframe))
    career_row_df = typing.cast(pd.DataFrame, stats_df[stats_df["season"] == "Career"])
    # Filter and sort non-career rows
    years_mask = stats_df["season"] != "Career"
    years_df = typing.cast(pd.DataFrame, stats_df[years_mask].copy())
    years_df = typing.cast(pd.DataFrame, years_df.sort_values(by=["season"]))
    av_by_year = _get_av_by_year(years_df, all_years)
    career_av = _calculate_career_av(career_row_df, av_by_year)
    weighted_career_av = _calculate_weighted_career_av(av_by_year, all_years)
    return av_by_year, career_av, weighted_career_av


def _handle_av_error(draft_df: pd.DataFrame, idx: typing.Hashable, av_columns: list[str]) -> None:
    """Mark AV columns as NaN and flag the row as incomplete in case of an error."""
    # If idx is a tuple, extract the first non-str element as the row index
    # Ensure row is a scalar (int or str), not a tuple or other type
    row = _get_at_index(draft_df, idx)
    for col in av_columns:
        draft_df.at[row, col] = np.nan
    draft_df.at[row, "career"] = np.nan
    draft_df.at[row, "weighted_career"] = np.nan
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
        for col in av_columns + ["career", "weighted_career"]:
            df[col] = np.nan
        df["av_complete"] = False
    return df


def _is_nonzero(value: typing.Any) -> bool:
    """Return True if value is a number and not zero."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return False
    try:
        return float(value) != 0.0
    except (TypeError, ValueError):
        return False


def _update_av_with_fallback(checkpoint_every: int = 20) -> None:
    """Single pass: update AV using sportsipy, fall back to PFR HTML, and checkpoint regularly."""
    draft_path = os.path.join(constants.DATA_PATH, "cleaned_draft_picks.csv")
    out_path = os.path.join(constants.DATA_PATH, "cleaned_draft_picks_with_av.csv")
    checkpoint_path = os.path.join(
        constants.DATA_PATH, "cleaned_draft_picks_with_av_checkpoint.csv"
    )
    all_years = list(range(constants.START_YEAR, constants.END_YEAR + 1))
    av_cols = [str(y) for y in all_years]
    # current_year_col = str(constants.END_YEAR)

    df = _initialize_draft_picks_df(draft_path, checkpoint_path, av_cols)
    if "av_complete" not in df.columns:
        df["av_complete"] = False

    processed = 0
    for idx, row_data in df.iterrows():
        av_complete = bool(row_data.get("av_complete", False))

        # current_year_val = row_data.get(current_year_col, np.nan)
        # Skip if already complete and current-year AV is nonzero
        # if av_complete and _is_nonzero(current_year_val):
        #     continue

        # Skip if already complete
        if av_complete:
            continue

        pid = row_data.get("pfr_player_id")
        name = row_data.get("pfr_player_name")
        pos = row_data.get("category")
        season = int(row_data.get("season") or 0)

        if season < 2024:
            log.info("ℹ️  Skipping %s; season %s < 2024", name, season)
            continue

        if pid is None or (isinstance(pid, float) and pd.isna(pid)) or str(pid).strip() == "":
            _handle_av_error(df, idx, av_cols)
            log.warning("⚠️  Missing pfr_player_id for %s; leaving incomplete", name)
            continue

        row = _get_at_index(df, idx)
        success = False
        try:
            av_by_year, career_av, w_av = _calculate_av(str(pid), all_years)
            for col in av_cols:
                df.at[row, col] = av_by_year[col]
            df.at[row, "career"] = career_av
            df.at[row, "weighted_career"] = w_av
            df.at[row, "av_complete"] = True
            success = True
            log.info(
                "✔️  AV via sportsipy: %s (%s) career=%s weighted=%s",
                name,
                pid,
                career_av,
                w_av,
            )
        except (ValueError, KeyError, AttributeError) as e:
            log.warning("⚠️  sportsipy error for %s (%s): %s", name, pid, e)

        if not success:
            try:
                av_by_year, c_av, w_av = _scrape_player_av_fallback(
                    str(pid), str(name), str(pos), all_years
                )
                if av_by_year is not None:
                    for col in av_cols:
                        df.at[row, col] = av_by_year[col]
                    df.at[row, "career"] = c_av
                    df.at[row, "weighted_career"] = w_av
                    df.at[row, "av_complete"] = True
                    success = True
                    log.info("🌐 AV via PFR: %s (%s)", name, pid)
            except (requests.RequestException, ValueError, AttributeError) as e:
                log.warning("⚠️  PFR fallback error for %s (%s): %s", name, pid, e)

        if not success:
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


def _parse_av_by_year_from_pfr(games_table, all_years):
    """Parse per‑season AV values from a PFR HTML table, ignoring header rows."""
    av_by_year = {str(y): 0 for y in all_years}
    for row in games_table.tbody.find_all("tr"):
        if row.get("class") and "thead" in row.get("class"):
            continue
        season_cell = row.find("th", {"data-stat": "year_id"})
        av_cell = row.find("td", {"data-stat": "av"})
        if not season_cell or not av_cell:
            continue
        year = season_cell.text.strip()
        year_av = av_cell.text.strip()
        if year.isdigit() and year_av.isdigit() and year in av_by_year:
            av_by_year[year] = int(year_av)
    return av_by_year


def _parse_career_av_from_pfr(
    games_table: BeautifulSoup | typing.Any, av_by_year: dict[str, int]
) -> int:
    """Extract the career AV from the table footer, or sum yearly AV if not present.

    Accepts either a BeautifulSoup or Tag object.
    """
    tfoot = games_table.find("tfoot")
    if tfoot:
        career_row = tfoot.find("tr")
        if career_row is not None:
            cell = career_row.find("td", {"data-stat": "av"})
            if cell and cell.text.strip().isdigit():
                return int(cell.text.strip())
    return sum(av_by_year.values())


def _calculate_weighted_from_pfr(av_by_year: dict[str, int], all_years: list[int]) -> float:
    """Calculate weighted AV from a PFR scrape, same weights as sportsipy version."""
    yearly = [av_by_year[str(y)] for y in all_years]
    sorted_av = sorted(yearly, reverse=True)
    weights = [max(1 - 0.05 * i, 0) for i in range(len(sorted_av))]
    return round(sum(a * w for a, w in zip(sorted_av, weights, strict=False)), 1)


def _scrape_player_av_fallback(
    player_id: str, player_name: str, player_pos: str, all_years: list[int]
) -> tuple[dict[str, int] | None, int | None, float | None]:
    """On failure of sportsipy, fetch and parse AV tables directly from PFR HTML."""
    url = f"https://www.pro-football-reference.com/players/{player_id[0]}/{player_id}.htm"
    resp = requests.get(url, timeout=10)
    time.sleep(3)
    if resp.status_code != 200:
        log.warning("🌐 PFR page fail for %s %s", player_name, player_id)
        return None, None, None
    soup = BeautifulSoup(resp.text, "html.parser")
    tbl_id = _get_table_id_for_position(player_pos)
    table = soup.find("table", id=tbl_id)
    if table is None:
        log.warning("🌐 No PFR table for %s %s", player_name, player_id)
        return None, None, None
    av_by_year = _parse_av_by_year_from_pfr(table, all_years)
    c_av = _parse_career_av_from_pfr(table, av_by_year)
    w_av = _calculate_weighted_from_pfr(av_by_year, all_years)
    return av_by_year, c_av, w_av


if __name__ == "__main__":
    _update_av_with_fallback()
