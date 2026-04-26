"""Wide Left Big Board Scraper.

Fetches Arif Hasan's consensus big boards from Wide Left
(https://www.wideleft.football). Each year's data is published as a Google Sheet; the scraper uses
the sheet's CSV export endpoint, parses the rows below the metadata header, and writes a normalized
``wl_big_board_{year}.csv`` with the columns ``rank, name, pos, school``.
"""

from __future__ import annotations

import csv
import io
import random
import time

import requests

from nfl_draft_scraper import constants
from nfl_draft_scraper.utils.csv_utils import save_csv
from nfl_draft_scraper.utils.logger import log

SLEEP_MIN = 1
SLEEP_MAX = 3
_rng = random.SystemRandom()

_GSHEETS_EXPORT_URL = "https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid=0"

_REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:128.0) Gecko/20100101 Firefox/128.0",
    "Accept": "text/csv,*/*;q=0.8",
}

# Header columns from the published sheets. The 2024 sheet uses "PLAYER" while 2025/2026 use
# "Player"; we match case-insensitively.
_HEADER_RANK = "ovr"
_HEADER_PLAYER = "player"
_HEADER_POSITION = "position"
_HEADER_SCHOOL = "school"


class UnknownYearError(KeyError):
    """Raised when no Wide Left sheet id is configured for the requested draft year."""


def fetch_csv(year: int) -> str:
    """Fetch the raw CSV export for the given year's Wide Left consensus big board.

    Raises
    ------
    UnknownYearError
        If ``year`` is not present in :data:`constants.WL_SHEET_IDS`.

    """
    sheet_id = constants.WL_SHEET_IDS.get(year)
    if sheet_id is None:
        msg = f"No Wide Left sheet id configured for year {year}"
        raise UnknownYearError(msg)
    url = _GSHEETS_EXPORT_URL.format(sheet_id=sheet_id)
    response = requests.get(url, timeout=30, headers=_REQUEST_HEADERS)
    response.raise_for_status()
    log.info("Fetched WL CSV for year %s", year)
    return response.text


def _find_header_row(rows: list[list[str]]) -> int | None:
    """Return the index of the canonical header row, or ``None`` if not found.

    The canonical header begins with an ``Ovr`` cell. We search the first ten rows to skip the
    short metadata block at the top of the sheet without scanning the entire file.
    """
    for i, row in enumerate(rows[:10]):
        if row and row[0].strip().lower() == _HEADER_RANK:
            return i
    return None


def parse_big_board(csv_text: str) -> list[dict[str, str]]:
    """Parse the Wide Left CSV export into ``rank/name/pos/school`` records.

    Skips the metadata rows at the top of the sheet, locates the header row by its leading
    ``Ovr`` cell, and emits one record per data row whose Ovr value is a positive integer.
    """
    if not csv_text.strip():
        return []

    rows = list(csv.reader(io.StringIO(csv_text)))
    header_idx = _find_header_row(rows)
    if header_idx is None:
        log.warning("Wide Left CSV is missing the canonical header row")
        return []

    header = [cell.strip().lower() for cell in rows[header_idx]]
    try:
        rank_col = header.index(_HEADER_RANK)
        name_col = header.index(_HEADER_PLAYER)
        pos_col = header.index(_HEADER_POSITION)
        school_col = header.index(_HEADER_SCHOOL)
    except ValueError:
        log.warning("Wide Left CSV header missing expected columns: %s", header)
        return []

    out: list[dict[str, str]] = []
    for row in rows[header_idx + 1 :]:
        if len(row) <= max(rank_col, name_col, pos_col, school_col):
            continue
        rank = row[rank_col].strip()
        if not rank.isdigit():
            continue
        name = row[name_col].strip()
        if not name:
            continue
        out.append(
            {
                "rank": rank,
                "name": name,
                "pos": row[pos_col].strip(),
                "school": row[school_col].strip(),
            }
        )
    return out


def scrape_year(year: int) -> list[dict[str, str]]:
    """Scrape the Wide Left consensus big board for a single year and save to CSV."""
    csv_text = fetch_csv(year)
    recs = parse_big_board(csv_text)
    file_name = f"wl_big_board_{year}.csv"
    save_csv(file_name, recs)
    log.info("Saved %s records to %s", len(recs), file_name)
    return recs


def main() -> None:
    """Scrape the Wide Left consensus big board for each known year in the configured range."""
    for year in range(constants.START_YEAR, constants.END_YEAR + 1):
        if year not in constants.WL_SHEET_IDS:
            log.warning("Skipping year %s — no Wide Left sheet id configured", year)
            continue
        scrape_year(year)
        time.sleep(_rng.uniform(SLEEP_MIN, SLEEP_MAX))


if __name__ == "__main__":
    main()
