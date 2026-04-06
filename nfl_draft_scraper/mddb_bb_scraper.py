"""Mock Draft Database Big Board Scraper.

Fetches the consensus big board for each draft year by extracting the JSON
data embedded in the page's React component props, avoiding brittle HTML/XPath
parsing entirely.
"""

from __future__ import annotations

import html
import json
import random
import re
import time

import requests

from nfl_draft_scraper import constants
from nfl_draft_scraper.utils.csv_utils import save_csv
from nfl_draft_scraper.utils.logger import log

SLEEP_MIN = 1
SLEEP_MAX = 3
_rng = random.SystemRandom()

_REACT_PROPS_RE = re.compile(r'data-react-props="([^"]+)"')

_REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:128.0) Gecko/20100101 Firefox/128.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}


def fetch_html(year: int) -> str:
    """Fetch the HTML page for the consensus big board for a given year."""
    url = f"{constants.MOCK_DRAFT_DB_BASE_URL}/{year}/consensus-big-board-{year}"
    response = requests.get(url, timeout=30, headers=_REQUEST_HEADERS)
    response.raise_for_status()
    log.info("Fetched HTML for year %s", year)
    return response.text


def _verify_year(page_html: str, expected_year: int) -> None:
    """Verify the React props year matches the expected year.

    Raises
    ------
    ValueError
        If no React props are found or the year does not match.

    """
    match = _REACT_PROPS_RE.search(page_html)
    if not match:
        msg = "No React props found in the page"
        raise ValueError(msg)
    props = json.loads(html.unescape(match.group(1)))
    props_year = str(props.get("year", ""))
    log.debug("React props year: %s (expected %d)", props_year, expected_year)
    if props_year != str(expected_year):
        msg = f"Year mismatch: expected {expected_year}, got {props_year}"
        raise ValueError(msg)


def parse_big_board(page: str) -> list[dict[str, str]]:
    """Extract player data from the React props JSON embedded in the HTML.

    The MDDB page ships all big-board data inside a ``data-react-props`` attribute
    on a ``<div>`` element. The JSON structure contains a ``mock.selections`` list
    where each entry has ``pick``, ``player.name``, ``player.position``, and
    ``player.college.name``.
    """
    match = _REACT_PROPS_RE.search(page)
    if not match:
        log.warning("No React props found in the page")
        return []

    props = json.loads(html.unescape(match.group(1)))
    selections = props.get("mock", {}).get("selections", [])

    out: list[dict[str, str]] = []
    for sel in selections:
        rank = str(sel.get("pick", ""))
        player = sel.get("player", {})
        name = player.get("name", "").strip()
        pos = player.get("position", "").strip()
        college_obj = player.get("college") or {}
        school = college_obj.get("name", "").strip()

        if not rank or not name:
            continue

        out.append({"rank": rank, "name": name, "pos": pos, "school": school})
        log.debug("Parsed row: rank=%s, name=%s, pos=%s, school=%s", rank, name, pos, school)

    return out


def scrape_year(year: int) -> list[dict[str, str]]:
    """Scrape the consensus big board for a single year and save to CSV."""
    mddb_html = fetch_html(year)
    _verify_year(mddb_html, year)
    recs = parse_big_board(mddb_html)
    file_name = f"mddb_big_board_{year}.csv"
    save_csv(file_name, recs)
    log.info("Saved %s records to %s", len(recs), file_name)
    return recs


def main() -> None:
    """Scrape the consensus big board for each year."""
    for year in range(constants.START_YEAR, constants.END_YEAR + 1):
        scrape_year(year)
        time.sleep(_rng.uniform(SLEEP_MIN, SLEEP_MAX))


if __name__ == "__main__":
    main()
