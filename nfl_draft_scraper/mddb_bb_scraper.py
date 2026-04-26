"""Mock Draft Database Big Board Scraper.

Fetches the consensus big board for each draft year by parsing the prospect-card markup rendered
on the public big-board page.
"""

from __future__ import annotations

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

# Each prospect card is a flat block of HTML containing a rank span, a player anchor, a position
# pill, and (usually) a college anchor. The site uses Tailwind utility classes; the patterns below
# anchor on the most-stable class fragments.
_PLAYER_ANCHOR_RE = re.compile(
    r'<a class="text-base sm:text-xl font-bold[^"]*"\s+href="/players/\d+/[^"]+">\s*'
    r"([^<\n]+?)\s*</a>",
    re.DOTALL,
)
_RANK_RE = re.compile(r"<span [^>]*font-black[^>]*>\s*(\d+)\s*</span>")
_POSITION_RE = re.compile(
    r'<span class="text-xs font-bold text-blue-700[^"]*">\s*([^<]+?)\s*</span>',
)
_COLLEGE_ANCHOR_RE = re.compile(
    r'<a class="text-sm text-gray-500[^"]*"\s+href="/colleges/\d+/[^"]+">\s*'
    r"([^<\n]+?)\s*</a>",
    re.DOTALL,
)

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
    """Verify the page belongs to the expected draft year.

    Raises
    ------
    ValueError
        If the canonical big-board path for ``expected_year`` is not present in the page.

    """
    marker = f"/big-boards/{expected_year}/consensus-big-board-{expected_year}"
    if marker not in page_html:
        msg = f"Year marker not found in page: expected {marker}"
        raise ValueError(msg)


def parse_big_board(page: str) -> list[dict[str, str]]:
    """Extract player data from the rendered MDDB consensus big-board page.

    Each prospect is represented by a card with a numeric rank, a player anchor, a position pill,
    and an optional college anchor. We locate every player anchor first and then slice the HTML
    between successive anchors to find the rank, position, and school for that prospect.
    """
    player_matches = list(_PLAYER_ANCHOR_RE.finditer(page))
    if not player_matches:
        log.warning("No prospect cards found in the page")
        return []

    out: list[dict[str, str]] = []
    for i, match in enumerate(player_matches):
        name = match.group(1).strip()

        prev_end = player_matches[i - 1].end() if i > 0 else 0
        before = page[prev_end : match.start()]
        rank_hits = _RANK_RE.findall(before)
        rank = rank_hits[-1] if rank_hits else ""

        next_start = player_matches[i + 1].start() if i + 1 < len(player_matches) else len(page)
        after = page[match.end() : next_start]

        pos_match = _POSITION_RE.search(after)
        pos = pos_match.group(1).strip() if pos_match else ""

        school_match = _COLLEGE_ANCHOR_RE.search(after)
        school = school_match.group(1).strip() if school_match else ""

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
