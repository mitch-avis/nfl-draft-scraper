"""Mock Draft Database Big Board Scraper."""

import random
import time

import requests
from lxml import html

from nfl_draft_scraper import constants
from nfl_draft_scraper.utils.csv_utils import save_csv
from nfl_draft_scraper.utils.logger import log

SLEEP_MIN = 1
SLEEP_MAX = 3
_rng = random.SystemRandom()


def fetch_html(year: int) -> str:
    """Fetch the HTML page for the consensus big board for a given year."""
    url = f"{constants.MOCK_DRAFT_DB_BASE_URL}/{year}/consensus-big-board-{year}"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    log.info("Fetched HTML for year %s", year)
    return response.text


def parse_big_board(page: str) -> list:
    """Parse the HTML page and extract player data."""
    tree = html.fromstring(page)
    big_board_rows = tree.xpath("//li[contains(@class,'mock-list-item')]")
    out = []
    for row in big_board_rows:
        # rank
        rank_path = row.xpath(".//div[contains(@class,'pick-number')]/text()")
        if not rank_path:
            continue
        rank = rank_path[0].strip()
        # player
        name_path = row.xpath(".//div[contains(@class,'player-name')]/text()")
        name = name_path[0].strip()
        # position
        pos_path = row.xpath(".//div[contains(@class,'player-details')]/text()")
        pos = pos_path[0].split("|")[0].strip() if pos_path else ""
        # school
        school_path = row.xpath(".//div[contains(@class,'player-details')]/a/text()")
        school = school_path[0].strip()
        out.append({"rank": rank, "name": name, "pos": pos, "school": school})
        log.debug("Parsed row: rank=%s, name=%s, pos=%s, school=%s", rank, name, pos, school)
    return out


def main():
    """Scrape the consensus big board for each year."""
    for year in range(constants.START_YEAR, constants.END_YEAR + 1):
        mddb_html = fetch_html(year)
        recs = parse_big_board(mddb_html)
        file_name = f"mddb_big_board_{year}.csv"
        save_csv(file_name, recs)
        log.info("Saved %s records to %s", len(recs), file_name)
        time.sleep(_rng.uniform(SLEEP_MIN, SLEEP_MAX))


if __name__ == "__main__":
    main()
