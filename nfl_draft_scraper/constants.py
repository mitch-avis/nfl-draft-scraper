"""Constants for the NFL Draft Scraper project."""

import os
from pathlib import Path

# Project directory configurations
ROOT_DIR = Path(__file__).parent.parent
DATA_PATH = os.path.join(ROOT_DIR, "data")

START_YEAR = 2018
END_YEAR = 2025

PFF_BASE_URL = "https://www.pff.com/draft/big-board"
MOCK_DRAFT_DB_BASE_URL = "https://www.nflmockdraftdatabase.com/big-boards"
JLBB_BASE_URL = "https://jacklich10.com/bigboard/nfl"
