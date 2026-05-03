"""Constants for the NFL Draft Scraper project."""

from __future__ import annotations

import datetime
from pathlib import Path

# Project directory configurations
ROOT_DIR = Path(__file__).parent.parent
DATA_PATH = ROOT_DIR / "data"

START_YEAR = 2016


def _most_recent_completed_season() -> int:
    """Compute the most recent completed NFL season.

    The NFL regular season ends in January, and draft analysis typically uses data through the prior
    season. If today is before September, the most recent completed season is the prior calendar
    year. Otherwise it's the current calendar year.
    """
    today = datetime.date.today()
    # NFL season starts in September and runs through the following February. After Week 18 +
    # playoffs + Super Bowl (~mid-Feb), the season is "complete." The draft is in late April. We use
    # September as the cutoff for the *next* season.
    if today.month < 3:
        return today.year - 1
    return today.year


END_YEAR: int = _most_recent_completed_season()

JLBB_BASE_URL = "https://jacklich10.com/bigboard/nfl/"

# Google Sheet ids for Arif Hasan's Wide Left consensus big boards. Sheets are public; the scraper
# downloads each one as CSV via the standard gviz export URL.
WL_SHEET_IDS: dict[int, str] = {
    2024: "1u_7bYeFLyPGldL6OqvyEXICH4LCvNZb_iqc2Z3740mo",
    2025: "1IUxTL9PXAmkasscUiGVYovtdKo7tawIuzXdMfDzmqI4",
    2026: "1kMMdFfdPhcIlmSRFWnKVzx3IGm5VODx5oqJRraGms5E",
}

# Map historical team abbreviations to their current equivalents. Only relocated/rebranded
# franchises need entries here.
TEAM_ABBREVIATION_MAP: dict[str, str] = {
    "OAK": "LVR",  # Oakland Raiders → Las Vegas Raiders (2020)
    "SDG": "LAC",  # San Diego Chargers → Los Angeles Chargers (2017)
    "STL": "LAR",  # St. Louis Rams → Los Angeles Rams (2016)
}


def normalize_team(abbreviation: str) -> str:
    """Normalize a team abbreviation to its current value.

    Maps historical abbreviations (e.g. OAK, SDG, STL) to the franchise's current abbreviation.
    Abbreviations not in the map pass through unchanged.
    """
    return TEAM_ABBREVIATION_MAP.get(abbreviation.strip(), abbreviation.strip())
