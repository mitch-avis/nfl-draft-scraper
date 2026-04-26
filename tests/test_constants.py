"""Tests for nfl_draft_scraper.constants."""

import datetime
from pathlib import Path
from unittest.mock import patch

from nfl_draft_scraper import constants
from nfl_draft_scraper.constants import (
    TEAM_ABBREVIATION_MAP,
    _most_recent_completed_season,
    normalize_team,
)


class TestConstants:
    """Test project constants."""

    def test_root_dir_is_path(self):
        """Verify root dir is path."""
        assert isinstance(constants.ROOT_DIR, Path)

    def test_data_path_under_root(self):
        """Verify data path under root."""
        assert str(constants.DATA_PATH).startswith(str(constants.ROOT_DIR))

    def test_year_range_valid(self):
        """Verify year range valid."""
        assert constants.START_YEAR <= constants.END_YEAR
        assert constants.START_YEAR >= 2000
        assert constants.END_YEAR <= 2100

    def test_urls_are_https(self):
        """Verify urls are https."""
        assert constants.JLBB_BASE_URL.startswith("https://")

    def test_wl_sheet_ids_present(self):
        """Verify Wide Left sheet ids exist for the supported years."""
        assert {2024, 2025, 2026} <= set(constants.WL_SHEET_IDS)
        for sheet_id in constants.WL_SHEET_IDS.values():
            assert sheet_id and isinstance(sheet_id, str)


class TestTeamAbbreviationMap:
    """Tests for TEAM_ABBREVIATION_MAP."""

    def test_oak_maps_to_lvr(self):
        """Verify Oakland Raiders abbreviation maps to Las Vegas Raiders."""
        assert TEAM_ABBREVIATION_MAP["OAK"] == "LVR"

    def test_sdg_maps_to_lac(self):
        """Verify San Diego Chargers abbreviation maps to LA Chargers."""
        assert TEAM_ABBREVIATION_MAP["SDG"] == "LAC"

    def test_stl_maps_to_lar(self):
        """Verify St. Louis Rams abbreviation maps to LA Rams."""
        assert TEAM_ABBREVIATION_MAP["STL"] == "LAR"

    def test_current_abbreviation_not_in_map(self):
        """Verify current team abbreviations are not keys in the map."""
        assert "LVR" not in TEAM_ABBREVIATION_MAP
        assert "LAC" not in TEAM_ABBREVIATION_MAP
        assert "LAR" not in TEAM_ABBREVIATION_MAP
        assert "DAL" not in TEAM_ABBREVIATION_MAP


class TestNormalizeTeam:
    """Tests for normalize_team."""

    def test_historical_oak_becomes_lvr(self):
        """Verify OAK is normalized to LVR."""
        assert normalize_team("OAK") == "LVR"

    def test_historical_sdg_becomes_lac(self):
        """Verify SDG is normalized to LAC."""
        assert normalize_team("SDG") == "LAC"

    def test_historical_stl_becomes_lar(self):
        """Verify STL is normalized to LAR."""
        assert normalize_team("STL") == "LAR"

    def test_current_abbreviation_unchanged(self):
        """Verify current abbreviations pass through unchanged."""
        assert normalize_team("DAL") == "DAL"
        assert normalize_team("LVR") == "LVR"
        assert normalize_team("LAC") == "LAC"
        assert normalize_team("LAR") == "LAR"

    def test_strips_whitespace(self):
        """Verify leading/trailing whitespace is stripped."""
        assert normalize_team("  OAK  ") == "LVR"
        assert normalize_team(" DAL ") == "DAL"


class TestMostRecentCompletedSeason:
    """Tests for _most_recent_completed_season."""

    def test_january_returns_prior_year(self):
        """In January, the prior calendar year is the most recent completed season."""
        with patch("nfl_draft_scraper.constants.datetime") as mock_dt:
            mock_dt.date.today.return_value = datetime.date(2025, 1, 15)
            assert _most_recent_completed_season() == 2024

    def test_february_returns_prior_year(self):
        """In February, the prior calendar year is still the most recent completed season."""
        with patch("nfl_draft_scraper.constants.datetime") as mock_dt:
            mock_dt.date.today.return_value = datetime.date(2025, 2, 28)
            assert _most_recent_completed_season() == 2024

    def test_march_returns_current_year(self):
        """From March onward, the current calendar year is the most recent completed season."""
        with patch("nfl_draft_scraper.constants.datetime") as mock_dt:
            mock_dt.date.today.return_value = datetime.date(2025, 3, 1)
            assert _most_recent_completed_season() == 2025

    def test_october_returns_current_year(self):
        """In October the current year is still the most recent completed season."""
        with patch("nfl_draft_scraper.constants.datetime") as mock_dt:
            mock_dt.date.today.return_value = datetime.date(2025, 10, 1)
            assert _most_recent_completed_season() == 2025
