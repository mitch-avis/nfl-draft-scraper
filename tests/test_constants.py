"""Tests for nfl_draft_scraper.constants."""

from pathlib import Path

from nfl_draft_scraper import constants


class TestConstants:
    """Test project constants."""

    def test_root_dir_is_path(self):
        """Verify root dir is path."""
        assert isinstance(constants.ROOT_DIR, Path)

    def test_data_path_under_root(self):
        """Verify data path under root."""
        assert constants.DATA_PATH.startswith(str(constants.ROOT_DIR))

    def test_year_range_valid(self):
        """Verify year range valid."""
        assert constants.START_YEAR <= constants.END_YEAR
        assert constants.START_YEAR >= 2000
        assert constants.END_YEAR <= 2100

    def test_urls_are_https(self):
        """Verify urls are https."""
        assert constants.PFF_BASE_URL.startswith("https://")
        assert constants.MOCK_DRAFT_DB_BASE_URL.startswith("https://")
        assert constants.JLBB_BASE_URL.startswith("https://")
