"""Tests for nfl_draft_scraper.mddb_bb_scraper — integration tests."""

from unittest.mock import patch

from nfl_draft_scraper.mddb_bb_scraper import fetch_html


class TestFetchHtml:
    """Tests for fetch_html."""

    @patch("nfl_draft_scraper.mddb_bb_scraper.requests.get")
    def test_returns_html(self, mock_get):
        """Verify returns html."""
        mock_get.return_value.text = "<html>test</html>"
        mock_get.return_value.raise_for_status = lambda: None
        result = fetch_html(2020)
        assert result == "<html>test</html>"
        mock_get.assert_called_once()

    @patch("nfl_draft_scraper.mddb_bb_scraper.requests.get")
    def test_calls_correct_url(self, mock_get):
        """Verify calls correct url."""
        mock_get.return_value.text = ""
        mock_get.return_value.raise_for_status = lambda: None
        fetch_html(2023)
        call_args = mock_get.call_args
        assert "2023" in call_args[0][0]
        assert "consensus-big-board-2023" in call_args[0][0]
