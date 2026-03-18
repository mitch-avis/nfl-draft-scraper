"""Tests for nfl_draft_scraper.scrape_av — integration-level tests."""

import os
from unittest.mock import MagicMock, patch

import pandas as pd

from nfl_draft_scraper.scrape_av import (
    _calculate_weighted_from_pfr,
    _initialize_draft_picks_df,
    _save_checkpoint,
    _scrape_player_av_fallback,
)


class TestInitializeDraftPicksDf:
    """Tests for _initialize_draft_picks_df."""

    def test_loads_from_checkpoint(self, tmp_path):
        """Verify loads from checkpoint."""
        checkpoint = tmp_path / "checkpoint.csv"
        df = pd.DataFrame(
            {
                "pfr_player_id": ["id1"],
                "2020": [5],
                "career": [5],
                "weighted_career": [5.0],
                "av_complete": [True],
            }
        )
        df.to_csv(checkpoint, index=False)

        result = _initialize_draft_picks_df(str(tmp_path / "draft.csv"), str(checkpoint), ["2020"])
        assert len(result) == 1
        assert result.iloc[0]["av_complete"]

    def test_initializes_from_draft(self, tmp_path):
        """Verify initializes from draft."""
        draft = tmp_path / "draft.csv"
        df = pd.DataFrame({"pfr_player_id": ["id1"], "pfr_player_name": ["Player"]})
        df.to_csv(draft, index=True)

        result = _initialize_draft_picks_df(str(draft), str(tmp_path / "nonexistent.csv"), ["2020"])
        assert "2020" in result.columns
        assert "career" in result.columns
        assert "weighted_career" in result.columns
        assert "av_complete" in result.columns
        assert not result.iloc[0]["av_complete"]

    def test_adds_av_complete_if_missing(self, tmp_path):
        """Verify adds av complete if missing."""
        checkpoint = tmp_path / "checkpoint.csv"
        df = pd.DataFrame({"pfr_player_id": ["id1"], "2020": [5]})
        df.to_csv(checkpoint, index=False)

        result = _initialize_draft_picks_df(str(tmp_path / "draft.csv"), str(checkpoint), ["2020"])
        assert "av_complete" in result.columns


class TestSaveCheckpoint:
    """Tests for _save_checkpoint."""

    def test_saves_csv(self, tmp_path):
        """Verify saves csv."""
        df = pd.DataFrame({"a": [1], "av_complete": [True]})
        path = str(tmp_path / "checkpoint.csv")
        _save_checkpoint(df, path)
        assert os.path.exists(path)
        result = pd.read_csv(path)
        assert len(result) == 1


class TestCalculateWeightedFromPfr:
    """Tests for _calculate_weighted_from_pfr."""

    def test_same_as_sportsipy_version(self):
        """Verify same as sportsipy version."""
        av_by_year = {"2020": 10, "2021": 8, "2022": 6}
        result = _calculate_weighted_from_pfr(av_by_year, [2020, 2021, 2022])
        # sorted: [10, 8, 6], weights: [1.0, 0.95, 0.9]
        expected = round(10 * 1.0 + 8 * 0.95 + 6 * 0.9, 1)
        assert result == expected


class TestScrapePlayerAvFallback:
    """Tests for _scrape_player_av_fallback."""

    @patch("nfl_draft_scraper.scrape_av.requests.get")
    def test_returns_none_on_bad_status(self, mock_get):
        """Verify returns none on bad status."""
        mock_resp = MagicMock()
        mock_resp.status_code = 404
        mock_get.return_value = mock_resp

        result = _scrape_player_av_fallback("AbcdXy01", "Test Player", "QB", [2020])
        assert result == (None, None, None)

    @patch("nfl_draft_scraper.scrape_av.requests.get")
    def test_returns_none_on_missing_table(self, mock_get):
        """Verify returns none on missing table."""
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.text = "<html><body><p>No tables here</p></body></html>"
        mock_get.return_value = mock_resp

        result = _scrape_player_av_fallback("AbcdXy01", "Test Player", "QB", [2020])
        assert result == (None, None, None)

    @patch("nfl_draft_scraper.scrape_av.requests.get")
    @patch("nfl_draft_scraper.scrape_av.time.sleep")
    def test_parses_valid_page(self, mock_sleep, mock_get):
        """Verify parses valid page."""
        html = """
        <html><body>
        <table id="passing">
            <tbody>
                <tr>
                    <th data-stat="year_id">2020</th>
                    <td data-stat="av">7</td>
                </tr>
            </tbody>
            <tfoot>
                <tr>
                    <td data-stat="av">7</td>
                </tr>
            </tfoot>
        </table>
        </body></html>
        """
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.text = html
        mock_get.return_value = mock_resp

        av_by_year, career, weighted = _scrape_player_av_fallback(
            "AbcdXy01", "Test Player", "QB", [2020]
        )
        assert av_by_year == {"2020": 7}
        assert career == 7
        assert weighted == 7.0
