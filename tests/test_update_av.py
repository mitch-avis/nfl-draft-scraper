"""Tests for nfl_draft_scraper.scrape_av — _update_av_with_fallback."""

from unittest.mock import patch

import pandas as pd

from nfl_draft_scraper.scrape_av import _update_av_with_fallback


class TestUpdateAvWithFallback:
    """Tests for _update_av_with_fallback."""

    def _setup_draft_csv(self, tmp_path, seasons=None, av_complete=None):
        """Create a minimal cleaned_draft_picks.csv."""
        if seasons is None:
            seasons = [2024]
        rows = []
        for i, s in enumerate(seasons):
            rows.append(
                {
                    "pfr_player_id": f"Pid{i:02d}01",
                    "pfr_player_name": f"Player {i}",
                    "category": "QB",
                    "season": s,
                }
            )
        df = pd.DataFrame(rows)
        df.to_csv(tmp_path / "cleaned_draft_picks.csv", index=True)
        return df

    @patch("nfl_draft_scraper.scrape_av._calculate_av")
    def test_sportsipy_success(self, mock_calc, tmp_path, monkeypatch):
        """Verify sportsipy success."""
        monkeypatch.setattr("nfl_draft_scraper.scrape_av.constants.DATA_PATH", str(tmp_path))
        monkeypatch.setattr("nfl_draft_scraper.scrape_av.constants.START_YEAR", 2024)
        monkeypatch.setattr("nfl_draft_scraper.scrape_av.constants.END_YEAR", 2024)

        self._setup_draft_csv(tmp_path, [2024])
        mock_calc.return_value = ({"2024": 5}, 5, 5.0)

        _update_av_with_fallback(checkpoint_every=100)

        out = pd.read_csv(tmp_path / "cleaned_draft_picks_with_av.csv")
        assert "2024" in out.columns
        assert out.iloc[0]["2024"] == 5
        assert out.iloc[0]["career"] == 5

    @patch("nfl_draft_scraper.scrape_av._scrape_player_av_fallback")
    @patch("nfl_draft_scraper.scrape_av._calculate_av")
    def test_falls_back_to_pfr(self, mock_calc, mock_fallback, tmp_path, monkeypatch):
        """Verify falls back to pfr."""
        monkeypatch.setattr("nfl_draft_scraper.scrape_av.constants.DATA_PATH", str(tmp_path))
        monkeypatch.setattr("nfl_draft_scraper.scrape_av.constants.START_YEAR", 2024)
        monkeypatch.setattr("nfl_draft_scraper.scrape_av.constants.END_YEAR", 2024)

        self._setup_draft_csv(tmp_path, [2024])
        mock_calc.side_effect = ValueError("sportsipy error")
        mock_fallback.return_value = ({"2024": 3}, 3, 3.0)

        _update_av_with_fallback(checkpoint_every=100)

        out = pd.read_csv(tmp_path / "cleaned_draft_picks_with_av.csv")
        assert out.iloc[0]["2024"] == 3

    @patch("nfl_draft_scraper.scrape_av._scrape_player_av_fallback")
    @patch("nfl_draft_scraper.scrape_av._calculate_av")
    def test_handles_both_failures(self, mock_calc, mock_fallback, tmp_path, monkeypatch):
        """Verify handles both failures."""
        monkeypatch.setattr("nfl_draft_scraper.scrape_av.constants.DATA_PATH", str(tmp_path))
        monkeypatch.setattr("nfl_draft_scraper.scrape_av.constants.START_YEAR", 2024)
        monkeypatch.setattr("nfl_draft_scraper.scrape_av.constants.END_YEAR", 2024)

        self._setup_draft_csv(tmp_path, [2024])
        mock_calc.side_effect = ValueError("fail")
        mock_fallback.side_effect = ValueError("fail too")

        _update_av_with_fallback(checkpoint_every=100)

        out = pd.read_csv(tmp_path / "cleaned_draft_picks_with_av.csv")
        assert pd.isna(out.iloc[0]["career"])

    def test_skips_old_seasons(self, tmp_path, monkeypatch):
        """Verify skips old seasons."""
        monkeypatch.setattr("nfl_draft_scraper.scrape_av.constants.DATA_PATH", str(tmp_path))
        monkeypatch.setattr("nfl_draft_scraper.scrape_av.constants.START_YEAR", 2020)
        monkeypatch.setattr("nfl_draft_scraper.scrape_av.constants.END_YEAR", 2020)

        self._setup_draft_csv(tmp_path, [2020])

        _update_av_with_fallback(checkpoint_every=100)

        out = pd.read_csv(tmp_path / "cleaned_draft_picks_with_av.csv")
        # Old season player should be skipped, AV columns remain NaN
        assert pd.isna(out.iloc[0]["career"])

    def test_skips_missing_player_id(self, tmp_path, monkeypatch):
        """Verify skips missing player id."""
        monkeypatch.setattr("nfl_draft_scraper.scrape_av.constants.DATA_PATH", str(tmp_path))
        monkeypatch.setattr("nfl_draft_scraper.scrape_av.constants.START_YEAR", 2024)
        monkeypatch.setattr("nfl_draft_scraper.scrape_av.constants.END_YEAR", 2024)

        df = pd.DataFrame(
            {
                "pfr_player_id": [""],
                "pfr_player_name": ["No ID"],
                "category": ["QB"],
                "season": [2024],
            }
        )
        df.to_csv(tmp_path / "cleaned_draft_picks.csv", index=True)

        _update_av_with_fallback(checkpoint_every=100)

        out = pd.read_csv(tmp_path / "cleaned_draft_picks_with_av.csv")
        assert pd.isna(out.iloc[0]["career"])

    @patch("nfl_draft_scraper.scrape_av._calculate_av")
    def test_checkpoints_regularly(self, mock_calc, tmp_path, monkeypatch):
        """Verify checkpoints regularly."""
        monkeypatch.setattr("nfl_draft_scraper.scrape_av.constants.DATA_PATH", str(tmp_path))
        monkeypatch.setattr("nfl_draft_scraper.scrape_av.constants.START_YEAR", 2024)
        monkeypatch.setattr("nfl_draft_scraper.scrape_av.constants.END_YEAR", 2024)

        # Create 3 players to trigger checkpoint at every=2
        df = pd.DataFrame(
            {
                "pfr_player_id": ["Pid001", "Pid002", "Pid003"],
                "pfr_player_name": ["P1", "P2", "P3"],
                "category": ["QB", "QB", "QB"],
                "season": [2024, 2024, 2024],
            }
        )
        df.to_csv(tmp_path / "cleaned_draft_picks.csv", index=True)
        mock_calc.return_value = ({"2024": 1}, 1, 1.0)

        _update_av_with_fallback(checkpoint_every=2)

        checkpoint = tmp_path / "cleaned_draft_picks_with_av_checkpoint.csv"
        assert checkpoint.exists()
