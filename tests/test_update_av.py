"""Tests for nfl_draft_scraper.scrape_av — _update_av."""

from unittest.mock import patch

import pandas as pd

from nfl_draft_scraper.scrape_av import _build_parser, _update_av


class TestUpdateAv:
    """Tests for _update_av."""

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

        _update_av(checkpoint_every=100)

        out = pd.read_csv(tmp_path / "cleaned_draft_picks_with_av.csv")
        assert "2024" in out.columns
        assert out.iloc[0]["2024"] == 5
        assert out.iloc[0]["career"] == 5

    @patch("nfl_draft_scraper.scrape_av._calculate_av")
    def test_handles_failure(self, mock_calc, tmp_path, monkeypatch):
        """Verify handles sportsipy failure gracefully."""
        monkeypatch.setattr("nfl_draft_scraper.scrape_av.constants.DATA_PATH", str(tmp_path))
        monkeypatch.setattr("nfl_draft_scraper.scrape_av.constants.START_YEAR", 2024)
        monkeypatch.setattr("nfl_draft_scraper.scrape_av.constants.END_YEAR", 2024)

        self._setup_draft_csv(tmp_path, [2024])
        mock_calc.side_effect = ValueError("fail")

        _update_av(checkpoint_every=100)

        out = pd.read_csv(tmp_path / "cleaned_draft_picks_with_av.csv")
        assert pd.isna(out.iloc[0]["career"])

    @patch("nfl_draft_scraper.scrape_av._calculate_av")
    def test_processes_all_seasons(self, mock_calc, tmp_path, monkeypatch):
        """Verify players from all seasons are processed, not just the latest."""
        monkeypatch.setattr("nfl_draft_scraper.scrape_av.constants.DATA_PATH", str(tmp_path))
        monkeypatch.setattr("nfl_draft_scraper.scrape_av.constants.START_YEAR", 2020)
        monkeypatch.setattr("nfl_draft_scraper.scrape_av.constants.END_YEAR", 2021)

        self._setup_draft_csv(tmp_path, [2020])
        mock_calc.return_value = ({"2020": 7, "2021": 3}, 10, 9.9)

        _update_av(checkpoint_every=100)

        out = pd.read_csv(tmp_path / "cleaned_draft_picks_with_av.csv")
        assert out.iloc[0]["career"] == 10

    @patch("nfl_draft_scraper.scrape_av._calculate_av")
    def test_force_rescrapes_complete_rows(self, mock_calc, tmp_path, monkeypatch):
        """Verify force=True re-scrapes rows that are already marked av_complete."""
        monkeypatch.setattr("nfl_draft_scraper.scrape_av.constants.DATA_PATH", str(tmp_path))
        monkeypatch.setattr("nfl_draft_scraper.scrape_av.constants.START_YEAR", 2024)
        monkeypatch.setattr("nfl_draft_scraper.scrape_av.constants.END_YEAR", 2024)

        # Create a checkpoint with one already-complete player
        df = pd.DataFrame(
            {
                "pfr_player_id": ["Pid0001"],
                "pfr_player_name": ["Player 0"],
                "category": ["QB"],
                "season": [2024],
                "2024": [5],
                "career": [5],
                "weighted_career": [5.0],
                "av_complete": [True],
            }
        )
        df.to_csv(tmp_path / "cleaned_draft_picks_with_av_checkpoint.csv", index=False)
        # Also need the source CSV for _initialize_draft_picks_df
        self._setup_draft_csv(tmp_path, [2024])

        # Return updated values
        mock_calc.return_value = ({"2024": 9}, 9, 9.0)

        _update_av(force=True, checkpoint_every=100)

        out = pd.read_csv(tmp_path / "cleaned_draft_picks_with_av.csv")
        assert out.iloc[0]["2024"] == 9
        assert out.iloc[0]["career"] == 9

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

        _update_av(checkpoint_every=100)

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

        _update_av(checkpoint_every=2)

        checkpoint = tmp_path / "cleaned_draft_picks_with_av_checkpoint.csv"
        assert checkpoint.exists()


class TestBuildParser:
    """Tests for _build_parser."""

    def test_defaults(self):
        """Verify default argument values."""
        parser = _build_parser()
        args = parser.parse_args([])
        assert args.force is False
        assert args.checkpoint_every == 20

    def test_force_flag(self):
        """Verify --force sets force to True."""
        parser = _build_parser()
        args = parser.parse_args(["--force"])
        assert args.force is True

    def test_checkpoint_every_flag(self):
        """Verify --checkpoint-every sets value."""
        parser = _build_parser()
        args = parser.parse_args(["--checkpoint-every", "50"])
        assert args.checkpoint_every == 50
