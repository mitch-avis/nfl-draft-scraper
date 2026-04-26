"""Tests for nfl_draft_scraper.scrape_av — update_av."""

from unittest.mock import patch

import polars as pl

from nfl_draft_scraper.scrape_av import _build_parser, update_av
from nfl_draft_scraper.utils.csv_utils import write_df_to_csv


class TestUpdateAv:
    """Tests for update_av."""

    def _setup_draft_csv(self, tmp_path, seasons=None, teams=None):
        """Create a minimal cleaned_draft_picks.csv on disk."""
        if seasons is None:
            seasons = [2024]
        if teams is None:
            teams = ["TST"] * len(seasons)
        rows = [
            {
                "pfr_player_id": f"Pid{i:02d}01",
                "pfr_player_name": f"Player {i}",
                "category": "QB",
                "season": s,
                "team": teams[i],
            }
            for i, s in enumerate(seasons)
        ]
        df = pl.DataFrame(rows)
        write_df_to_csv(df, tmp_path / "cleaned_draft_picks.csv", index=True)
        return df

    @patch("nfl_draft_scraper.scrape_av._calculate_av")
    def test_sportsipy_success(self, mock_calc, tmp_path, monkeypatch):
        """Verify sportsipy success."""
        monkeypatch.setattr("nfl_draft_scraper.scrape_av.constants.DATA_PATH", tmp_path)
        monkeypatch.setattr("nfl_draft_scraper.scrape_av.constants.START_YEAR", 2024)
        monkeypatch.setattr("nfl_draft_scraper.scrape_av.constants.END_YEAR", 2024)

        self._setup_draft_csv(tmp_path, [2024], teams=["TST"])
        mock_calc.return_value = ({"2024": 5}, 5, 5.0, 5, 5.0)

        update_av(checkpoint_every=100)

        out = pl.read_csv(tmp_path / "cleaned_draft_picks_with_av.csv")
        assert "2024" in out.columns
        row = out.row(0, named=True)
        assert row["2024"] == 5
        assert row["career"] == 5
        assert row["draft_team_career"] == 5
        assert row["draft_team_weighted_career"] == 5.0

    @patch("nfl_draft_scraper.scrape_av._calculate_av")
    def test_handles_failure(self, mock_calc, tmp_path, monkeypatch):
        """Verify handles sportsipy failure gracefully."""
        monkeypatch.setattr("nfl_draft_scraper.scrape_av.constants.DATA_PATH", tmp_path)
        monkeypatch.setattr("nfl_draft_scraper.scrape_av.constants.START_YEAR", 2024)
        monkeypatch.setattr("nfl_draft_scraper.scrape_av.constants.END_YEAR", 2024)

        self._setup_draft_csv(tmp_path, [2024], teams=["TST"])
        mock_calc.side_effect = ValueError("fail")

        update_av(checkpoint_every=100)

        out = pl.read_csv(tmp_path / "cleaned_draft_picks_with_av.csv")
        row = out.row(0, named=True)
        assert row["career"] is None
        assert row["draft_team_career"] is None
        assert row["draft_team_weighted_career"] is None

    @patch("nfl_draft_scraper.scrape_av._calculate_av")
    def test_processes_all_seasons(self, mock_calc, tmp_path, monkeypatch):
        """Verify players from all seasons are processed, not just the latest."""
        monkeypatch.setattr("nfl_draft_scraper.scrape_av.constants.DATA_PATH", tmp_path)
        monkeypatch.setattr("nfl_draft_scraper.scrape_av.constants.START_YEAR", 2020)
        monkeypatch.setattr("nfl_draft_scraper.scrape_av.constants.END_YEAR", 2021)

        self._setup_draft_csv(tmp_path, [2020], teams=["DAL"])
        mock_calc.return_value = ({"2020": 7, "2021": 3}, 10, 9.9, 10, 9.9)

        update_av(checkpoint_every=100)

        out = pl.read_csv(tmp_path / "cleaned_draft_picks_with_av.csv")
        row = out.row(0, named=True)
        assert row["career"] == 10
        assert row["draft_team_career"] == 10

    @patch("nfl_draft_scraper.scrape_av._calculate_av")
    def test_force_rescrapes_complete_rows(self, mock_calc, tmp_path, monkeypatch):
        """Verify force=True re-scrapes rows that are already marked av_complete."""
        monkeypatch.setattr("nfl_draft_scraper.scrape_av.constants.DATA_PATH", tmp_path)
        monkeypatch.setattr("nfl_draft_scraper.scrape_av.constants.START_YEAR", 2024)
        monkeypatch.setattr("nfl_draft_scraper.scrape_av.constants.END_YEAR", 2024)

        df = pl.DataFrame(
            {
                "pfr_player_id": ["Pid0001"],
                "pfr_player_name": ["Player 0"],
                "category": ["QB"],
                "season": [2024],
                "team": ["TST"],
                "2024": [5],
                "career": [5],
                "weighted_career": [5.0],
                "draft_team_career": [5],
                "draft_team_weighted_career": [5.0],
                "av_complete": [True],
            }
        )
        df.write_csv(tmp_path / "cleaned_draft_picks_with_av_checkpoint.csv")
        self._setup_draft_csv(tmp_path, [2024], teams=["TST"])

        mock_calc.return_value = ({"2024": 9}, 9, 9.0, 9, 9.0)

        update_av(force=True, checkpoint_every=100)

        out = pl.read_csv(tmp_path / "cleaned_draft_picks_with_av.csv")
        row = out.row(0, named=True)
        assert row["2024"] == 9
        assert row["career"] == 9
        assert row["draft_team_career"] == 9

    def test_skips_missing_player_id(self, tmp_path, monkeypatch):
        """Verify skips missing player id."""
        monkeypatch.setattr("nfl_draft_scraper.scrape_av.constants.DATA_PATH", tmp_path)
        monkeypatch.setattr("nfl_draft_scraper.scrape_av.constants.START_YEAR", 2024)
        monkeypatch.setattr("nfl_draft_scraper.scrape_av.constants.END_YEAR", 2024)

        df = pl.DataFrame(
            {
                "pfr_player_id": [""],
                "pfr_player_name": ["No ID"],
                "category": ["QB"],
                "season": [2024],
                "team": ["TST"],
            }
        )
        write_df_to_csv(df, tmp_path / "cleaned_draft_picks.csv", index=True)

        update_av(checkpoint_every=100)

        out = pl.read_csv(tmp_path / "cleaned_draft_picks_with_av.csv")
        row = out.row(0, named=True)
        assert row["career"] is None
        assert row["draft_team_career"] is None

    @patch("nfl_draft_scraper.scrape_av._calculate_av")
    def test_checkpoints_regularly(self, mock_calc, tmp_path, monkeypatch):
        """Verify checkpoints regularly."""
        monkeypatch.setattr("nfl_draft_scraper.scrape_av.constants.DATA_PATH", tmp_path)
        monkeypatch.setattr("nfl_draft_scraper.scrape_av.constants.START_YEAR", 2024)
        monkeypatch.setattr("nfl_draft_scraper.scrape_av.constants.END_YEAR", 2024)

        df = pl.DataFrame(
            {
                "pfr_player_id": ["Pid001", "Pid002", "Pid003"],
                "pfr_player_name": ["P1", "P2", "P3"],
                "category": ["QB", "QB", "QB"],
                "season": [2024, 2024, 2024],
                "team": ["TST", "TST", "TST"],
            }
        )
        write_df_to_csv(df, tmp_path / "cleaned_draft_picks.csv", index=True)
        mock_calc.return_value = ({"2024": 1}, 1, 1.0, 1, 1.0)

        update_av(checkpoint_every=2)

        checkpoint = tmp_path / "cleaned_draft_picks_with_av_checkpoint.csv"
        assert checkpoint.exists()

    @patch("nfl_draft_scraper.scrape_av._calculate_av")
    def test_draft_team_av_differs_from_total(self, mock_calc, tmp_path, monkeypatch):
        """Verify draft_team_career can be less than career when player was traded."""
        monkeypatch.setattr("nfl_draft_scraper.scrape_av.constants.DATA_PATH", tmp_path)
        monkeypatch.setattr("nfl_draft_scraper.scrape_av.constants.START_YEAR", 2020)
        monkeypatch.setattr("nfl_draft_scraper.scrape_av.constants.END_YEAR", 2021)

        self._setup_draft_csv(tmp_path, [2020], teams=["DAL"])
        mock_calc.return_value = ({"2020": 7, "2021": 8}, 15, 14.6, 7, 7.0)

        update_av(checkpoint_every=100)

        out = pl.read_csv(tmp_path / "cleaned_draft_picks_with_av.csv")
        row = out.row(0, named=True)
        assert row["career"] == 15
        assert row["draft_team_career"] == 7
        assert row["draft_team_weighted_career"] == 7.0


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


class TestUpdateAvSkipComplete:
    """Cover the av_complete-skip branch in update_av (force=False)."""

    @patch("nfl_draft_scraper.scrape_av._calculate_av")
    def test_skips_rows_already_marked_complete(self, mock_calc, tmp_path, monkeypatch):
        """Verify av_complete rows are skipped when force is False."""
        monkeypatch.setattr("nfl_draft_scraper.scrape_av.constants.DATA_PATH", tmp_path)
        monkeypatch.setattr("nfl_draft_scraper.scrape_av.constants.START_YEAR", 2024)
        monkeypatch.setattr("nfl_draft_scraper.scrape_av.constants.END_YEAR", 2024)

        df = pl.DataFrame(
            {
                "pfr_player_id": ["Pid0001"],
                "pfr_player_name": ["Player 0"],
                "category": ["QB"],
                "season": [2024],
                "team": ["TST"],
                "2024": [5],
                "career": [5],
                "weighted_career": [5.0],
                "draft_team_career": [5],
                "draft_team_weighted_career": [5.0],
                "av_complete": [True],
            }
        )
        df.write_csv(tmp_path / "cleaned_draft_picks_with_av_checkpoint.csv")

        update_av(checkpoint_every=100)

        mock_calc.assert_not_called()
        out = pl.read_csv(tmp_path / "cleaned_draft_picks_with_av.csv")
        assert out.row(0, named=True)["2024"] == 5
