"""Tests for nfl_draft_scraper.draft_picks_cleaner."""

import pandas as pd

from nfl_draft_scraper.draft_picks_cleaner import main


class TestDraftPicksCleaner:
    """Tests for draft_picks_cleaner.main."""

    def test_cleans_and_saves(self, tmp_path, monkeypatch):
        """Verify cleans and saves."""
        monkeypatch.setattr(
            "nfl_draft_scraper.draft_picks_cleaner.constants.DATA_PATH", str(tmp_path)
        )

        df = pd.DataFrame(
            {
                "season": [2021, 2020, 2021],
                "round": [1, 1, 2],
                "pick": [3, 1, 35],
                "team": ["NYJ", "JAX", "NYJ"],
                "pfr_player_id": ["id1", "id2", "id3"],
                "pfr_player_name": ["Player A", "Player B", "Player C"],
                "position": ["QB", "QB", "WR"],
                "category": ["O", "O", "O"],
                "college": ["School1", "School2", "School3"],
                "extra_col": ["x", "y", "z"],
            }
        )
        df.to_csv(tmp_path / "draft_picks.csv", index=False)

        main()

        result = pd.read_csv(tmp_path / "cleaned_draft_picks.csv")
        assert "extra_col" not in result.columns
        assert "season" in result.columns
        # Should be sorted by season then pick
        assert result.iloc[0]["season"] == 2020
        assert result.iloc[0]["pick"] == 1

    def test_handles_missing_columns(self, tmp_path, monkeypatch):
        """Verify handles missing columns."""
        monkeypatch.setattr(
            "nfl_draft_scraper.draft_picks_cleaner.constants.DATA_PATH", str(tmp_path)
        )

        df = pd.DataFrame(
            {
                "season": [2020],
                "round": [1],
                "pick": [1],
                "team": ["JAX"],
                # Missing pfr_player_id and others
            }
        )
        df.to_csv(tmp_path / "draft_picks.csv", index=False)

        main()

        result = pd.read_csv(tmp_path / "cleaned_draft_picks.csv")
        assert "season" in result.columns
