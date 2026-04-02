"""Tests for nfl_draft_scraper.scrape_av — integration-level tests."""

import os

import pandas as pd

from nfl_draft_scraper.scrape_av import (
    _calculate_weighted_career_av,
    _initialize_draft_picks_df,
    _save_checkpoint,
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


class TestCalculateWeightedCareerAvIntegration:
    """Integration test for _calculate_weighted_career_av used by both paths."""

    def test_weighted_av_formula(self):
        """Verify weighted AV applies 5% declining weights to sorted yearly values."""
        av_by_year = {"2020": 10, "2021": 8, "2022": 6}
        result = _calculate_weighted_career_av(av_by_year, [2020, 2021, 2022])
        # sorted: [10, 8, 6], weights: [1.0, 0.95, 0.9]
        expected = round(10 * 1.0 + 8 * 0.95 + 6 * 0.9, 1)
        assert result == expected
