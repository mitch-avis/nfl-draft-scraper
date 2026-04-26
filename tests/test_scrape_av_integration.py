"""Tests for nfl_draft_scraper.scrape_av — integration-level tests."""

import os

import polars as pl

from nfl_draft_scraper.scrape_av import (
    _calculate_weighted_career_av,
    _initialize_draft_picks_df,
    _save_checkpoint,
)
from nfl_draft_scraper.utils.csv_utils import write_df_to_csv


class TestInitializeDraftPicksDf:
    """Tests for _initialize_draft_picks_df."""

    def test_loads_from_checkpoint(self, tmp_path):
        """Verify loads from checkpoint."""
        checkpoint = tmp_path / "checkpoint.csv"
        df = pl.DataFrame(
            {
                "pfr_player_id": ["id1"],
                "2020": [5],
                "career": [5],
                "weighted_career": [5.0],
                "av_complete": [True],
            }
        )
        df.write_csv(checkpoint)

        result = _initialize_draft_picks_df(str(tmp_path / "draft.csv"), str(checkpoint), ["2020"])
        assert len(result) == 1
        assert result[0]["av_complete"] is True

    def test_initializes_from_draft(self, tmp_path):
        """Verify initializes from draft."""
        draft = tmp_path / "draft.csv"
        df = pl.DataFrame({"pfr_player_id": ["id1"], "pfr_player_name": ["Player"]})
        write_df_to_csv(df, draft, index=True)

        result = _initialize_draft_picks_df(str(draft), str(tmp_path / "nonexistent.csv"), ["2020"])
        assert "2020" in result[0]
        assert "career" in result[0]
        assert "weighted_career" in result[0]
        assert "av_complete" in result[0]
        assert result[0]["av_complete"] is False

    def test_adds_av_complete_if_missing(self, tmp_path):
        """Verify adds av complete if missing."""
        checkpoint = tmp_path / "checkpoint.csv"
        df = pl.DataFrame({"pfr_player_id": ["id1"], "2020": [5]})
        df.write_csv(checkpoint)

        result = _initialize_draft_picks_df(str(tmp_path / "draft.csv"), str(checkpoint), ["2020"])
        assert "av_complete" in result[0]


class TestSaveCheckpoint:
    """Tests for _save_checkpoint."""

    def test_saves_csv(self, tmp_path):
        """Verify saves csv."""
        rows = [{"a": 1, "av_complete": True}]
        path = str(tmp_path / "checkpoint.csv")
        _save_checkpoint(rows, path)
        assert os.path.exists(path)
        result = pl.read_csv(path)
        assert result.height == 1


class TestCalculateWeightedCareerAvIntegration:
    """Integration test for _calculate_weighted_career_av used by both paths."""

    def test_weighted_av_formula(self):
        """Verify weighted AV applies 5% declining weights to sorted yearly values."""
        av_by_year = {"2020": 10, "2021": 8, "2022": 6}
        result = _calculate_weighted_career_av(av_by_year, [2020, 2021, 2022])
        expected = round(10 * 1.0 + 8 * 0.95 + 6 * 0.9, 1)
        assert result == expected
