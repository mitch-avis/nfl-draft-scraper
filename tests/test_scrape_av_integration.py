"""Tests for nfl_draft_scraper.scrape_av — integration-level tests."""

import os

import polars as pl

from nfl_draft_scraper.scrape_av import (
    _calculate_weighted_career_av,
    _initialize_draft_picks_df,
    _merge_new_draft_rows,
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


class TestMergeNewDraftRows:
    """Tests for _merge_new_draft_rows."""

    def test_no_new_rows_when_source_matches_checkpoint(self, tmp_path):
        """Verify result is unchanged when all source rows already exist in the checkpoint."""
        draft = tmp_path / "draft.csv"
        write_df_to_csv(
            pl.DataFrame({"pfr_player_id": ["id1"], "season": [2024]}),
            str(draft),
            index=True,
        )
        rows = [{"pfr_player_id": "id1", "season": 2024, "2024": 5, "av_complete": True}]
        result = _merge_new_draft_rows(rows, str(draft), ["2024"])
        assert len(result) == 1
        assert result[0]["pfr_player_id"] == "id1"

    def test_appends_new_rows_from_source(self, tmp_path):
        """Verify rows present in source but absent from the checkpoint are appended."""
        draft = tmp_path / "draft.csv"
        write_df_to_csv(
            pl.DataFrame(
                {
                    "pfr_player_id": ["id1", "id2"],
                    "season": [2024, 2026],
                    "pfr_player_name": ["Player One", "Player Two"],
                }
            ),
            str(draft),
            index=True,
        )
        rows = [{"pfr_player_id": "id1", "season": 2024, "2024": 5, "av_complete": True}]
        result = _merge_new_draft_rows(rows, str(draft), ["2024", "2026"])
        assert len(result) == 2
        new_row = result[1]
        assert new_row["pfr_player_id"] == "id2"
        assert new_row["av_complete"] is False
        assert new_row["2024"] is None
        assert new_row["2026"] is None

    def test_adds_missing_av_columns_to_existing_rows(self, tmp_path):
        """Verify new year columns in av_columns are added to existing rows as None."""
        draft = tmp_path / "nonexistent.csv"
        rows = [{"pfr_player_id": "id1", "season": 2024, "2024": 5, "av_complete": True}]
        result = _merge_new_draft_rows(rows, str(draft), ["2024", "2026"])
        assert len(result) == 1
        assert result[0]["2026"] is None

    def test_source_does_not_exist_returns_rows_unchanged(self, tmp_path):
        """Verify rows are returned unchanged when draft_path does not exist."""
        rows = [{"pfr_player_id": "id1", "season": 2024, "2024": 5, "av_complete": True}]
        result = _merge_new_draft_rows(rows, str(tmp_path / "missing.csv"), ["2024"])
        assert result == rows


class TestInitializeDraftPicksDfWithNewRows:
    """Tests for _initialize_draft_picks_df when the checkpoint is missing new source rows."""

    def test_checkpoint_picks_up_new_source_rows(self, tmp_path):
        """Verify rows in source but absent from the checkpoint are merged in on resume."""
        checkpoint = tmp_path / "ck.csv"
        pl.DataFrame(
            {
                "pfr_player_id": ["id1"],
                "season": [2024],
                "2024": [5],
                "career": [5],
                "weighted_career": [5.0],
                "draft_team_career": [5],
                "draft_team_weighted_career": [5.0],
                "av_complete": [True],
            }
        ).write_csv(checkpoint)

        draft = tmp_path / "draft.csv"
        write_df_to_csv(
            pl.DataFrame(
                {
                    "pfr_player_id": ["id1", "id2"],
                    "season": [2024, 2026],
                    "pfr_player_name": ["P1", "P2"],
                }
            ),
            str(draft),
            index=True,
        )

        result = _initialize_draft_picks_df(str(draft), str(checkpoint), ["2024", "2026"])
        assert len(result) == 2
        existing = next(r for r in result if r["pfr_player_id"] == "id1")
        new_pick = next(r for r in result if r["pfr_player_id"] == "id2")
        assert existing["av_complete"] is True
        assert new_pick["av_complete"] is False
        assert new_pick["2026"] is None
