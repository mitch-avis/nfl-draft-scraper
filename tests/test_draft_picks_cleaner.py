"""Tests for nfl_draft_scraper.draft_picks_cleaner."""

from __future__ import annotations

from unittest.mock import patch

import pandas as pd
import pytest

from nfl_draft_scraper.draft_picks_cleaner import (
    NFLVERSE_DRAFT_PICKS_URL,
    _clean_draft_picks,
    _fetch_raw_draft_picks,
    main,
)


def _make_raw_df(
    seasons: list[int] | None = None,
    *,
    string_cols: bool = False,
) -> pd.DataFrame:
    """Build a raw draft-picks DataFrame resembling the NFLverse CSV.

    When *string_cols* is True the numeric columns are stored as strings with
    leading whitespace, mimicking the quirk in the real CSV that caused the
    original string-sort bug.
    """
    if seasons is None:
        seasons = [2016, 2020, 2021]

    rows: list[dict[str, object]] = []
    for season in seasons:
        for rnd in (1, 2):
            for i in range(1, 4):
                pick = (rnd - 1) * 32 + i
                rows.append(
                    {
                        "season": f" {season}" if string_cols else season,
                        "round": f" {rnd}" if string_cols else rnd,
                        "pick": f" {pick}" if string_cols else pick,
                        "team": "TST",
                        "pfr_player_id": f"id_{season}_{pick}",
                        "pfr_player_name": f"Player {season}-{pick}",
                        "position": "QB",
                        "category": "O",
                        "college": "TestU",
                        "w_av": 10 + pick,
                        "dr_av": 5 + pick,
                        "extra_col": "x",
                    }
                )
    return pd.DataFrame(rows)


class TestCleanDraftPicks:
    """Tests for _clean_draft_picks."""

    def test_filters_seasons_below_start_year(self):
        """Verify rows before START_YEAR are removed."""
        raw = _make_raw_df([2014, 2015, 2016, 2020])
        result = _clean_draft_picks(raw, start_year=2016)
        assert set(result["season"].unique()) == {2016, 2020}

    def test_drops_extra_columns(self):
        """Verify columns not in the keep list are dropped."""
        raw = _make_raw_df([2020])
        result = _clean_draft_picks(raw, start_year=2016)
        assert "extra_col" not in result.columns

    def test_keeps_expected_columns(self):
        """Verify the cleaned output contains all expected columns."""
        raw = _make_raw_df([2020])
        result = _clean_draft_picks(raw, start_year=2016)
        expected = {
            "season",
            "round",
            "pick",
            "team",
            "pfr_player_id",
            "pfr_player_name",
            "position",
            "category",
            "college",
            "w_av",
            "dr_av",
            "round_pick",
        }
        assert set(result.columns) == expected

    def test_column_order_round_pick_after_round(self):
        """Verify round_pick appears after round and before pick in column order."""
        raw = _make_raw_df([2020])
        result = _clean_draft_picks(raw, start_year=2016)
        cols = list(result.columns)
        round_idx = cols.index("round")
        rp_idx = cols.index("round_pick")
        pick_idx = cols.index("pick")
        assert rp_idx == round_idx + 1, "round_pick should be right after round"
        assert rp_idx < pick_idx, "round_pick should come before pick"

    def test_sorts_by_season_and_pick_as_integers(self):
        """Verify sorting treats season and pick as integers not strings.

        With string sorting, pick '9' would sort after '80'. This test uses
        string-typed columns to reproduce the original bug.
        """
        raw = _make_raw_df([2020], string_cols=True)
        result = _clean_draft_picks(raw, start_year=2016)
        picks = result["pick"].tolist()
        assert picks == sorted(picks)
        assert result["season"].dtype in ("int64", "int32", int)

    def test_round_pick_is_sequential_per_round(self):
        """Verify round_pick counts from 1 within each (season, round) group."""
        raw = _make_raw_df([2020])
        result = _clean_draft_picks(raw, start_year=2016)
        grouped = result.groupby(["season", "round"])
        for _key, group in grouped:
            assert group["round_pick"].tolist() == list(range(1, len(group) + 1))

    def test_normalizes_historical_team_abbreviations(self):
        """Verify historical team abbreviations are mapped to current ones."""
        raw = pd.DataFrame(
            {
                "season": [2016, 2016, 2018],
                "round": [1, 1, 1],
                "pick": [1, 2, 3],
                "team": ["SDG", "OAK", "STL"],
                "pfr_player_id": ["id1", "id2", "id3"],
                "pfr_player_name": ["Player A", "Player B", "Player C"],
                "position": ["QB", "RB", "WR"],
                "category": ["O", "O", "O"],
                "college": ["SchoolA", "SchoolB", "SchoolC"],
                "w_av": [10, 20, 30],
                "dr_av": [5, 10, 15],
            }
        )
        result = _clean_draft_picks(raw, start_year=2016)
        teams = result["team"].tolist()
        assert "SDG" not in teams
        assert "OAK" not in teams
        assert "STL" not in teams
        assert "LAC" in teams
        assert "LVR" in teams
        assert "LAR" in teams

    def test_current_team_abbreviations_unchanged(self):
        """Verify current team abbreviations are not modified during cleaning."""
        raw = pd.DataFrame(
            {
                "season": [2023, 2023],
                "round": [1, 1],
                "pick": [1, 2],
                "team": ["LVR", "LAC"],
                "pfr_player_id": ["id1", "id2"],
                "pfr_player_name": ["Player A", "Player B"],
                "position": ["QB", "RB"],
                "category": ["O", "O"],
                "college": ["SchoolA", "SchoolB"],
                "w_av": [10, 20],
                "dr_av": [5, 10],
            }
        )
        result = _clean_draft_picks(raw, start_year=2016)
        teams = result["team"].tolist()
        assert teams == ["LVR", "LAC"]

    def test_handles_missing_optional_columns(self):
        """Verify cleaning works when some expected columns are absent."""
        raw = pd.DataFrame(
            {
                "season": [2020],
                "round": [1],
                "pick": [1],
                "team": ["JAX"],
            }
        )
        result = _clean_draft_picks(raw, start_year=2016)
        assert "season" in result.columns
        assert "pfr_player_id" not in result.columns

    def test_strips_whitespace_from_column_names(self):
        """Verify leading/trailing spaces in column headers are stripped."""
        raw = pd.DataFrame(
            {
                " season": [2020],
                "round ": [1],
                " pick ": [1],
                "team": ["JAX"],
                "pfr_player_id": ["id1"],
                "pfr_player_name": ["Player A"],
                "position": ["QB"],
                "category": ["O"],
                "college": ["TestU"],
            }
        )
        result = _clean_draft_picks(raw, start_year=2016)
        assert "season" in result.columns
        assert "round" in result.columns
        assert "pick" in result.columns


class TestFetchRawDraftPicks:
    """Tests for _fetch_raw_draft_picks."""

    def test_reads_csv_from_url(self):
        """Verify _fetch_raw_draft_picks calls pd.read_csv with the NFLverse URL."""
        fake_df = _make_raw_df([2020])
        with patch("nfl_draft_scraper.draft_picks_cleaner.pd.read_csv", return_value=fake_df) as m:
            result = _fetch_raw_draft_picks()
        m.assert_called_once_with(NFLVERSE_DRAFT_PICKS_URL, low_memory=False)
        pd.testing.assert_frame_equal(result, fake_df)


class TestMain:
    """Tests for main orchestration."""

    def test_downloads_cleans_and_saves(self, tmp_path, monkeypatch):
        """Verify main fetches remote data, cleans it, and writes the CSV."""
        monkeypatch.setattr("nfl_draft_scraper.draft_picks_cleaner.constants.DATA_PATH", tmp_path)
        monkeypatch.setattr("nfl_draft_scraper.draft_picks_cleaner.constants.START_YEAR", 2020)

        raw = _make_raw_df([2016, 2020, 2021])
        with patch(
            "nfl_draft_scraper.draft_picks_cleaner._fetch_raw_draft_picks", return_value=raw
        ):
            main()

        result = pd.read_csv(tmp_path / "cleaned_draft_picks.csv")
        # 2016 rows should be filtered out
        assert 2016 not in result["season"].values
        assert set(result["season"].unique()) == {2020, 2021}
        # Should be sorted by season then pick
        assert result.iloc[0]["season"] == 2020
        assert result.iloc[0]["pick"] == 1

    def test_saves_raw_draft_picks_csv(self, tmp_path, monkeypatch):
        """Verify main saves the raw download as data/draft_picks.csv."""
        monkeypatch.setattr("nfl_draft_scraper.draft_picks_cleaner.constants.DATA_PATH", tmp_path)
        monkeypatch.setattr("nfl_draft_scraper.draft_picks_cleaner.constants.START_YEAR", 2020)

        raw = _make_raw_df([2020])
        with patch(
            "nfl_draft_scraper.draft_picks_cleaner._fetch_raw_draft_picks", return_value=raw
        ):
            main()

        raw_path = tmp_path / "draft_picks.csv"
        assert raw_path.exists(), "Raw draft_picks.csv should be saved"
        saved = pd.read_csv(raw_path)
        # The raw file should contain all original rows and columns
        assert "extra_col" in saved.columns
        assert len(saved) == len(raw)

    def test_overwrites_existing_raw_draft_picks(self, tmp_path, monkeypatch):
        """Verify main overwrites an existing draft_picks.csv."""
        monkeypatch.setattr("nfl_draft_scraper.draft_picks_cleaner.constants.DATA_PATH", tmp_path)
        monkeypatch.setattr("nfl_draft_scraper.draft_picks_cleaner.constants.START_YEAR", 2020)

        # Write a pre-existing file with different content
        old_df = pd.DataFrame({"old_col": [1]})
        old_df.to_csv(tmp_path / "draft_picks.csv", index=False)

        raw = _make_raw_df([2020])
        with patch(
            "nfl_draft_scraper.draft_picks_cleaner._fetch_raw_draft_picks", return_value=raw
        ):
            main()

        saved = pd.read_csv(tmp_path / "draft_picks.csv")
        assert "old_col" not in saved.columns
        assert "extra_col" in saved.columns

    def test_no_local_draft_picks_csv_needed(self, tmp_path, monkeypatch):
        """Verify main does NOT read a local draft_picks.csv file."""
        monkeypatch.setattr("nfl_draft_scraper.draft_picks_cleaner.constants.DATA_PATH", tmp_path)
        monkeypatch.setattr("nfl_draft_scraper.draft_picks_cleaner.constants.START_YEAR", 2020)

        raw = _make_raw_df([2020])
        # No draft_picks.csv exists in tmp_path — this should still work
        assert not (tmp_path / "draft_picks.csv").exists()
        with patch(
            "nfl_draft_scraper.draft_picks_cleaner._fetch_raw_draft_picks", return_value=raw
        ):
            main()

        assert (tmp_path / "cleaned_draft_picks.csv").exists()

    def test_raises_on_fetch_failure(self, tmp_path, monkeypatch):
        """Verify main propagates network errors from fetch."""
        monkeypatch.setattr("nfl_draft_scraper.draft_picks_cleaner.constants.DATA_PATH", tmp_path)

        with (
            patch(
                "nfl_draft_scraper.draft_picks_cleaner._fetch_raw_draft_picks",
                side_effect=OSError("Network error"),
            ),
            pytest.raises(OSError, match="Network error"),
        ):
            main()
