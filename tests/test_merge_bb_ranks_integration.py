"""Tests for nfl_draft_scraper.merge_bb_ranks_to_picks — integration tests."""

import polars as pl

from nfl_draft_scraper.merge_bb_ranks_to_picks import (
    _merge_big_board_ranks_for_year,
    _reorder_and_save,
    main,
)


class TestReorderAndSave:
    """Tests for _reorder_and_save."""

    def test_reorders_and_renames(self, tmp_path):
        """Verify reorders and renames."""
        df = pl.DataFrame(
            {
                "round": [1],
                "round_pick": [1],
                "pick": [1],
                "team": ["JAX"],
                "pfr_player_id": ["LawrTr00"],
                "pfr_player_name": ["Trevor Lawrence"],
                "position": ["QB"],
                "category": ["O"],
                "college": ["Clemson"],
                "WL Rank": [1.0],
                "JLBB Rank": [1.0],
                "Consensus": [1.0],
                "JL_Avg": [1.5],
                "JL_SD": [0.5],
                "JL_Sources": [10],
                "Sources": [16],
                "2021": [5],
                "career": [5],
                "weighted_career": [5.0],
            }
        )
        path = tmp_path / "output.csv"
        _reorder_and_save(df, path, ["2021", "career", "weighted_career"])
        result = pl.read_csv(path)
        assert "player" in result.columns
        assert "overall_pick" in result.columns
        assert "pfr_player_name" not in result.columns
        assert "pfr_player_id" in result.columns
        assert "Consensus" in result.columns
        assert "JL_Avg" in result.columns
        assert "JL_SD" in result.columns
        assert "JL_Sources" in result.columns
        assert "Sources" in result.columns
        assert "AvgRank" not in result.columns


class TestMergeBigBoardRanksForYear:
    """Tests for _merge_big_board_ranks_for_year."""

    def test_merges_ranks(self, tmp_path, monkeypatch):
        """Verify merges ranks."""
        monkeypatch.setattr(
            "nfl_draft_scraper.merge_bb_ranks_to_picks.constants.DATA_PATH", tmp_path
        )

        picks_df = pl.DataFrame(
            {
                "season": [2020, 2020],
                "round": [1, 2],
                "pick": [1, 33],
                "team": ["CIN", "CIN"],
                "pfr_player_id": ["id1", "id2"],
                "pfr_player_name": ["Joe Burrow", "Tee Higgins"],
                "position": ["QB", "WR"],
                "category": ["O", "O"],
                "college": ["LSU", "Clemson"],
                "2020": [5, 2],
                "career": [5, 2],
                "weighted_career": [5.0, 2.0],
            }
        )
        picks_df.write_csv(tmp_path / "cleaned_draft_picks_with_av.csv")

        bb_df = pl.DataFrame(
            {
                "Player": ["Joe Burrow", "Tee Higgins"],
                "Position": ["QB", "WR"],
                "School": ["LSU", "Clemson"],
                "WL": [1, 25],
                "JLBB": [1, 30],
                "JL_Avg": [1.5, 28.0],
                "JL_SD": [0.5, 3.0],
                "JL_Sources": [10, 8],
                "Consensus": [1.2, 26.1],
                "Sources": [16, 14],
            }
        )
        bb_df.write_csv(tmp_path / "combined_big_board_2020.csv")

        _merge_big_board_ranks_for_year(2020)

        output = tmp_path / "draft_picks_with_big_board_ranks_2020.csv"
        assert output.exists()
        result = pl.read_csv(output)
        assert "WL Rank" in result.columns
        assert "JLBB Rank" in result.columns
        assert "Consensus" in result.columns
        assert "JL_Avg" in result.columns
        assert "JL_SD" in result.columns
        assert "JL_Sources" in result.columns
        assert "Sources" in result.columns
        assert "AvgRank" not in result.columns
        assert result.height == 2

    def test_skips_missing_picks_file(self, tmp_path, monkeypatch):
        """Verify skips missing picks file."""
        monkeypatch.setattr(
            "nfl_draft_scraper.merge_bb_ranks_to_picks.constants.DATA_PATH", tmp_path
        )
        # Should not raise
        _merge_big_board_ranks_for_year(2020)

    def test_skips_missing_bb_file(self, tmp_path, monkeypatch):
        """Verify skips missing bb file."""
        monkeypatch.setattr(
            "nfl_draft_scraper.merge_bb_ranks_to_picks.constants.DATA_PATH", tmp_path
        )
        picks_df = pl.DataFrame(
            {
                "season": [2020],
                "round": [1],
                "pick": [1],
                "team": ["CIN"],
                "pfr_player_id": ["id1"],
                "pfr_player_name": ["Joe Burrow"],
                "position": ["QB"],
                "category": ["O"],
                "college": ["LSU"],
            }
        )
        picks_df.write_csv(tmp_path / "cleaned_draft_picks_with_av.csv")
        # No big board file — should not raise
        _merge_big_board_ranks_for_year(2020)

    def test_skips_empty_year(self, tmp_path, monkeypatch):
        """Verify skips empty year."""
        monkeypatch.setattr(
            "nfl_draft_scraper.merge_bb_ranks_to_picks.constants.DATA_PATH", tmp_path
        )
        picks_df = pl.DataFrame(
            {
                "season": [2019],
                "round": [1],
                "pick": [1],
                "team": ["ARI"],
                "pfr_player_id": ["id1"],
                "pfr_player_name": ["Kyler Murray"],
                "position": ["QB"],
                "category": ["O"],
                "college": ["Oklahoma"],
            }
        )
        picks_df.write_csv(tmp_path / "cleaned_draft_picks_with_av.csv")
        bb_df = pl.DataFrame(
            {
                "Player": ["P1"],
                "Position": ["QB"],
                "School": ["School1"],
                "WL": [1],
                "JLBB": [1],
                "JL_Avg": [1.5],
                "JL_SD": [0.5],
                "JL_Sources": [10],
                "Consensus": [1.2],
                "Sources": [16],
            }
        )
        bb_df.write_csv(tmp_path / "combined_big_board_2020.csv")
        # Year 2020 has no picks — should not raise
        _merge_big_board_ranks_for_year(2020)


class TestMain:
    """Tests for main."""

    def test_iterates_all_years(self, tmp_path, monkeypatch):
        """Verify iterates all years."""
        monkeypatch.setattr(
            "nfl_draft_scraper.merge_bb_ranks_to_picks.constants.DATA_PATH", tmp_path
        )
        monkeypatch.setattr("nfl_draft_scraper.merge_bb_ranks_to_picks.constants.START_YEAR", 2020)
        monkeypatch.setattr("nfl_draft_scraper.merge_bb_ranks_to_picks.constants.END_YEAR", 2020)

        picks_df = pl.DataFrame(
            {
                "season": [2020],
                "round": [1],
                "pick": [1],
                "team": ["CIN"],
                "pfr_player_id": ["id1"],
                "pfr_player_name": ["Joe Burrow"],
                "position": ["QB"],
                "category": ["O"],
                "college": ["LSU"],
                "2020": [5],
                "career": [5],
                "weighted_career": [5.0],
            }
        )
        picks_df.write_csv(tmp_path / "cleaned_draft_picks_with_av.csv")
        bb_df = pl.DataFrame(
            {
                "Player": ["Joe Burrow"],
                "Position": ["QB"],
                "School": ["LSU"],
                "WL": [1],
                "JLBB": [1],
                "JL_Avg": [1.5],
                "JL_SD": [0.5],
                "JL_Sources": [10],
                "Consensus": [1.2],
                "Sources": [16],
            }
        )
        bb_df.write_csv(tmp_path / "combined_big_board_2020.csv")

        main()

        assert (tmp_path / "draft_picks_with_big_board_ranks_2020.csv").exists()
