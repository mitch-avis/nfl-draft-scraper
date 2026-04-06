"""Tests for nfl_draft_scraper.big_board_combiner — higher-level functions."""

import pandas as pd

from nfl_draft_scraper.big_board_combiner import MDDB_WEIGHT, _combine_year, main


class TestCombineYear:
    """Tests for _combine_year."""

    def test_combines_two_boards(self, tmp_path, monkeypatch):
        """Verify combines two boards with weighted consensus."""
        monkeypatch.setattr("nfl_draft_scraper.big_board_combiner.constants.DATA_PATH", tmp_path)

        mddb = pd.DataFrame(
            {
                "name": ["Alice Smith", "Bob Jones", "Charlie Brown"],
                "rank": [1, 2, 3],
                "pos": ["QB", "WR", "RB"],
                "school": ["MIT", "Stanford", "Harvard"],
            }
        )
        jlbb = pd.DataFrame(
            {
                "name": ["Alice Smith", "Charlie Brown", "Dave Wilson"],
                "rank": [2, 1, 3],
                "pos": ["QB", "RB", "TE"],
                "school": ["MIT", "Harvard", "Yale"],
                "conference": ["Ivy", "Ivy", "Ivy"],
                "avg": [1.5, 2.0, 3.0],
                "sd": [0.5, 0.7, 1.0],
                "ESPN": [1, 2, 3],
                "PFF": [2, 2, 3],
            }
        )
        mddb.to_csv(tmp_path / "mddb_big_board_2020.csv", index=False)
        jlbb.to_csv(tmp_path / "jl_big_board_2020.csv", index=False)

        _combine_year(2020)

        output = tmp_path / "combined_big_board_2020.csv"
        assert output.exists()
        result = pd.read_csv(output)
        assert len(result) >= 3
        assert "Player" in result.columns
        assert "Consensus" in result.columns
        assert "JL_Avg" in result.columns
        assert "JL_SD" in result.columns
        assert "JL_Sources" in result.columns
        assert "Sources" in result.columns
        assert "MDDB" in result.columns
        assert "JLBB" in result.columns

    def test_handles_no_overlap(self, tmp_path, monkeypatch):
        """Verify handles no overlap — MDDB-only gets MDDB_WEIGHT sources."""
        monkeypatch.setattr("nfl_draft_scraper.big_board_combiner.constants.DATA_PATH", tmp_path)

        mddb = pd.DataFrame({"name": ["Alice"], "rank": [1], "pos": ["QB"], "school": ["MIT"]})
        jlbb = pd.DataFrame(
            {
                "name": ["Bob"],
                "rank": [1],
                "pos": ["WR"],
                "school": ["Yale"],
                "ESPN": [1],
            }
        )
        mddb.to_csv(tmp_path / "mddb_big_board_2021.csv", index=False)
        jlbb.to_csv(tmp_path / "jl_big_board_2021.csv", index=False)

        _combine_year(2021)

        result = pd.read_csv(tmp_path / "combined_big_board_2021.csv")
        assert len(result) == 2
        # Alice is MDDB-only
        alice = result[result["Player"] == "Alice"].iloc[0]
        assert alice["Sources"] == MDDB_WEIGHT


class TestMain:
    """Tests for main."""

    def test_iterates_all_years(self, tmp_path, monkeypatch):
        """Verify iterates all years."""
        monkeypatch.setattr("nfl_draft_scraper.big_board_combiner.constants.DATA_PATH", tmp_path)
        monkeypatch.setattr("nfl_draft_scraper.big_board_combiner.constants.START_YEAR", 2020)
        monkeypatch.setattr("nfl_draft_scraper.big_board_combiner.constants.END_YEAR", 2021)

        for year in [2020, 2021]:
            mddb = pd.DataFrame({"name": ["P1"], "rank": [1], "pos": ["QB"], "school": ["U"]})
            jlbb = pd.DataFrame(
                {
                    "name": ["P1"],
                    "rank": [1],
                    "pos": ["QB"],
                    "school": ["U"],
                    "ESPN": [1],
                }
            )
            mddb.to_csv(tmp_path / f"mddb_big_board_{year}.csv", index=False)
            jlbb.to_csv(tmp_path / f"jl_big_board_{year}.csv", index=False)

        main()

        assert (tmp_path / "combined_big_board_2020.csv").exists()
        assert (tmp_path / "combined_big_board_2021.csv").exists()
