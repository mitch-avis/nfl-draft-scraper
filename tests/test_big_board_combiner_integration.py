"""Tests for nfl_draft_scraper.big_board_combiner — higher-level functions."""

import polars as pl

from nfl_draft_scraper.big_board_combiner import WL_WEIGHT, _combine_year, main


class TestCombineYear:
    """Tests for _combine_year."""

    def test_combines_two_boards(self, tmp_path, monkeypatch):
        """Verify combines two boards with weighted consensus."""
        monkeypatch.setattr("nfl_draft_scraper.big_board_combiner.constants.DATA_PATH", tmp_path)

        wl = pl.DataFrame(
            {
                "name": ["Alice Smith", "Bob Jones", "Charlie Brown"],
                "rank": [1, 2, 3],
                "pos": ["QB", "WR", "RB"],
                "school": ["MIT", "Stanford", "Harvard"],
            }
        )
        jlbb = pl.DataFrame(
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
        wl.write_csv(tmp_path / "wl_big_board_2020.csv")
        jlbb.write_csv(tmp_path / "jl_big_board_2020.csv")

        _combine_year(2020)

        output = tmp_path / "combined_big_board_2020.csv"
        assert output.exists()
        result = pl.read_csv(output)
        assert result.height >= 3
        for col in (
            "Player",
            "Consensus",
            "JL_Avg",
            "JL_SD",
            "JL_Sources",
            "Sources",
            "WL",
            "JLBB",
        ):
            assert col in result.columns

    def test_handles_no_overlap(self, tmp_path, monkeypatch):
        """Verify handles no overlap — WL-only gets WL_WEIGHT sources."""
        monkeypatch.setattr("nfl_draft_scraper.big_board_combiner.constants.DATA_PATH", tmp_path)

        wl = pl.DataFrame({"name": ["Alice"], "rank": [1], "pos": ["QB"], "school": ["MIT"]})
        jlbb = pl.DataFrame(
            {
                "name": ["Bob"],
                "rank": [1],
                "pos": ["WR"],
                "school": ["Yale"],
                "ESPN": [1],
            }
        )
        wl.write_csv(tmp_path / "wl_big_board_2021.csv")
        jlbb.write_csv(tmp_path / "jl_big_board_2021.csv")

        _combine_year(2021)

        result = pl.read_csv(tmp_path / "combined_big_board_2021.csv")
        assert result.height == 2
        alice = result.filter(pl.col("Player") == "Alice").row(0, named=True)
        assert alice["Sources"] == WL_WEIGHT


class TestMain:
    """Tests for main."""

    def test_iterates_all_years(self, tmp_path, monkeypatch):
        """Verify iterates all years."""
        monkeypatch.setattr("nfl_draft_scraper.big_board_combiner.constants.DATA_PATH", tmp_path)
        monkeypatch.setattr("nfl_draft_scraper.big_board_combiner.constants.START_YEAR", 2020)
        monkeypatch.setattr("nfl_draft_scraper.big_board_combiner.constants.END_YEAR", 2021)

        for year in (2020, 2021):
            wl = pl.DataFrame({"name": ["P1"], "rank": [1], "pos": ["QB"], "school": ["U"]})
            jlbb = pl.DataFrame(
                {
                    "name": ["P1"],
                    "rank": [1],
                    "pos": ["QB"],
                    "school": ["U"],
                    "ESPN": [1],
                }
            )
            wl.write_csv(tmp_path / f"wl_big_board_{year}.csv")
            jlbb.write_csv(tmp_path / f"jl_big_board_{year}.csv")

        main()

        assert (tmp_path / "combined_big_board_2020.csv").exists()
        assert (tmp_path / "combined_big_board_2021.csv").exists()
