"""Tests for nfl_draft_scraper.big_board_combiner — higher-level functions."""

import polars as pl

from nfl_draft_scraper.big_board_combiner import _combine_year, main


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
            "WL",
            "JLBB",
        ):
            assert col in result.columns

    def test_handles_no_overlap(self, tmp_path, monkeypatch):
        """Verify handles no overlap — WL-only gets WL_NEFF effective sources."""
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


class TestCombineYearWithMddb:
    """Tests for _combine_year when an MDDB CSV is present."""

    def test_combines_three_boards_when_mddb_present(self, tmp_path, monkeypatch):
        """Verify MDDB CSV contributes an MDDB column when the file exists for the year."""
        monkeypatch.setattr("nfl_draft_scraper.big_board_combiner.constants.DATA_PATH", tmp_path)

        wl = pl.DataFrame({"name": ["Alice"], "rank": [1], "pos": ["QB"], "school": ["MIT"]})
        jlbb = pl.DataFrame(
            {
                "name": ["Alice"],
                "rank": [3],
                "pos": ["QB"],
                "school": ["MIT"],
                "ESPN": [2],
            }
        )
        mddb = pl.DataFrame({"name": ["Alice"], "rank": [2], "pos": ["QB"], "school": ["MIT"]})
        wl.write_csv(tmp_path / "wl_big_board_2022.csv")
        jlbb.write_csv(tmp_path / "jl_big_board_2022.csv")
        mddb.write_csv(tmp_path / "mddb_big_board_2022.csv")

        _combine_year(2022)

        result = pl.read_csv(tmp_path / "combined_big_board_2022.csv")
        assert "MDDB" in result.columns
        alice = result.filter(pl.col("Player") == "Alice").row(0, named=True)
        assert alice["WL"] == 1.0
        assert alice["MDDB"] == 2.0


class TestCombineYearWithoutWl:
    """Tests for _combine_year when no WL CSV exists for the given year."""

    def test_combine_year_jl_and_mddb_when_wl_absent(self, tmp_path, monkeypatch):
        """Verify year combines successfully using JL + MDDB when WL file is absent."""
        monkeypatch.setattr("nfl_draft_scraper.big_board_combiner.constants.DATA_PATH", tmp_path)

        jlbb = pl.DataFrame(
            {
                "name": ["Alice Smith"],
                "rank": [1],
                "pos": ["QB"],
                "school": ["MIT"],
                "ESPN": [1],
            }
        )
        mddb = pl.DataFrame(
            {"name": ["Alice Smith"], "rank": [2], "pos": ["QB"], "school": ["MIT"]}
        )
        jlbb.write_csv(tmp_path / "jl_big_board_2018.csv")
        mddb.write_csv(tmp_path / "mddb_big_board_2018.csv")
        # No wl_big_board_2018.csv written

        _combine_year(2018)

        output = tmp_path / "combined_big_board_2018.csv"
        assert output.exists()
        result = pl.read_csv(output)
        assert result.height == 1
        alice = result.filter(pl.col("Player") == "Alice Smith").row(0, named=True)
        assert alice["WL"] is None
        assert alice["MDDB"] == 2.0
        assert alice["JLBB"] == 1.0
        assert alice["Consensus"] is not None

    def test_combine_year_jl_only_when_wl_and_mddb_absent(self, tmp_path, monkeypatch):
        """Verify year combines using JL only when both WL and MDDB files are absent."""
        monkeypatch.setattr("nfl_draft_scraper.big_board_combiner.constants.DATA_PATH", tmp_path)

        jlbb = pl.DataFrame(
            {
                "name": ["Bob Jones"],
                "rank": [1],
                "pos": ["WR"],
                "school": ["Yale"],
                "ESPN": [1],
            }
        )
        jlbb.write_csv(tmp_path / "jl_big_board_2019.csv")
        # No wl or mddb files

        _combine_year(2019)

        output = tmp_path / "combined_big_board_2019.csv"
        assert output.exists()
        result = pl.read_csv(output)
        assert result.height == 1
        bob = result.filter(pl.col("Player") == "Bob Jones").row(0, named=True)
        assert bob["WL"] is None
        assert bob["MDDB"] is None
        assert bob["JLBB"] == 1.0
        assert bob["Consensus"] is not None

    def test_main_iterates_years_with_mixed_wl_availability(self, tmp_path, monkeypatch):
        """Verify main succeeds when some years have WL files and some do not."""
        monkeypatch.setattr("nfl_draft_scraper.big_board_combiner.constants.DATA_PATH", tmp_path)
        monkeypatch.setattr("nfl_draft_scraper.big_board_combiner.constants.START_YEAR", 2020)
        monkeypatch.setattr("nfl_draft_scraper.big_board_combiner.constants.END_YEAR", 2021)

        # 2020 — no WL, JL only
        jlbb_2020 = pl.DataFrame(
            {"name": ["P1"], "rank": [1], "pos": ["QB"], "school": ["U"], "ESPN": [1]}
        )
        jlbb_2020.write_csv(tmp_path / "jl_big_board_2020.csv")

        # 2021 — WL + JL
        wl_2021 = pl.DataFrame({"name": ["P1"], "rank": [1], "pos": ["QB"], "school": ["U"]})
        jlbb_2021 = pl.DataFrame(
            {"name": ["P1"], "rank": [1], "pos": ["QB"], "school": ["U"], "ESPN": [1]}
        )
        wl_2021.write_csv(tmp_path / "wl_big_board_2021.csv")
        jlbb_2021.write_csv(tmp_path / "jl_big_board_2021.csv")

        main()

        assert (tmp_path / "combined_big_board_2020.csv").exists()
        assert (tmp_path / "combined_big_board_2021.csv").exists()
        result_2020 = pl.read_csv(tmp_path / "combined_big_board_2020.csv")
        result_2021 = pl.read_csv(tmp_path / "combined_big_board_2021.csv")
        assert result_2020.filter(pl.col("WL").is_null()).height == result_2020.height
        assert result_2021.filter(pl.col("WL").is_not_null()).height == result_2021.height
