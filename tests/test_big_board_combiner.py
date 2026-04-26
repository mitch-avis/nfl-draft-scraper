"""Tests for nfl_draft_scraper.big_board_combiner."""

import polars as pl
import pytest

from nfl_draft_scraper.big_board_combiner import (
    WL_WEIGHT,
    _best_match,
    _build_combined_rows,
    _clean_df,
    _extract_jl_source_ranks,
    _get_record,
)


class TestCleanDf:
    """Tests for _clean_df."""

    def test_drops_missing_name(self):
        """Verify drops missing name."""
        df = pl.DataFrame({"name": ["Alice", None], "rank": [1, 2], "pos": ["QB", "WR"]})
        result = _clean_df(df, ["name", "rank", "pos"])
        assert result.height == 1
        assert result.row(0, named=True)["name"] == "Alice"

    def test_drops_missing_rank(self):
        """Verify drops missing rank."""
        df = pl.DataFrame({"name": ["Alice", "Bob"], "rank": [1, None], "pos": ["QB", "WR"]})
        result = _clean_df(df, ["name", "rank", "pos"])
        assert result.height == 1

    def test_drops_duplicate_names(self):
        """Verify drops duplicate names."""
        df = pl.DataFrame(
            {"name": ["Alice", "Alice", "Bob"], "rank": [1, 2, 3], "pos": ["QB", "QB", "WR"]}
        )
        result = _clean_df(df, ["name", "rank", "pos"])
        assert result.height == 2

    def test_casts_rank_to_int(self):
        """Verify casts rank to int."""
        df = pl.DataFrame({"name": ["Alice"], "rank": [1.0], "pos": ["QB"]})
        result = _clean_df(df, ["name", "rank", "pos"])
        assert result.row(0, named=True)["rank"] == 1
        assert result["rank"].dtype == pl.Int64

    def test_sorts_by_rank(self):
        """Verify sorts by rank."""
        df = pl.DataFrame({"name": ["Bob", "Alice"], "rank": [2, 1], "pos": ["WR", "QB"]})
        result = _clean_df(df, ["name", "rank", "pos"])
        assert result.row(0, named=True)["name"] == "Alice"
        assert result.row(1, named=True)["name"] == "Bob"

    def test_selects_columns(self):
        """Verify selects columns."""
        df = pl.DataFrame({"name": ["Alice"], "rank": [1], "pos": ["QB"], "extra": ["val"]})
        result = _clean_df(df, ["name", "rank"])
        assert result.columns == ["name", "rank"]


class TestBestMatch:
    """Tests for _best_match."""

    def test_exact_match(self):
        """Verify exact match."""
        assert (
            _best_match("Patrick Mahomes", ["Patrick Mahomes", "Josh Allen"]) == "Patrick Mahomes"
        )

    def test_fuzzy_match(self):
        """Verify fuzzy match."""
        result = _best_match("Pat Mahomes", ["Patrick Mahomes", "Josh Allen"])
        assert result == "Patrick Mahomes"

    def test_no_match_below_cutoff(self):
        """Verify no match below cutoff."""
        assert _best_match("ZZZZZ", ["Alice", "Bob"]) is None

    def test_empty_choices(self):
        """Verify empty choices."""
        assert _best_match("Alice", []) is None

    def test_custom_cutoff(self):
        """Verify custom cutoff."""
        assert _best_match("Pat", ["Patrick Mahomes"], cutoff=0.99) is None

    def test_rejects_different_last_name(self):
        """Verify fuzzy match is rejected when last names differ."""
        assert _best_match("Mason Rudolph", ["Mason Randolph"]) is None

    def test_rejects_different_first_name_same_last(self):
        """Verify fuzzy match is rejected when first names are incompatible."""
        assert _best_match("Faion Hicks", ["Ja'Von Hicks"]) is None

    def test_accepts_compatible_first_name_abbreviation(self):
        """Verify fuzzy match is accepted when first name is an abbreviation."""
        assert _best_match("Pat Mahomes", ["Patrick Mahomes"]) == "Patrick Mahomes"


class TestGetRecord:
    """Tests for _get_record."""

    def test_exact_name_found(self):
        """Verify exact name found."""
        df = pl.DataFrame({"name": ["Alice", "Bob"], "rank": [1, 2]})
        result = _get_record("Alice", df, ["Alice", "Bob"])
        assert result is not None
        assert result["rank"] == 1

    def test_fuzzy_name_found(self):
        """Verify fuzzy name found."""
        df = pl.DataFrame({"name": ["Patrick Mahomes", "Josh Allen"], "rank": [1, 2]})
        result = _get_record("Pat Mahomes", df, ["Patrick Mahomes", "Josh Allen"])
        assert result is not None
        assert result["name"] == "Patrick Mahomes"

    def test_no_match(self):
        """Verify no match."""
        df = pl.DataFrame({"name": ["Alice"], "rank": [1]})
        result = _get_record("ZZZZZ", df, ["Alice"])
        assert result is None


class TestBuildCombinedRows:
    """Tests for _build_combined_rows."""

    def test_both_sources_have_player(self):
        """Verify both sources have player."""
        wl_df = pl.DataFrame({"name": ["Alice"], "rank": [1], "pos": ["QB"], "school": ["MIT"]})
        jlbb_df = pl.DataFrame({"name": ["Alice"], "rank": [3], "pos": ["QB"], "school": ["MIT"]})
        rows = _build_combined_rows(
            ["Alice"], wl_df, jlbb_df, ["Alice"], ["Alice"], jl_source_ranks={}
        )
        assert len(rows) == 1
        assert rows[0]["WL"] == 1.0
        assert rows[0]["JLBB"] == 3.0

    def test_only_wl_has_player(self):
        """Verify only wl has player."""
        wl_df = pl.DataFrame({"name": ["Alice"], "rank": [1], "pos": ["QB"], "school": ["MIT"]})
        jlbb_df = pl.DataFrame(
            {"name": ["Bob"], "rank": [2], "pos": ["WR"], "school": ["Stanford"]}
        )
        rows = _build_combined_rows(
            ["Alice"], wl_df, jlbb_df, ["Alice"], ["Bob"], jl_source_ranks={}
        )
        assert len(rows) == 1
        assert rows[0]["WL"] == 1.0
        assert rows[0]["JLBB"] is None

    def test_only_jlbb_has_player(self):
        """Verify only jlbb has player."""
        wl_df = pl.DataFrame({"name": ["Bob"], "rank": [2], "pos": ["QB"], "school": ["MIT"]})
        jlbb_df = pl.DataFrame(
            {"name": ["Alice"], "rank": [3], "pos": ["WR"], "school": ["Stanford"]}
        )
        rows = _build_combined_rows(
            ["Alice"], wl_df, jlbb_df, ["Bob"], ["Alice"], jl_source_ranks={}
        )
        assert len(rows) == 1
        assert rows[0]["WL"] is None
        assert rows[0]["JLBB"] == 3.0
        assert rows[0]["School"] == "Stanford"

    def test_neither_source_has_player(self):
        """Verify neither source has player."""
        wl_df = pl.DataFrame({"name": ["Bob"], "rank": [2], "pos": ["QB"], "school": ["MIT"]})
        jlbb_df = pl.DataFrame(
            {"name": ["Bob"], "rank": [2], "pos": ["WR"], "school": ["Stanford"]}
        )
        rows = _build_combined_rows(["ZZZZZ"], wl_df, jlbb_df, ["Bob"], ["Bob"], jl_source_ranks={})
        assert len(rows) == 1
        assert rows[0]["WL"] is None
        assert rows[0]["JLBB"] is None

    def test_school_falls_back_to_jlbb(self):
        """Verify JLBB school is used when WL has no match."""
        wl_df = pl.DataFrame({"name": ["Bob"], "rank": [2], "pos": ["QB"], "school": ["MIT"]})
        jlbb_df = pl.DataFrame(
            {"name": ["Alice"], "rank": [1], "pos": ["WR"], "school": ["Stanford"]}
        )
        rows = _build_combined_rows(
            ["Alice"], wl_df, jlbb_df, ["Bob"], ["Alice"], jl_source_ranks={}
        )
        assert rows[0]["School"] == "Stanford"

    def test_weighted_consensus_both_sources(self):
        """Verify weighted consensus when both WL and JL sources are present."""
        wl_df = pl.DataFrame({"name": ["Alice"], "rank": [1], "pos": ["QB"], "school": ["MIT"]})
        jlbb_df = pl.DataFrame({"name": ["Alice"], "rank": [3], "pos": ["QB"], "school": ["MIT"]})
        jl_source_ranks = {"Alice": [2.0, 4.0, 6.0]}
        rows = _build_combined_rows(
            ["Alice"], wl_df, jlbb_df, ["Alice"], ["Alice"], jl_source_ranks=jl_source_ranks
        )
        assert rows[0]["WL"] == 1.0
        assert rows[0]["JLBB"] == 3.0
        assert rows[0]["JL_Avg"] == pytest.approx(4.0)
        assert rows[0]["JL_Sources"] == 3
        assert rows[0]["Consensus"] == pytest.approx(2.0)
        assert rows[0]["Sources"] == 3 + WL_WEIGHT

    def test_weighted_consensus_only_jl(self):
        """Verify consensus equals JL avg when only JL sources are present."""
        wl_df = pl.DataFrame({"name": ["Bob"], "rank": [2], "pos": ["QB"], "school": ["MIT"]})
        jlbb_df = pl.DataFrame(
            {"name": ["Alice"], "rank": [1], "pos": ["WR"], "school": ["Stanford"]}
        )
        jl_source_ranks = {"Alice": [1.0, 3.0]}
        rows = _build_combined_rows(
            ["Alice"], wl_df, jlbb_df, ["Bob"], ["Alice"], jl_source_ranks=jl_source_ranks
        )
        assert rows[0]["JL_Avg"] == pytest.approx(2.0)
        assert rows[0]["JL_Sources"] == 2
        assert rows[0]["Consensus"] == pytest.approx(2.0)
        assert rows[0]["Sources"] == 2

    def test_weighted_consensus_only_wl(self):
        """Verify consensus equals WL rank when only WL is present."""
        wl_df = pl.DataFrame({"name": ["Alice"], "rank": [5], "pos": ["QB"], "school": ["MIT"]})
        jlbb_df = pl.DataFrame(
            {"name": ["Bob"], "rank": [2], "pos": ["WR"], "school": ["Stanford"]}
        )
        rows = _build_combined_rows(
            ["Alice"], wl_df, jlbb_df, ["Alice"], ["Bob"], jl_source_ranks={}
        )
        assert rows[0]["Consensus"] == pytest.approx(5.0)
        assert rows[0]["Sources"] == WL_WEIGHT
        assert rows[0]["JL_Avg"] is None
        assert rows[0]["JL_Sources"] is None

    def test_weighted_consensus_neither_source(self):
        """Verify consensus is None when neither source has the player."""
        wl_df = pl.DataFrame({"name": ["Bob"], "rank": [2], "pos": ["QB"], "school": ["MIT"]})
        jlbb_df = pl.DataFrame(
            {"name": ["Bob"], "rank": [2], "pos": ["WR"], "school": ["Stanford"]}
        )
        rows = _build_combined_rows(["ZZZZZ"], wl_df, jlbb_df, ["Bob"], ["Bob"], jl_source_ranks={})
        assert rows[0]["Consensus"] is None
        assert rows[0]["Sources"] is None

    def test_jl_sd_populated(self):
        """Verify JL_SD is computed from JL source ranks."""
        wl_df = pl.DataFrame({"name": ["Alice"], "rank": [1], "pos": ["QB"], "school": ["MIT"]})
        jlbb_df = pl.DataFrame({"name": ["Alice"], "rank": [1], "pos": ["QB"], "school": ["MIT"]})
        jl_source_ranks = {"Alice": [3.0, 3.0, 3.0]}
        rows = _build_combined_rows(
            ["Alice"], wl_df, jlbb_df, ["Alice"], ["Alice"], jl_source_ranks=jl_source_ranks
        )
        assert rows[0]["JL_SD"] == pytest.approx(0.0)


class TestExtractJlSourceRanks:
    """Tests for _extract_jl_source_ranks."""

    def test_extracts_numeric_source_ranks(self):
        """Verify numeric source ranks are collected, null values excluded."""
        jl_df = pl.DataFrame(
            {
                "name": ["Alice", "Bob"],
                "rank": [1, 2],
                "pos": ["QB", "WR"],
                "school": ["MIT", "Stanford"],
                "conference": ["SEC", "Pac 12"],
                "avg": [2.0, 5.0],
                "sd": [1.0, 2.0],
                "ESPN": [1.0, 3.0],
                "PFF": [3.0, None],
                "CBS": [None, 7.0],
            }
        )
        result = _extract_jl_source_ranks(jl_df)
        assert result["Alice"] == [1.0, 3.0]
        assert result["Bob"] == [3.0, 7.0]

    def test_empty_dataframe(self):
        """Verify empty DataFrame returns empty dict."""
        jl_df = pl.DataFrame(
            schema={"name": pl.String, "rank": pl.Int64, "pos": pl.String, "school": pl.String}
        )
        result = _extract_jl_source_ranks(jl_df)
        assert result == {}

    def test_no_source_columns(self):
        """Verify DataFrame with only structural columns returns empty lists."""
        jl_df = pl.DataFrame({"name": ["Alice"], "rank": [1], "pos": ["QB"], "school": ["MIT"]})
        result = _extract_jl_source_ranks(jl_df)
        assert result["Alice"] == []

    def test_all_nan_sources(self):
        """Verify player with all-null source ranks gets empty list."""
        jl_df = pl.DataFrame(
            {
                "name": ["Alice"],
                "rank": [1],
                "pos": ["QB"],
                "school": ["MIT"],
                "ESPN": [None],
                "PFF": [None],
            },
            schema={
                "name": pl.String,
                "rank": pl.Int64,
                "pos": pl.String,
                "school": pl.String,
                "ESPN": pl.Float64,
                "PFF": pl.Float64,
            },
        )
        result = _extract_jl_source_ranks(jl_df)
        assert result["Alice"] == []


class TestBuildCombinedRowsWithMddb:
    """Tests for _build_combined_rows when MDDB is supplied as a third source."""

    def test_mddb_contributes_to_consensus(self):
        """Verify MDDB rank participates in the weighted consensus alongside WL."""
        from nfl_draft_scraper.big_board_combiner import MDDB_WEIGHT

        wl_df = pl.DataFrame({"name": ["Alice"], "rank": [2], "pos": ["QB"], "school": ["MIT"]})
        jlbb_df = pl.DataFrame(
            {"name": ["Bob"], "rank": [9], "pos": ["WR"], "school": ["Stanford"]}
        )
        mddb_df = pl.DataFrame({"name": ["Alice"], "rank": [4], "pos": ["QB"], "school": ["MIT"]})
        rows = _build_combined_rows(
            ["Alice"],
            wl_df,
            jlbb_df,
            ["Alice"],
            ["Bob"],
            jl_source_ranks={},
            mddb_df=mddb_df,
            mddb_names=["Alice"],
        )
        assert rows[0]["WL"] == 2.0
        assert rows[0]["MDDB"] == 4.0
        assert rows[0]["Consensus"] == pytest.approx(3.0)
        assert rows[0]["Sources"] == WL_WEIGHT + MDDB_WEIGHT

    def test_only_mddb_has_player(self):
        """Verify pos/school fall back to MDDB when WL and JLBB lack the player."""
        from nfl_draft_scraper.big_board_combiner import MDDB_WEIGHT

        wl_df = pl.DataFrame({"name": ["Bob"], "rank": [1], "pos": ["QB"], "school": ["MIT"]})
        jlbb_df = pl.DataFrame(
            {"name": ["Bob"], "rank": [1], "pos": ["WR"], "school": ["Stanford"]}
        )
        mddb_df = pl.DataFrame({"name": ["Alice"], "rank": [7], "pos": ["DE"], "school": ["Yale"]})
        rows = _build_combined_rows(
            ["Alice"],
            wl_df,
            jlbb_df,
            ["Bob"],
            ["Bob"],
            jl_source_ranks={},
            mddb_df=mddb_df,
            mddb_names=["Alice"],
        )
        assert rows[0]["MDDB"] == 7.0
        assert rows[0]["WL"] is None
        assert rows[0]["Position"] == "DE"
        assert rows[0]["School"] == "Yale"
        assert rows[0]["Consensus"] == pytest.approx(7.0)
        assert rows[0]["Sources"] == MDDB_WEIGHT
