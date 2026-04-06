"""Tests for nfl_draft_scraper.big_board_combiner."""

import pandas as pd
import pytest

from nfl_draft_scraper.big_board_combiner import (
    MDDB_WEIGHT,
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
        df = pd.DataFrame({"name": ["Alice", None], "rank": [1, 2], "pos": ["QB", "WR"]})
        result = _clean_df(df, ["name", "rank", "pos"])
        assert len(result) == 1
        assert result.iloc[0]["name"] == "Alice"

    def test_drops_missing_rank(self):
        """Verify drops missing rank."""
        df = pd.DataFrame({"name": ["Alice", "Bob"], "rank": [1, None], "pos": ["QB", "WR"]})
        result = _clean_df(df, ["name", "rank", "pos"])
        assert len(result) == 1

    def test_drops_duplicate_names(self):
        """Verify drops duplicate names."""
        df = pd.DataFrame(
            {"name": ["Alice", "Alice", "Bob"], "rank": [1, 2, 3], "pos": ["QB", "QB", "WR"]}
        )
        result = _clean_df(df, ["name", "rank", "pos"])
        assert len(result) == 2

    def test_casts_rank_to_int(self):
        """Verify casts rank to int."""
        df = pd.DataFrame({"name": ["Alice"], "rank": [1.0], "pos": ["QB"]})
        result = _clean_df(df, ["name", "rank", "pos"])
        assert result.iloc[0]["rank"] == 1
        assert int(result.iloc[0]["rank"]) == 1

    def test_sorts_by_rank(self):
        """Verify sorts by rank."""
        df = pd.DataFrame({"name": ["Bob", "Alice"], "rank": [2, 1], "pos": ["WR", "QB"]})
        result = _clean_df(df, ["name", "rank", "pos"])
        assert result.iloc[0]["name"] == "Alice"
        assert result.iloc[1]["name"] == "Bob"

    def test_selects_columns(self):
        """Verify selects columns."""
        df = pd.DataFrame({"name": ["Alice"], "rank": [1], "pos": ["QB"], "extra": ["val"]})
        result = _clean_df(df, ["name", "rank"])
        assert list(result.columns) == ["name", "rank"]


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
        # Very high cutoff should reject fuzzy matches
        """Verify custom cutoff."""
        assert _best_match("Pat", ["Patrick Mahomes"], cutoff=0.99) is None

    def test_rejects_different_last_name(self):
        """Verify fuzzy match is rejected when last names differ."""
        # "Mason Rudolph" should not match "Mason Randolph" — different players
        assert _best_match("Mason Rudolph", ["Mason Randolph"]) is None

    def test_rejects_different_first_name_same_last(self):
        """Verify fuzzy match is rejected when first names are incompatible.

        Faion Hicks (MDDB) and Ja'Von Hicks (JLBB) share a last name but are
        completely different players.  The name ratio of 0.78 exceeds the 0.75
        cutoff, so only the first-name check prevents a false merge.
        """
        assert _best_match("Faion Hicks", ["Ja'Von Hicks"]) is None

    def test_accepts_compatible_first_name_abbreviation(self):
        """Verify fuzzy match is accepted when first name is an abbreviation.

        Pat Mahomes and Patrick Mahomes are the same person — Pat⊂Patrick.
        """
        assert _best_match("Pat Mahomes", ["Patrick Mahomes"]) == "Patrick Mahomes"


class TestGetRecord:
    """Tests for _get_record."""

    def test_exact_name_found(self):
        """Verify exact name found."""
        df = pd.DataFrame({"name": ["Alice", "Bob"], "rank": [1, 2]})
        result = _get_record("Alice", df, ["Alice", "Bob"])
        assert result is not None
        assert result["rank"] == 1

    def test_fuzzy_name_found(self):
        """Verify fuzzy name found."""
        df = pd.DataFrame({"name": ["Patrick Mahomes", "Josh Allen"], "rank": [1, 2]})
        result = _get_record("Pat Mahomes", df, ["Patrick Mahomes", "Josh Allen"])
        assert result is not None
        assert result["name"] == "Patrick Mahomes"

    def test_no_match(self):
        """Verify no match."""
        df = pd.DataFrame({"name": ["Alice"], "rank": [1]})
        result = _get_record("ZZZZZ", df, ["Alice"])
        assert result is None


class TestBuildCombinedRows:
    """Tests for _build_combined_rows."""

    def test_both_sources_have_player(self):
        """Verify both sources have player."""
        mddb_df = pd.DataFrame({"name": ["Alice"], "rank": [1], "pos": ["QB"], "school": ["MIT"]})
        jlbb_df = pd.DataFrame({"name": ["Alice"], "rank": [3], "pos": ["QB"], "school": ["MIT"]})
        rows = _build_combined_rows(
            ["Alice"], mddb_df, jlbb_df, ["Alice"], ["Alice"], jl_source_ranks={}
        )
        assert len(rows) == 1
        assert rows[0]["MDDB"] == 1.0
        assert rows[0]["JLBB"] == 3.0

    def test_only_mddb_has_player(self):
        """Verify only mddb has player."""
        mddb_df = pd.DataFrame({"name": ["Alice"], "rank": [1], "pos": ["QB"], "school": ["MIT"]})
        jlbb_df = pd.DataFrame(
            {"name": ["Bob"], "rank": [2], "pos": ["WR"], "school": ["Stanford"]}
        )
        rows = _build_combined_rows(
            ["Alice"], mddb_df, jlbb_df, ["Alice"], ["Bob"], jl_source_ranks={}
        )
        assert len(rows) == 1
        assert rows[0]["MDDB"] == 1.0
        assert rows[0]["JLBB"] is None

    def test_only_jlbb_has_player(self):
        """Verify only jlbb has player."""
        mddb_df = pd.DataFrame({"name": ["Bob"], "rank": [2], "pos": ["QB"], "school": ["MIT"]})
        jlbb_df = pd.DataFrame(
            {"name": ["Alice"], "rank": [3], "pos": ["WR"], "school": ["Stanford"]}
        )
        rows = _build_combined_rows(
            ["Alice"], mddb_df, jlbb_df, ["Bob"], ["Alice"], jl_source_ranks={}
        )
        assert len(rows) == 1
        assert rows[0]["MDDB"] is None
        assert rows[0]["JLBB"] == 3.0
        assert rows[0]["School"] == "Stanford"

    def test_neither_source_has_player(self):
        """Verify neither source has player."""
        mddb_df = pd.DataFrame({"name": ["Bob"], "rank": [2], "pos": ["QB"], "school": ["MIT"]})
        jlbb_df = pd.DataFrame(
            {"name": ["Bob"], "rank": [2], "pos": ["WR"], "school": ["Stanford"]}
        )
        rows = _build_combined_rows(
            ["ZZZZZ"], mddb_df, jlbb_df, ["Bob"], ["Bob"], jl_source_ranks={}
        )
        assert len(rows) == 1
        assert rows[0]["MDDB"] is None
        assert rows[0]["JLBB"] is None

    def test_school_falls_back_to_jlbb(self):
        """Verify JLBB school is used when MDDB has no match."""
        mddb_df = pd.DataFrame({"name": ["Bob"], "rank": [2], "pos": ["QB"], "school": ["MIT"]})
        jlbb_df = pd.DataFrame(
            {"name": ["Alice"], "rank": [1], "pos": ["WR"], "school": ["Stanford"]}
        )
        rows = _build_combined_rows(
            ["Alice"], mddb_df, jlbb_df, ["Bob"], ["Alice"], jl_source_ranks={}
        )
        assert rows[0]["School"] == "Stanford"

    def test_weighted_consensus_both_sources(self):
        """Verify weighted consensus when both MDDB and JL sources are present.

        With MDDB_WEIGHT=6, 3 JL sources, MDDB=1:
        consensus = (jl_avg * 3 + 1 * 6) / (3 + 6)
        jl_avg = (2 + 4 + 6) / 3 = 4.0
        consensus = (4.0 * 3 + 1 * 6) / 9 = (12 + 6) / 9 = 2.0
        """
        mddb_df = pd.DataFrame({"name": ["Alice"], "rank": [1], "pos": ["QB"], "school": ["MIT"]})
        jlbb_df = pd.DataFrame({"name": ["Alice"], "rank": [3], "pos": ["QB"], "school": ["MIT"]})
        jl_source_ranks = {"Alice": [2.0, 4.0, 6.0]}
        rows = _build_combined_rows(
            ["Alice"], mddb_df, jlbb_df, ["Alice"], ["Alice"], jl_source_ranks=jl_source_ranks
        )
        assert rows[0]["MDDB"] == 1.0
        assert rows[0]["JLBB"] == 3.0
        assert rows[0]["JL_Avg"] == pytest.approx(4.0)
        assert rows[0]["JL_Sources"] == 3
        # consensus = (4.0 * 3 + 1 * 6) / (3 + 6) = 18/9 = 2.0
        assert rows[0]["Consensus"] == pytest.approx(2.0)
        assert rows[0]["Sources"] == 3 + MDDB_WEIGHT

    def test_weighted_consensus_only_jl(self):
        """Verify consensus equals JL avg when only JL sources are present."""
        mddb_df = pd.DataFrame({"name": ["Bob"], "rank": [2], "pos": ["QB"], "school": ["MIT"]})
        jlbb_df = pd.DataFrame(
            {"name": ["Alice"], "rank": [1], "pos": ["WR"], "school": ["Stanford"]}
        )
        jl_source_ranks = {"Alice": [1.0, 3.0]}
        rows = _build_combined_rows(
            ["Alice"], mddb_df, jlbb_df, ["Bob"], ["Alice"], jl_source_ranks=jl_source_ranks
        )
        assert rows[0]["JL_Avg"] == pytest.approx(2.0)
        assert rows[0]["JL_Sources"] == 2
        assert rows[0]["Consensus"] == pytest.approx(2.0)
        assert rows[0]["Sources"] == 2

    def test_weighted_consensus_only_mddb(self):
        """Verify consensus equals MDDB rank when only MDDB is present."""
        mddb_df = pd.DataFrame({"name": ["Alice"], "rank": [5], "pos": ["QB"], "school": ["MIT"]})
        jlbb_df = pd.DataFrame(
            {"name": ["Bob"], "rank": [2], "pos": ["WR"], "school": ["Stanford"]}
        )
        rows = _build_combined_rows(
            ["Alice"], mddb_df, jlbb_df, ["Alice"], ["Bob"], jl_source_ranks={}
        )
        assert rows[0]["Consensus"] == pytest.approx(5.0)
        assert rows[0]["Sources"] == MDDB_WEIGHT
        assert rows[0]["JL_Avg"] is None
        assert rows[0]["JL_Sources"] is None

    def test_weighted_consensus_neither_source(self):
        """Verify consensus is None when neither source has the player."""
        mddb_df = pd.DataFrame({"name": ["Bob"], "rank": [2], "pos": ["QB"], "school": ["MIT"]})
        jlbb_df = pd.DataFrame(
            {"name": ["Bob"], "rank": [2], "pos": ["WR"], "school": ["Stanford"]}
        )
        rows = _build_combined_rows(
            ["ZZZZZ"], mddb_df, jlbb_df, ["Bob"], ["Bob"], jl_source_ranks={}
        )
        assert rows[0]["Consensus"] is None
        assert rows[0]["Sources"] is None

    def test_jl_sd_populated(self):
        """Verify JL_SD is computed from JL source ranks."""
        mddb_df = pd.DataFrame({"name": ["Alice"], "rank": [1], "pos": ["QB"], "school": ["MIT"]})
        jlbb_df = pd.DataFrame({"name": ["Alice"], "rank": [1], "pos": ["QB"], "school": ["MIT"]})
        # All same rank → sd=0
        jl_source_ranks = {"Alice": [3.0, 3.0, 3.0]}
        rows = _build_combined_rows(
            ["Alice"], mddb_df, jlbb_df, ["Alice"], ["Alice"], jl_source_ranks=jl_source_ranks
        )
        assert rows[0]["JL_SD"] == pytest.approx(0.0)


class TestExtractJlSourceRanks:
    """Tests for _extract_jl_source_ranks."""

    def test_extracts_numeric_source_ranks(self):
        """Verify numeric source ranks are collected, NaN values excluded."""
        jl_df = pd.DataFrame(
            {
                "name": ["Alice", "Bob"],
                "rank": [1, 2],
                "pos": ["QB", "WR"],
                "school": ["MIT", "Stanford"],
                "conference": ["SEC", "Pac 12"],
                "avg": [2.0, 5.0],
                "sd": [1.0, 2.0],
                "ESPN": [1.0, 3.0],
                "PFF": [3.0, float("nan")],
                "CBS": [float("nan"), 7.0],
            }
        )
        result = _extract_jl_source_ranks(jl_df)
        assert result["Alice"] == [1.0, 3.0]
        assert result["Bob"] == [3.0, 7.0]

    def test_empty_dataframe(self):
        """Verify empty DataFrame returns empty dict."""
        jl_df = pd.DataFrame(columns=["name", "rank", "pos", "school"])
        result = _extract_jl_source_ranks(jl_df)
        assert result == {}

    def test_no_source_columns(self):
        """Verify DataFrame with only structural columns returns empty lists."""
        jl_df = pd.DataFrame({"name": ["Alice"], "rank": [1], "pos": ["QB"], "school": ["MIT"]})
        result = _extract_jl_source_ranks(jl_df)
        assert result["Alice"] == []

    def test_all_nan_sources(self):
        """Verify player with all-NaN source ranks gets empty list."""
        jl_df = pd.DataFrame(
            {
                "name": ["Alice"],
                "rank": [1],
                "pos": ["QB"],
                "school": ["MIT"],
                "ESPN": [float("nan")],
                "PFF": [float("nan")],
            }
        )
        result = _extract_jl_source_ranks(jl_df)
        assert result["Alice"] == []
