"""Tests for nfl_draft_scraper.big_board_combiner."""

import pandas as pd

from nfl_draft_scraper.big_board_combiner import (
    _best_match,
    _build_combined_rows,
    _clean_df,
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
        jlbb_df = pd.DataFrame({"name": ["Alice"], "rank": [3], "pos": ["QB"]})
        rows = _build_combined_rows(["Alice"], mddb_df, jlbb_df, ["Alice"], ["Alice"])
        assert len(rows) == 1
        assert rows[0]["MDDB"] == 1.0
        assert rows[0]["JLBB"] == 3.0
        assert rows[0]["AvgRank"] == 2.0

    def test_only_mddb_has_player(self):
        """Verify only mddb has player."""
        mddb_df = pd.DataFrame({"name": ["Alice"], "rank": [1], "pos": ["QB"], "school": ["MIT"]})
        jlbb_df = pd.DataFrame({"name": ["Bob"], "rank": [2], "pos": ["WR"]})
        rows = _build_combined_rows(["Alice"], mddb_df, jlbb_df, ["Alice"], ["Bob"])
        assert len(rows) == 1
        assert rows[0]["MDDB"] == 1.0
        assert rows[0]["JLBB"] is None
        assert rows[0]["AvgRank"] == 1.0

    def test_only_jlbb_has_player(self):
        """Verify only jlbb has player."""
        mddb_df = pd.DataFrame({"name": ["Bob"], "rank": [2], "pos": ["QB"], "school": ["MIT"]})
        jlbb_df = pd.DataFrame({"name": ["Alice"], "rank": [3], "pos": ["WR"]})
        rows = _build_combined_rows(["Alice"], mddb_df, jlbb_df, ["Bob"], ["Alice"])
        assert len(rows) == 1
        assert rows[0]["MDDB"] is None
        assert rows[0]["JLBB"] == 3.0
        assert rows[0]["AvgRank"] == 3.0

    def test_neither_source_has_player(self):
        """Verify neither source has player."""
        mddb_df = pd.DataFrame({"name": ["Bob"], "rank": [2], "pos": ["QB"], "school": ["MIT"]})
        jlbb_df = pd.DataFrame({"name": ["Bob"], "rank": [2], "pos": ["WR"]})
        rows = _build_combined_rows(["ZZZZZ"], mddb_df, jlbb_df, ["Bob"], ["Bob"])
        assert len(rows) == 1
        assert rows[0]["MDDB"] is None
        assert rows[0]["JLBB"] is None
        assert rows[0]["AvgRank"] is None
