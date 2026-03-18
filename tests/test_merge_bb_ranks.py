"""Tests for nfl_draft_scraper.merge_bb_ranks_to_picks."""

import pandas as pd

from nfl_draft_scraper.merge_bb_ranks_to_picks import (
    _fuzzy_match_player,
    _get_av_columns,
    _get_rank_lists,
)


class TestFuzzyMatchPlayer:
    """Tests for _fuzzy_match_player."""

    def test_exact_match(self):
        """Verify exact match."""
        assert _fuzzy_match_player("Alice", ["Alice", "Bob"]) == "Alice"

    def test_fuzzy_match(self):
        """Verify fuzzy match."""
        result = _fuzzy_match_player("Alic", ["Alice", "Bob"])
        assert result == "Alice"

    def test_no_match(self):
        """Verify no match."""
        assert _fuzzy_match_player("ZZZZZ", ["Alice", "Bob"]) is None

    def test_empty_choices(self):
        """Verify empty choices."""
        assert _fuzzy_match_player("Alice", []) is None

    def test_custom_cutoff(self):
        """Verify custom cutoff."""
        assert _fuzzy_match_player("A", ["Alice"], cutoff=0.99) is None


class TestGetAvColumns:
    """Tests for _get_av_columns."""

    def test_returns_matching_columns(self):
        """Verify returns matching columns."""
        df = pd.DataFrame(
            {"2020": [1], "2021": [2], "career": [3], "weighted_career": [4], "other": [5]}
        )
        result = _get_av_columns(df)
        assert "2020" in result
        assert "2021" in result
        assert "career" in result
        assert "weighted_career" in result
        assert "other" not in result

    def test_returns_empty_for_no_matches(self):
        """Verify returns empty for no matches."""
        df = pd.DataFrame({"col_a": [1], "col_b": [2]})
        result = _get_av_columns(df)
        assert result == []


class TestGetRankLists:
    """Tests for _get_rank_lists."""

    def test_matches_player(self):
        """Verify matches player."""
        picks = pd.DataFrame(
            {
                "pfr_player_name": ["Alice"],
                "pfr_player_name_clean": ["alice"],
                "college": ["MIT"],
            }
        )
        bb_lookup = {"alice": {"MDDB": 1, "JLBB": 3, "AvgRank": 2.0}}
        mddb, jlbb, avg = _get_rank_lists(picks, bb_lookup, ["alice"])
        assert mddb == [1]
        assert jlbb == [3]
        assert avg == [2.0]

    def test_no_match(self):
        """Verify no match."""
        picks = pd.DataFrame(
            {
                "pfr_player_name": ["Zzzz"],
                "pfr_player_name_clean": ["zzzz"],
                "college": ["Nowhere"],
            }
        )
        bb_lookup = {"alice": {"MDDB": 1, "JLBB": 3, "AvgRank": 2.0}}
        mddb, jlbb, avg = _get_rank_lists(picks, bb_lookup, ["alice"])
        assert mddb == [None]
        assert jlbb == [None]
        assert avg == [None]

    def test_multiple_players(self):
        """Verify multiple players."""
        picks = pd.DataFrame(
            {
                "pfr_player_name": ["Alice", "Bob"],
                "pfr_player_name_clean": ["alice", "bob"],
                "college": ["MIT", "MIT"],
            }
        )
        bb_lookup = {
            "alice": {"MDDB": 1, "JLBB": 3, "AvgRank": 2.0},
            "bob": {"MDDB": 5, "JLBB": 7, "AvgRank": 6.0},
        }
        mddb, jlbb, avg = _get_rank_lists(picks, bb_lookup, ["alice", "bob"])
        assert mddb == [1, 5]
        assert jlbb == [3, 7]
        assert avg == [2.0, 6.0]
