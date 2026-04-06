"""Tests for nfl_draft_scraper.scrape_av."""

import typing

import pandas as pd
import polars as pl

from nfl_draft_scraper.scrape_av import (
    FRANCHISE_EQUIVALENTS,
    _calculate_career_av,
    _calculate_weighted_career_av,
    _clean_stats_df,
    _get_at_index,
    _get_av_by_year,
    _get_draft_team_av_by_year,
    _handle_av_error,
)


class TestGetAtIndex:
    """Tests for _get_at_index."""

    def test_scalar_index(self):
        """Verify scalar index."""
        df = pd.DataFrame({"a": [1, 2]})
        assert _get_at_index(df, 0) == 0

    def test_tuple_index_non_multi(self):
        """Verify tuple index non multi."""
        df = pd.DataFrame({"a": [1, 2]})
        assert _get_at_index(df, (0,)) == 0

    def test_multi_index(self):
        """Verify multi index."""
        df = pd.DataFrame({"a": [1, 2], "b": [3, 4]}).set_index(["a", "b"])
        idx = typing.cast(typing.Hashable, df.index[0])
        result = _get_at_index(df, idx)
        assert result == idx


class TestCleanStatsDf:
    """Tests for _clean_stats_df."""

    def test_removes_empty_season(self):
        """Verify removes rows with empty season values."""
        df = pl.DataFrame({"season": ["2020", "", "2022"], "av": [1, 2, 3]})
        result = _clean_stats_df(df)
        assert result.height == 2
        assert "season" in result.columns

    def test_removes_null_season(self):
        """Verify removes rows with null season values."""
        df = pl.DataFrame({"season": ["2020", None, "2022"], "av": [1, 2, 3]})
        result = _clean_stats_df(df)
        assert result.height == 2

    def test_preserves_valid_seasons(self):
        """Verify keeps rows with valid season values."""
        df = pl.DataFrame({"season": ["2020", "Career"], "av": [1, 10]})
        result = _clean_stats_df(df)
        assert result.height == 2


class TestGetAvByYear:
    """Tests for _get_av_by_year."""

    def test_basic(self):
        """Verify basic."""
        years_df = pl.DataFrame(
            {"season": ["2020", "2021", "2022"], "approximate_value": [5, 3, None]}
        )
        result = _get_av_by_year(years_df, [2020, 2021, 2022])
        assert result == {"2020": 5, "2021": 3, "2022": 0}

    def test_ignores_non_digit_season(self):
        """Verify ignores non digit season."""
        years_df = pl.DataFrame({"season": ["2020", "Career"], "approximate_value": [5, 10]})
        result = _get_av_by_year(years_df, [2020])
        assert result == {"2020": 5}

    def test_ignores_years_outside_range(self):
        """Verify ignores years outside range."""
        years_df = pl.DataFrame({"season": ["2019", "2020"], "approximate_value": [5, 3]})
        result = _get_av_by_year(years_df, [2020])
        assert result == {"2020": 3}

    def test_empty_df(self):
        """Verify empty df."""
        years_df = pl.DataFrame(
            {
                "season": pl.Series([], dtype=pl.String),
                "approximate_value": pl.Series([], dtype=pl.Int64),
            }
        )
        result = _get_av_by_year(years_df, [2020, 2021])
        assert result == {"2020": 0, "2021": 0}


class TestCalculateCareerAv:
    """Tests for _calculate_career_av."""

    def test_sums_yearly_values(self):
        """Verify career AV is the sum of yearly AVs."""
        av_by_year = {"2020": 5, "2021": 10}
        assert _calculate_career_av(av_by_year) == 15

    def test_empty_dict(self):
        """Verify empty dict returns zero."""
        assert _calculate_career_av({}) == 0

    def test_zeros(self):
        """Verify all-zero values sum to zero."""
        av_by_year = {"2020": 0, "2021": 0}
        assert _calculate_career_av(av_by_year) == 0


class TestCalculateWeightedCareerAv:
    """Tests for _calculate_weighted_career_av."""

    def test_single_year(self):
        """Verify single year."""
        av_by_year = {"2020": 10}
        result = _calculate_weighted_career_av(av_by_year, [2020])
        assert result == 10.0

    def test_multiple_years_descending_weight(self):
        """Verify multiple years descending weight."""
        av_by_year = {"2020": 10, "2021": 10}
        result = _calculate_weighted_career_av(av_by_year, [2020, 2021])
        # sorted descending: [10, 10], weights: [1.0, 0.95]
        assert result == round(10 * 1.0 + 10 * 0.95, 1)

    def test_all_zeros(self):
        """Verify all zeros."""
        av_by_year = {"2020": 0, "2021": 0}
        result = _calculate_weighted_career_av(av_by_year, [2020, 2021])
        assert result == 0.0


class TestHandleAvError:
    """Tests for _handle_av_error."""

    def test_marks_columns_nan(self):
        """Verify marks columns nan."""
        df = pd.DataFrame(
            {
                "2020": [5],
                "2021": [3],
                "career": [8],
                "weighted_career": [7.0],
                "draft_team_career": [6],
                "draft_team_weighted_career": [5.5],
                "av_complete": [True],
            }
        )
        _handle_av_error(df, 0, ["2020", "2021"])
        assert pd.isna(df.at[0, "2020"])
        assert pd.isna(df.at[0, "2021"])
        assert pd.isna(df.at[0, "career"])
        assert pd.isna(df.at[0, "weighted_career"])
        assert pd.isna(df.at[0, "draft_team_career"])
        assert pd.isna(df.at[0, "draft_team_weighted_career"])
        assert not df.at[0, "av_complete"]


class TestFranchiseEquivalents:
    """Tests for FRANCHISE_EQUIVALENTS mapping."""

    def test_sdg_lac_equivalent(self):
        """Verify SDG and LAC map to the same franchise set."""
        assert "SDG" in FRANCHISE_EQUIVALENTS
        assert "LAC" in FRANCHISE_EQUIVALENTS
        assert FRANCHISE_EQUIVALENTS["SDG"] == FRANCHISE_EQUIVALENTS["LAC"]
        assert "SDG" in FRANCHISE_EQUIVALENTS["LAC"]

    def test_oak_lvr_equivalent(self):
        """Verify OAK and LVR map to the same franchise set."""
        assert FRANCHISE_EQUIVALENTS["OAK"] == FRANCHISE_EQUIVALENTS["LVR"]

    def test_stl_lar_equivalent(self):
        """Verify STL and LAR map to the same franchise set."""
        assert FRANCHISE_EQUIVALENTS["STL"] == FRANCHISE_EQUIVALENTS["LAR"]

    def test_unknown_team_not_in_mapping(self):
        """Verify teams with no relocations are not in the mapping."""
        assert "DAL" not in FRANCHISE_EQUIVALENTS


class TestGetDraftTeamAvByYear:
    """Tests for _get_draft_team_av_by_year."""

    def test_basic_same_team_all_years(self):
        """Verify AV is counted when player stays on the drafting team."""
        years_df = pl.DataFrame(
            {
                "season": ["2020", "2021", "2022"],
                "approximate_value": [5, 3, 7],
                "team_abbreviation": ["DAL", "DAL", "DAL"],
            }
        )
        result = _get_draft_team_av_by_year(years_df, "DAL", [2020, 2021, 2022])
        assert result == {"2020": 5, "2021": 3, "2022": 7}

    def test_player_traded_away(self):
        """Verify AV is zero for years on a different team."""
        years_df = pl.DataFrame(
            {
                "season": ["2020", "2021", "2022"],
                "approximate_value": [5, 3, 7],
                "team_abbreviation": ["DAL", "DAL", "NYG"],
            }
        )
        result = _get_draft_team_av_by_year(years_df, "DAL", [2020, 2021, 2022])
        assert result == {"2020": 5, "2021": 3, "2022": 0}

    def test_franchise_relocation_sdg_to_lac(self):
        """Verify AV counts when team relocates (SDG → LAC)."""
        years_df = pl.DataFrame(
            {
                "season": ["2016", "2017", "2018"],
                "approximate_value": [4, 6, 8],
                "team_abbreviation": ["SDG", "LAC", "LAC"],
            }
        )
        result = _get_draft_team_av_by_year(years_df, "SDG", [2016, 2017, 2018])
        assert result == {"2016": 4, "2017": 6, "2018": 8}

    def test_franchise_relocation_reverse_lookup(self):
        """Verify AV counts when draft_team uses the new abbreviation."""
        years_df = pl.DataFrame(
            {
                "season": ["2020", "2021"],
                "approximate_value": [3, 5],
                "team_abbreviation": ["OAK", "LVR"],
            }
        )
        # Draft team recorded as LVR but sportsipy returns OAK for 2020
        result = _get_draft_team_av_by_year(years_df, "LVR", [2020, 2021])
        assert result == {"2020": 3, "2021": 5}

    def test_immediate_trade_zero_av(self):
        """Verify a player traded on draft day earns zero draft-team AV."""
        years_df = pl.DataFrame(
            {
                "season": ["2020", "2021"],
                "approximate_value": [10, 8],
                "team_abbreviation": ["NYJ", "NYJ"],
            }
        )
        result = _get_draft_team_av_by_year(years_df, "IND", [2020, 2021])
        assert result == {"2020": 0, "2021": 0}

    def test_null_team_abbreviation(self):
        """Verify rows with null team_abbreviation are treated as non-matching."""
        years_df = pl.DataFrame(
            {
                "season": ["2020", "2021"],
                "approximate_value": [5, 3],
                "team_abbreviation": [None, "DAL"],
            }
        )
        result = _get_draft_team_av_by_year(years_df, "DAL", [2020, 2021])
        assert result == {"2020": 0, "2021": 3}

    def test_empty_df(self):
        """Verify empty DataFrame returns all zeros."""
        years_df = pl.DataFrame(
            {
                "season": pl.Series([], dtype=pl.String),
                "approximate_value": pl.Series([], dtype=pl.Int64),
                "team_abbreviation": pl.Series([], dtype=pl.String),
            }
        )
        result = _get_draft_team_av_by_year(years_df, "DAL", [2020, 2021])
        assert result == {"2020": 0, "2021": 0}

    def test_multi_team_season_not_counted(self):
        """Verify '2TM' multi-team indicator is not matched to any franchise."""
        years_df = pl.DataFrame(
            {
                "season": ["2020", "2021", "2022"],
                "approximate_value": [5, 6, 3],
                "team_abbreviation": ["CLE", "CLE", "2TM"],
            }
        )
        result = _get_draft_team_av_by_year(years_df, "CLE", [2020, 2021, 2022])
        assert result == {"2020": 5, "2021": 6, "2022": 0}
