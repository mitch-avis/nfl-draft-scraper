"""Tests for nfl_draft_scraper.scrape_av."""

import typing

import pandas as pd
import polars as pl

from nfl_draft_scraper.scrape_av import (
    _calculate_career_av,
    _calculate_weighted_career_av,
    _clean_stats_df,
    _get_at_index,
    _get_av_by_year,
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
                "av_complete": [True],
            }
        )
        _handle_av_error(df, 0, ["2020", "2021"])
        assert pd.isna(df.at[0, "2020"])
        assert pd.isna(df.at[0, "2021"])
        assert pd.isna(df.at[0, "career"])
        assert pd.isna(df.at[0, "weighted_career"])
        assert not df.at[0, "av_complete"]
