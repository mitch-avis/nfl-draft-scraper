"""Tests for nfl_draft_scraper.scrape_av."""

import typing

import pandas as pd

from nfl_draft_scraper.scrape_av import (
    _calculate_career_av,
    _calculate_weighted_career_av,
    _clean_stats_df,
    _get_at_index,
    _get_av_by_year,
    _get_table_id_for_position,
    _handle_av_error,
    _is_nonzero,
    _parse_av_by_year_from_pfr,
    _parse_career_av_from_pfr,
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


class TestGetTableIdForPosition:
    """Tests for _get_table_id_for_position."""

    def test_known_positions(self):
        """Verify known positions."""
        assert _get_table_id_for_position("QB") == "passing"
        assert _get_table_id_for_position("RB") == "rushing_and_receiving"
        assert _get_table_id_for_position("WR") == "rushing_and_receiving"
        assert _get_table_id_for_position("TE") == "rushing_and_receiving"
        assert _get_table_id_for_position("OL") == "games_played"
        assert _get_table_id_for_position("DL") == "defense"
        assert _get_table_id_for_position("LB") == "defense"
        assert _get_table_id_for_position("DB") == "defense"
        assert _get_table_id_for_position("K") == "kicking"
        assert _get_table_id_for_position("P") == "punting"

    def test_unknown_position_defaults(self):
        """Verify unknown position defaults."""
        assert _get_table_id_for_position("XYZ") == "games_played"


class TestCleanStatsDf:
    """Tests for _clean_stats_df."""

    def test_removes_empty_index(self):
        """Verify removes empty index."""
        df = pd.DataFrame({"av": [1, 2, 3]}, index=pd.Index(["2020", "", "2022"], dtype=object))
        result = _clean_stats_df(df)
        # Empty string row is removed
        assert len(result) == 2
        assert "season" in result.columns

    def test_renames_index_to_season(self):
        """Verify renames index to season."""
        df = pd.DataFrame({"av": [1]}, index=["2020"])
        result = _clean_stats_df(df)
        assert "season" in result.columns
        assert result.iloc[0]["season"] == "2020"


class TestGetAvByYear:
    """Tests for _get_av_by_year."""

    def test_basic(self):
        """Verify basic."""
        years_df = pd.DataFrame(
            {"season": ["2020", "2021", "2022"], "approximate_value": [5, 3, None]}
        )
        result = _get_av_by_year(years_df, [2020, 2021, 2022])
        assert result == {"2020": 5, "2021": 3, "2022": 0}

    def test_ignores_non_digit_season(self):
        """Verify ignores non digit season."""
        years_df = pd.DataFrame({"season": ["2020", "Career"], "approximate_value": [5, 10]})
        result = _get_av_by_year(years_df, [2020])
        assert result == {"2020": 5}

    def test_ignores_years_outside_range(self):
        """Verify ignores years outside range."""
        years_df = pd.DataFrame({"season": ["2019", "2020"], "approximate_value": [5, 3]})
        result = _get_av_by_year(years_df, [2020])
        assert result == {"2020": 3}

    def test_empty_df(self):
        """Verify empty df."""
        years_df = pd.DataFrame({"season": [], "approximate_value": []})
        result = _get_av_by_year(years_df, [2020, 2021])
        assert result == {"2020": 0, "2021": 0}


class TestCalculateCareerAv:
    """Tests for _calculate_career_av."""

    def test_career_row_present(self):
        """Verify career row present."""
        career_row = pd.DataFrame({"approximate_value": [15.0]})
        av_by_year = {"2020": 5, "2021": 10}
        assert _calculate_career_av(career_row, av_by_year) == 15

    def test_career_row_empty(self):
        """Verify career row empty."""
        career_row = pd.DataFrame()
        av_by_year = {"2020": 5, "2021": 10}
        assert _calculate_career_av(career_row, av_by_year) == 15

    def test_career_row_with_nan(self):
        """Verify career row with nan."""
        career_row = pd.DataFrame({"approximate_value": [None]})
        av_by_year = {"2020": 5}
        result = _calculate_career_av(career_row, av_by_year)
        assert result == 0  # fillna(0) makes it 0


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


class TestIsNonzero:
    """Tests for _is_nonzero."""

    def test_nonzero_int(self):
        """Verify nonzero int."""
        assert _is_nonzero(5) is True

    def test_zero(self):
        """Verify zero."""
        assert _is_nonzero(0) is False

    def test_none(self):
        """Verify none."""
        assert _is_nonzero(None) is False

    def test_nan(self):
        """Verify nan."""
        assert _is_nonzero(float("nan")) is False

    def test_nonzero_float(self):
        """Verify nonzero float."""
        assert _is_nonzero(3.5) is True

    def test_string_raises_false(self):
        """Verify string raises false."""
        assert _is_nonzero("not a number") is False


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


class TestParseAvByYearFromPfr:
    """Tests for _parse_av_by_year_from_pfr."""

    def _make_table(self, rows_html):
        """Verify make table."""
        from bs4 import BeautifulSoup

        html = f"<table><tbody>{rows_html}</tbody></table>"
        return BeautifulSoup(html, "html.parser").find("table")

    def test_parses_valid_rows(self):
        """Verify parses valid rows."""
        rows = """
        <tr>
            <th data-stat="year_id">2020</th>
            <td data-stat="av">5</td>
        </tr>
        <tr>
            <th data-stat="year_id">2021</th>
            <td data-stat="av">8</td>
        </tr>
        """
        table = self._make_table(rows)
        result = _parse_av_by_year_from_pfr(table, [2020, 2021, 2022])
        assert result == {"2020": 5, "2021": 8, "2022": 0}

    def test_skips_header_rows(self):
        """Verify skips header rows."""
        rows = """
        <tr class="thead">
            <th data-stat="year_id">Year</th>
            <td data-stat="av">AV</td>
        </tr>
        <tr>
            <th data-stat="year_id">2020</th>
            <td data-stat="av">5</td>
        </tr>
        """
        table = self._make_table(rows)
        result = _parse_av_by_year_from_pfr(table, [2020])
        assert result == {"2020": 5}

    def test_ignores_non_digit_year(self):
        """Verify ignores non digit year."""
        rows = """
        <tr>
            <th data-stat="year_id">Career</th>
            <td data-stat="av">10</td>
        </tr>
        """
        table = self._make_table(rows)
        result = _parse_av_by_year_from_pfr(table, [2020])
        assert result == {"2020": 0}


class TestParseCareerAvFromPfr:
    """Tests for _parse_career_av_from_pfr."""

    def test_with_tfoot(self):
        """Verify with tfoot."""
        from bs4 import BeautifulSoup

        html = """
        <table>
            <tbody></tbody>
            <tfoot><tr><td data-stat="av">25</td></tr></tfoot>
        </table>
        """
        table = BeautifulSoup(html, "html.parser").find("table")
        result = _parse_career_av_from_pfr(table, {"2020": 10, "2021": 5})
        assert result == 25

    def test_without_tfoot_sums_yearly(self):
        """Verify without tfoot sums yearly."""
        from bs4 import BeautifulSoup

        html = "<table><tbody></tbody></table>"
        table = BeautifulSoup(html, "html.parser").find("table")
        result = _parse_career_av_from_pfr(table, {"2020": 10, "2021": 5})
        assert result == 15
