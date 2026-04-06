"""Tests for nfl_draft_scraper.mddb_bb_scraper."""

import html
import json

import pytest

from nfl_draft_scraper.mddb_bb_scraper import _verify_year, parse_big_board


def _build_page(selections: list[dict], *, year: str = "2025") -> str:
    """Build a minimal HTML page with embedded React props."""
    props = {"year": year, "mock": {"selections": selections}}
    escaped = html.escape(json.dumps(props), quote=True)
    return f'<div data-react-props="{escaped}"></div>'


class TestParseBigBoard:
    """Tests for parse_big_board."""

    def test_parses_valid_props(self):
        """Verify parses valid React props JSON."""
        selections = [
            {
                "pick": 1,
                "player": {
                    "name": "Travis Hunter",
                    "position": "CB",
                    "college": {"name": "Colorado"},
                },
            },
            {
                "pick": 2,
                "player": {
                    "name": "Shedeur Sanders",
                    "position": "QB",
                    "college": {"name": "Colorado"},
                },
            },
        ]
        page = _build_page(selections)
        result = parse_big_board(page)
        assert len(result) == 2
        assert result[0]["rank"] == "1"
        assert result[0]["name"] == "Travis Hunter"
        assert result[0]["pos"] == "CB"
        assert result[0]["school"] == "Colorado"
        assert result[1]["rank"] == "2"
        assert result[1]["name"] == "Shedeur Sanders"

    def test_empty_html(self):
        """Verify empty html returns empty list."""
        result = parse_big_board("<html><body></body></html>")
        assert result == []

    def test_missing_name_skips_row(self):
        """Verify selections with missing player name are skipped."""
        selections = [
            {
                "pick": 1,
                "player": {"name": "", "position": "WR", "college": {"name": "School"}},
            },
        ]
        page = _build_page(selections)
        result = parse_big_board(page)
        assert result == []

    def test_missing_college_returns_empty_school(self):
        """Verify a null college object produces an empty school string."""
        selections = [
            {
                "pick": 1,
                "player": {"name": "John Doe", "position": "QB", "college": None},
            },
        ]
        page = _build_page(selections)
        result = parse_big_board(page)
        assert len(result) == 1
        assert result[0]["school"] == ""


class TestVerifyYear:
    """Tests for _verify_year."""

    def test_passes_when_year_matches(self):
        """Verify no error when props year matches expected year."""
        page_html = _build_page([], year="2020")
        _verify_year(page_html, 2020)

    def test_raises_on_year_mismatch(self):
        """Verify ValueError when props year does not match expected year."""
        page_html = _build_page([], year="2023")
        with pytest.raises(ValueError, match="Year mismatch.*expected 2020.*got 2023"):
            _verify_year(page_html, 2020)

    def test_raises_on_missing_props(self):
        """Verify ValueError when no React props are found."""
        with pytest.raises(ValueError, match="No React props found"):
            _verify_year("<html></html>", 2020)

    def test_raises_on_missing_year_key(self):
        """Verify ValueError when React props lack a year field."""
        props = {"mock": {"selections": []}}
        escaped = html.escape(json.dumps(props), quote=True)
        page_html = f'<div data-react-props="{escaped}"></div>'
        with pytest.raises(ValueError, match="Year mismatch.*expected 2020.*got "):
            _verify_year(page_html, 2020)
