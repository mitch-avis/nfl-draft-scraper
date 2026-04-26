"""Tests for nfl_draft_scraper.mddb_bb_scraper."""

import pytest

from nfl_draft_scraper.mddb_bb_scraper import _verify_year, parse_big_board


def _build_card(
    *,
    rank: str | None,
    name: str | None,
    pos: str = "QB",
    school: str | None = "School",
    year: int = 2026,
) -> str:
    """Build the HTML for a single prospect card matching the MDDB layout."""
    parts: list[str] = []
    if rank is not None:
        parts.append(
            f'<span class="text-lg sm:text-2xl font-black leading-none" '
            f'style="color: #111827;">{rank}</span>'
        )
    if name is not None:
        parts.append(
            f'<a class="text-base sm:text-xl font-bold text-gray-900 hover:text-blue-600 block" '
            f'href="/players/{year}/slug">\n  {name}\n</a>'
        )
    parts.append(
        f'<span class="text-xs font-bold text-blue-700 bg-blue-50 px-1.5 py-0.5 rounded">'
        f"{pos}</span>"
    )
    if school is not None:
        parts.append(
            f'<a class="text-sm text-gray-500 truncate hover:text-blue-600" '
            f'href="/colleges/{year}/slug">\n  {school}\n</a>'
        )
    return "\n".join(parts)


def _build_page(cards: list[str], *, year: int = 2026) -> str:
    """Wrap one or more prospect cards in a minimal big-board page."""
    body = "\n".join(cards)
    marker = f'<a href="/big-boards/{year}/consensus-big-board-{year}">link</a>'
    return f"<html><body>{marker}\n{body}</body></html>"


class TestParseBigBoard:
    """Tests for parse_big_board."""

    def test_parses_valid_cards(self):
        """Verify parses a multi-prospect page into ordered records."""
        cards = [
            _build_card(rank="1", name="Travis Hunter", pos="CB", school="Colorado"),
            _build_card(rank="2", name="Shedeur Sanders", pos="QB", school="Colorado"),
        ]
        result = parse_big_board(_build_page(cards))
        assert len(result) == 2
        assert result[0] == {
            "rank": "1",
            "name": "Travis Hunter",
            "pos": "CB",
            "school": "Colorado",
        }
        assert result[1]["rank"] == "2"
        assert result[1]["name"] == "Shedeur Sanders"

    def test_empty_html(self):
        """Verify empty html returns empty list."""
        result = parse_big_board("<html><body></body></html>")
        assert result == []

    def test_missing_school_returns_empty_string(self):
        """Verify a card with no college anchor produces an empty school string."""
        page = _build_page([_build_card(rank="1", name="John Doe", pos="QB", school=None)])
        result = parse_big_board(page)
        assert len(result) == 1
        assert result[0]["school"] == ""

    def test_missing_rank_skips_row(self):
        """Verify a card with no rank span is skipped."""
        page = _build_page([_build_card(rank=None, name="Nameless", pos="QB", school="S")])
        result = parse_big_board(page)
        assert result == []


class TestVerifyYear:
    """Tests for _verify_year."""

    def test_passes_when_marker_present(self):
        """Verify no error when the canonical big-board path is present."""
        _verify_year(_build_page([], year=2020), 2020)

    def test_raises_on_missing_marker(self):
        """Verify ValueError when the canonical big-board path is absent."""
        with pytest.raises(ValueError, match="Year marker not found"):
            _verify_year("<html></html>", 2020)

    def test_raises_on_year_mismatch(self):
        """Verify ValueError when only a different year's marker is present."""
        with pytest.raises(ValueError, match="big-boards/2020"):
            _verify_year(_build_page([], year=2023), 2020)


class TestScrapeYear:
    """Tests for scrape_year."""

    def test_calls_helpers_and_saves(self, monkeypatch):
        """Verify scrape_year fetches HTML, verifies the year, parses, and saves the records."""
        from unittest.mock import MagicMock

        from nfl_draft_scraper import mddb_bb_scraper

        fake_html = _build_page(
            [_build_card(rank="1", name="X", pos="QB", school="S", year=2024)],
            year=2024,
        )
        mock_fetch = MagicMock(return_value=fake_html)
        mock_save = MagicMock()
        monkeypatch.setattr(mddb_bb_scraper, "fetch_html", mock_fetch)
        monkeypatch.setattr(mddb_bb_scraper, "save_csv", mock_save)

        result = mddb_bb_scraper.scrape_year(2024)

        mock_fetch.assert_called_once_with(2024)
        mock_save.assert_called_once()
        assert mock_save.call_args.args[0] == "mddb_big_board_2024.csv"
        assert result[0]["name"] == "X"


class TestMain:
    """Tests for main."""

    def test_iterates_over_year_range_and_sleeps(self, monkeypatch):
        """Verify main calls scrape_year for each year and sleeps between calls."""
        from unittest.mock import MagicMock

        from nfl_draft_scraper import constants, mddb_bb_scraper

        monkeypatch.setattr(constants, "START_YEAR", 2024)
        monkeypatch.setattr(constants, "END_YEAR", 2025)
        mock_scrape = MagicMock()
        mock_sleep = MagicMock()
        monkeypatch.setattr(mddb_bb_scraper, "scrape_year", mock_scrape)
        monkeypatch.setattr(mddb_bb_scraper.time, "sleep", mock_sleep)

        mddb_bb_scraper.main()

        assert mock_scrape.call_args_list == [((2024,),), ((2025,),)]
        assert mock_sleep.call_count == 2
