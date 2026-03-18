"""Tests for nfl_draft_scraper.mddb_bb_scraper."""

from nfl_draft_scraper.mddb_bb_scraper import parse_big_board


class TestParseBigBoard:
    """Tests for parse_big_board."""

    def test_parses_valid_html(self):
        """Verify parses valid html."""
        html = """
        <html><body>
        <li class="mock-list-item">
            <div class="pick-number">1</div>
            <div class="player-name">Travis Hunter</div>
            <div class="player-details">CB | <a>Colorado</a></div>
        </li>
        <li class="mock-list-item">
            <div class="pick-number">2</div>
            <div class="player-name">Shedeur Sanders</div>
            <div class="player-details">QB | <a>Colorado</a></div>
        </li>
        </body></html>
        """
        result = parse_big_board(html)
        assert len(result) == 2
        assert result[0]["rank"] == "1"
        assert result[0]["name"] == "Travis Hunter"
        assert result[0]["pos"] == "CB"
        assert result[0]["school"] == "Colorado"
        assert result[1]["rank"] == "2"

    def test_empty_html(self):
        """Verify empty html."""
        result = parse_big_board("<html><body></body></html>")
        assert result == []

    def test_missing_rank_skips_row(self):
        """Verify missing rank skips row."""
        html = """
        <html><body>
        <li class="mock-list-item">
            <div class="player-name">No Rank Player</div>
            <div class="player-details">WR | <a>School</a></div>
        </li>
        </body></html>
        """
        result = parse_big_board(html)
        assert result == []
