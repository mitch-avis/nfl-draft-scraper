"""Tests for nfl_draft_scraper.utils.csv_utils — read_write_data."""

import pandas as pd

from nfl_draft_scraper.utils.csv_utils import read_write_data


class TestReadWriteData:
    """Tests for read_write_data."""

    def test_generates_data_when_file_missing(self, tmp_path, monkeypatch):
        """Verify generates data when file missing."""
        monkeypatch.setattr("nfl_draft_scraper.utils.csv_utils.constants.DATA_PATH", tmp_path)

        def generate():
            """Verify generate."""
            return [{"a": 1, "b": 2}, {"a": 3, "b": 4}]

        result = read_write_data("test_data", generate)
        assert len(result) == 2
        assert (tmp_path / "test_data.csv").exists()

    def test_reads_existing_data(self, tmp_path, monkeypatch):
        """Verify reads existing data."""
        monkeypatch.setattr("nfl_draft_scraper.utils.csv_utils.constants.DATA_PATH", tmp_path)

        df = pd.DataFrame({"a": [10, 20]})
        df.to_csv(tmp_path / "existing.csv", index=True)

        call_count = 0

        def should_not_be_called():
            """Verify should not be called."""
            nonlocal call_count
            call_count += 1
            return [{"a": 99}]

        result = read_write_data("existing", should_not_be_called)
        assert call_count == 0
        assert len(result) == 2

    def test_force_refresh_regenerates(self, tmp_path, monkeypatch):
        """Verify force refresh regenerates."""
        monkeypatch.setattr("nfl_draft_scraper.utils.csv_utils.constants.DATA_PATH", tmp_path)

        df = pd.DataFrame({"a": [10, 20]})
        df.to_csv(tmp_path / "refresh.csv", index=True)

        def generate():
            """Verify generate."""
            return [{"a": 99}]

        result = read_write_data("refresh", generate, force_refresh=True)
        assert len(result) == 1
        assert result.iloc[0]["a"] == 99
