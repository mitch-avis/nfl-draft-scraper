"""Tests for nfl_draft_scraper.pipeline."""

from unittest.mock import patch

from nfl_draft_scraper.pipeline import (
    STAGES,
    _av_file_exists,
    _build_parser,
    _cleaned_picks_exist,
    _combined_files_exist,
    _jlbb_files_exist,
    _mddb_files_exist,
    _merged_files_exist,
    run_pipeline,
)


class TestFileExistenceChecks:
    """Tests for pipeline file-existence helpers."""

    def test_mddb_files_missing(self, tmp_path, monkeypatch):
        """Verify returns False when MDDB files are missing."""
        monkeypatch.setattr("nfl_draft_scraper.pipeline.constants.DATA_PATH", tmp_path)
        assert _mddb_files_exist() is False

    def test_jlbb_files_missing(self, tmp_path, monkeypatch):
        """Verify returns False when JLBB files are missing."""
        monkeypatch.setattr("nfl_draft_scraper.pipeline.constants.DATA_PATH", tmp_path)
        assert _jlbb_files_exist() is False

    def test_combined_files_missing(self, tmp_path, monkeypatch):
        """Verify returns False when combined files are missing."""
        monkeypatch.setattr("nfl_draft_scraper.pipeline.constants.DATA_PATH", tmp_path)
        assert _combined_files_exist() is False

    def test_cleaned_picks_missing(self, tmp_path, monkeypatch):
        """Verify returns False when cleaned picks are missing."""
        monkeypatch.setattr("nfl_draft_scraper.pipeline.constants.DATA_PATH", tmp_path)
        assert _cleaned_picks_exist() is False

    def test_av_file_missing(self, tmp_path, monkeypatch):
        """Verify returns False when AV file is missing."""
        monkeypatch.setattr("nfl_draft_scraper.pipeline.constants.DATA_PATH", tmp_path)
        assert _av_file_exists() is False

    def test_merged_files_missing(self, tmp_path, monkeypatch):
        """Verify returns False when merged files are missing."""
        monkeypatch.setattr("nfl_draft_scraper.pipeline.constants.DATA_PATH", tmp_path)
        assert _merged_files_exist() is False

    def test_cleaned_picks_exists(self, tmp_path, monkeypatch):
        """Verify returns True when cleaned picks file exists."""
        monkeypatch.setattr("nfl_draft_scraper.pipeline.constants.DATA_PATH", tmp_path)
        (tmp_path / "cleaned_draft_picks.csv").touch()
        assert _cleaned_picks_exist() is True

    def test_av_file_exists(self, tmp_path, monkeypatch):
        """Verify returns True when AV file exists."""
        monkeypatch.setattr("nfl_draft_scraper.pipeline.constants.DATA_PATH", tmp_path)
        (tmp_path / "cleaned_draft_picks_with_av.csv").touch()
        assert _av_file_exists() is True


class TestRunPipeline:
    """Tests for run_pipeline."""

    @patch("nfl_draft_scraper.pipeline._run_scrape_mddb")
    @patch("nfl_draft_scraper.pipeline._mddb_files_exist", return_value=True)
    def test_skips_existing_data(self, mock_check, mock_run):
        """Verify stages are skipped when data already exists."""
        run_pipeline(stages=["scrape-mddb"], force=False)
        mock_run.assert_not_called()

    @patch("nfl_draft_scraper.pipeline._run_scrape_mddb")
    @patch("nfl_draft_scraper.pipeline._mddb_files_exist", return_value=True)
    def test_force_runs_even_if_exists(self, mock_check, mock_run):
        """Verify force flag overrides skip logic."""
        run_pipeline(stages=["scrape-mddb"], force=True)
        mock_run.assert_called_once()

    @patch("nfl_draft_scraper.pipeline._run_scrape_mddb")
    @patch("nfl_draft_scraper.pipeline._mddb_files_exist", return_value=False)
    def test_runs_when_missing(self, mock_check, mock_run):
        """Verify stages run when data is missing."""
        run_pipeline(stages=["scrape-mddb"], force=False)
        mock_run.assert_called_once()

    def test_unknown_stage_does_not_raise(self):
        """Verify unknown stage names are logged but not fatal."""
        run_pipeline(stages=["nonexistent-stage"], force=False)


class TestBuildParser:
    """Tests for _build_parser."""

    def test_default_args(self):
        """Verify default arguments."""
        parser = _build_parser()
        args = parser.parse_args([])
        assert args.stages == []
        assert args.force is False

    def test_force_flag(self):
        """Verify force flag is parsed."""
        parser = _build_parser()
        args = parser.parse_args(["--force"])
        assert args.force is True

    def test_stages_argument(self):
        """Verify stage names are parsed."""
        parser = _build_parser()
        args = parser.parse_args(["scrape-mddb", "combine"])
        assert args.stages == ["scrape-mddb", "combine"]


class TestStagesConstant:
    """Tests for the STAGES tuple."""

    def test_stages_order(self):
        """Verify stages are in the expected dependency order."""
        assert STAGES == (
            "scrape-mddb",
            "scrape-jlbb",
            "combine",
            "clean-picks",
            "scrape-av",
            "merge",
        )
