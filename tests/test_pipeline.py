"""Tests for nfl_draft_scraper.pipeline."""

from unittest.mock import patch

from nfl_draft_scraper.pipeline import (
    STAGES,
    _av_file_exists,
    _build_parser,
    _cleaned_picks_exist,
    _combined_files_exist,
    _jlbb_files_exist,
    _merged_files_exist,
    _wl_files_exist,
    run_pipeline,
)


class TestFileExistenceChecks:
    """Tests for pipeline file-existence helpers."""

    def test_wl_files_missing(self, tmp_path, monkeypatch):
        """Verify returns False when WL files are missing."""
        monkeypatch.setattr("nfl_draft_scraper.pipeline.constants.DATA_PATH", tmp_path)
        assert _wl_files_exist() is False

    def test_wl_files_returns_true_when_no_year_in_range_has_a_sheet(self, tmp_path, monkeypatch):
        """Verify returns True when no year in range has a configured WL sheet id."""
        monkeypatch.setattr("nfl_draft_scraper.pipeline.constants.DATA_PATH", tmp_path)
        monkeypatch.setattr("nfl_draft_scraper.pipeline.constants.START_YEAR", 2018)
        monkeypatch.setattr("nfl_draft_scraper.pipeline.constants.END_YEAR", 2020)
        monkeypatch.setattr("nfl_draft_scraper.pipeline.constants.WL_SHEET_IDS", {})
        assert _wl_files_exist() is True

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

    @patch("nfl_draft_scraper.pipeline._run_scrape_wl")
    @patch("nfl_draft_scraper.pipeline._wl_files_exist", return_value=True)
    def test_skips_existing_data(self, mock_check, mock_run):
        """Verify stages are skipped when data already exists."""
        run_pipeline(stages=["scrape-wl"], force=False)
        mock_run.assert_not_called()

    @patch("nfl_draft_scraper.pipeline._run_scrape_wl")
    @patch("nfl_draft_scraper.pipeline._wl_files_exist", return_value=True)
    def test_force_runs_even_if_exists(self, mock_check, mock_run):
        """Verify force flag overrides skip logic."""
        run_pipeline(stages=["scrape-wl"], force=True)
        mock_run.assert_called_once()

    @patch("nfl_draft_scraper.pipeline._run_scrape_wl")
    @patch("nfl_draft_scraper.pipeline._wl_files_exist", return_value=False)
    def test_runs_when_missing(self, mock_check, mock_run):
        """Verify stages run when data is missing."""
        run_pipeline(stages=["scrape-wl"], force=False)
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
        args = parser.parse_args(["scrape-wl", "combine"])
        assert args.stages == ["scrape-wl", "combine"]


class TestStagesConstant:
    """Tests for the STAGES tuple."""

    def test_stages_order(self):
        """Verify stages are in the expected dependency order."""
        assert STAGES == (
            "scrape-wl",
            "scrape-jlbb",
            "combine",
            "clean-picks",
            "scrape-av",
            "merge",
        )


class TestRunners:
    """Tests for the per-stage `_run_*` runner helpers."""

    def test_run_scrape_wl_invokes_main(self):
        """Verify _run_scrape_wl calls wl_bb_scraper.main."""
        from nfl_draft_scraper.pipeline import _run_scrape_wl

        with patch("nfl_draft_scraper.wl_bb_scraper.main") as mock_main:
            _run_scrape_wl()
            mock_main.assert_called_once()

    def test_run_scrape_jlbb_invokes_main(self):
        """Verify _run_scrape_jlbb calls jl_bb_scraper.main."""
        from nfl_draft_scraper.pipeline import _run_scrape_jlbb

        with patch("nfl_draft_scraper.jl_bb_scraper.main") as mock_main:
            _run_scrape_jlbb()
            mock_main.assert_called_once()

    def test_run_combine_invokes_main(self):
        """Verify _run_combine calls big_board_combiner.main."""
        from nfl_draft_scraper.pipeline import _run_combine

        with patch("nfl_draft_scraper.big_board_combiner.main") as mock_main:
            _run_combine()
            mock_main.assert_called_once()

    def test_run_clean_picks_invokes_main(self):
        """Verify _run_clean_picks calls draft_picks_cleaner.main."""
        from nfl_draft_scraper.pipeline import _run_clean_picks

        with patch("nfl_draft_scraper.draft_picks_cleaner.main") as mock_main:
            _run_clean_picks()
            mock_main.assert_called_once()

    def test_run_scrape_av_passes_force_flag(self):
        """Verify _run_scrape_av forwards the force flag to update_av."""
        from nfl_draft_scraper.pipeline import _run_scrape_av

        with patch("nfl_draft_scraper.scrape_av.update_av") as mock_update:
            _run_scrape_av(force_av=True)
            mock_update.assert_called_once_with(force=True)

    def test_run_merge_invokes_main(self):
        """Verify _run_merge calls merge_bb_ranks_to_picks.main."""
        from nfl_draft_scraper.pipeline import _run_merge

        with patch("nfl_draft_scraper.merge_bb_ranks_to_picks.main") as mock_main:
            _run_merge()
            mock_main.assert_called_once()


class TestMain:
    """Tests for the pipeline main entry point."""

    def test_main_runs_with_no_args(self, monkeypatch):
        """Verify main parses an empty argv and dispatches to run_pipeline."""
        from nfl_draft_scraper import pipeline

        monkeypatch.setattr("sys.argv", ["pipeline.py"])
        with patch("nfl_draft_scraper.pipeline.run_pipeline") as mock_run:
            pipeline.main()
            mock_run.assert_called_once_with(stages=None, force=False)

    def test_main_forwards_explicit_stages_and_force(self, monkeypatch):
        """Verify main forwards stage names and the --force flag to run_pipeline."""
        from nfl_draft_scraper import pipeline

        monkeypatch.setattr("sys.argv", ["pipeline.py", "scrape-wl", "--force"])
        with patch("nfl_draft_scraper.pipeline.run_pipeline") as mock_run:
            pipeline.main()
            mock_run.assert_called_once_with(stages=["scrape-wl"], force=True)

    def test_main_handles_keyboard_interrupt(self, monkeypatch):
        """Verify main exits with code 130 on Ctrl-C during run_pipeline."""
        import pytest

        from nfl_draft_scraper import pipeline

        monkeypatch.setattr("sys.argv", ["pipeline.py"])
        with (
            patch("nfl_draft_scraper.pipeline.run_pipeline", side_effect=KeyboardInterrupt),
            pytest.raises(SystemExit) as excinfo,
        ):
            pipeline.main()
        assert excinfo.value.code == 130
