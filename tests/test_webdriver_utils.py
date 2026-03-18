"""Tests for nfl_draft_scraper.utils.webdriver_utils."""

from unittest.mock import MagicMock, patch

from nfl_draft_scraper.utils.webdriver_utils import (
    WebdriverThread,
    get_webdriver,
    stop_webdriver,
)


class TestWebdriverThread:
    """Tests for WebdriverThread."""

    def test_init_driver_none(self):
        """Verify init driver none."""
        t = WebdriverThread()
        assert t.driver is None


class TestGetWebdriver:
    """Tests for get_webdriver."""

    @patch("nfl_draft_scraper.utils.webdriver_utils.Firefox")
    def test_creates_driver(self, mock_firefox_cls):
        """Verify creates driver."""
        mock_driver = MagicMock()
        mock_firefox_cls.return_value = mock_driver

        thread = WebdriverThread()
        driver = get_webdriver(thread)

        assert driver is mock_driver
        assert thread.driver is mock_driver
        mock_driver.set_page_load_timeout.assert_called_once_with(30)
        mock_driver.implicitly_wait.assert_called_once_with(30)

    @patch("nfl_draft_scraper.utils.webdriver_utils.Firefox")
    def test_reuses_existing_driver(self, mock_firefox_cls):
        """Verify reuses existing driver."""
        existing = MagicMock()
        thread = WebdriverThread()
        thread.driver = existing

        driver = get_webdriver(thread)
        assert driver is existing
        mock_firefox_cls.assert_not_called()


class TestStopWebdriver:
    """Tests for stop_webdriver."""

    def test_quits_and_clears(self):
        """Verify quits and clears."""
        mock_driver = MagicMock()
        thread = WebdriverThread()
        thread.driver = mock_driver

        stop_webdriver(thread)

        mock_driver.quit.assert_called_once()
        assert thread.driver is None

    def test_noop_when_no_driver(self):
        """Verify noop when no driver."""
        thread = WebdriverThread()
        stop_webdriver(thread)  # Should not raise
