"""Module to manage the Selenium WebDriver for web scraping."""

import threading

from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.firefox.service import Service as FirefoxService
from selenium.webdriver.firefox.webdriver import WebDriver as Firefox

from nfl_draft_scraper.utils.logger import log


class WebdriverThread(threading.Thread):
    """Custom Thread subclass that holds a Selenium WebDriver instance."""

    def __init__(self) -> None:
        """Initialize the WebdriverThread."""
        super().__init__()
        self.driver: Firefox | None = None


def get_webdriver(thread: WebdriverThread) -> Firefox:
    """Initialize the Selenium web scraper."""
    if thread.driver is None:
        options = FirefoxOptions()
        service = FirefoxService()
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--no-sandbox")
        options.set_preference("permissions.default.image", 2)  # Disable images
        options.set_preference("permissions.default.stylesheet", 2)  # Disable CSS
        options.set_preference(
            "general.useragent.override",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/58.0.3029.110 Safari/537.3",
        )
        new_driver = Firefox(service=service, options=options)
        new_driver.set_page_load_timeout(30)
        new_driver.implicitly_wait(30)
        thread.driver = new_driver
    log.info("Selenium WebDriver initialized.")
    return thread.driver


def stop_webdriver(thread: WebdriverThread) -> None:
    """Shut down the Selenium web scraper."""
    if thread.driver:
        thread.driver.quit()
        thread.driver = None
        log.info("Selenium WebDriver stopped.")
