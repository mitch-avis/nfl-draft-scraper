"""Jacklich10 Big Board Scraper."""

import random
import time

from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.webdriver import WebDriver as Firefox
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.support.ui import WebDriverWait

from nfl_draft_scraper import constants
from nfl_draft_scraper.utils.csv_utils import save_csv
from nfl_draft_scraper.utils.logger import log
from nfl_draft_scraper.utils.webdriver_utils import WebdriverThread, get_webdriver, stop_webdriver

SLEEP_MIN = 1
SLEEP_MAX = 3
_rng = random.SystemRandom()

BASE_URL = "https://jacklich10.com/bigboard/nfl/"


def fetch_and_parse(driver: Firefox, year: int) -> list:
    """Fetch and parse the big board data for a given year."""
    driver.get(BASE_URL)

    # wait for draft year panel to load
    WebDriverWait(driver, 20).until(
        ec.presence_of_element_located(
            (By.XPATH, "/html/body/div[1]/div/div/div[1]/div/div[1]/form/div[1]/div/div")
        )
    )

    # open year dropdown
    year_dropdown = driver.find_element(
        By.XPATH,
        "/html/body/div[1]/div/div/div[1]/div/div[1]/form/div[1]/div/div/div/div/div/button",
    )
    driver.execute_script("arguments[0].click();", year_dropdown)

    # select the year dynamically (2016 → index 0, 2017 → 1, 2018 → 2, etc.)
    year_selector = WebDriverWait(driver, 5).until(
        ec.element_to_be_clickable((By.ID, f"bs-select-1-{year - 2016}"))
    )
    driver.execute_script("arguments[0].click();", year_selector)

    # click update
    update_button = WebDriverWait(driver, 5).until(ec.element_to_be_clickable((By.ID, "update")))
    driver.execute_script("arguments[0].click();", update_button)

    # wait for table panel
    WebDriverWait(driver, 20).until(
        ec.presence_of_element_located(
            (By.XPATH, "/html/body/div[1]/div/div/div[1]/div/div[2]/div/div/div[2]/div/div[1]")
        )
    )
    time.sleep(2)

    # parse all pages
    out = []
    # base XPath to the container holding rows
    body_xpath = "/html/body/div[1]/div/div/div[1]/div/div[2]/div/div/div[2]/div/div[1]/div[2]"
    while True:
        # find all row‑divs under the body
        rows = driver.find_elements(By.XPATH, f"{body_xpath}/div")
        for i in range(1, len(rows)):
            root = f"{body_xpath}/div[{i}]"
            rank = driver.find_element(By.XPATH, f"{root}/div/div[1]/div").text.strip()
            pos = driver.find_element(
                By.XPATH, f"{root}/div/div[4]/div/div/div[1]/span"
            ).text.strip()
            fname = driver.find_element(
                By.XPATH, f"{root}/div/div[5]/div/div/div[1]/span"
            ).text.strip()
            lname = driver.find_element(
                By.XPATH, f"{root}/div/div[5]/div/div/div[2]/span"
            ).text.strip()
            out.append(
                {
                    "rank": rank,
                    "pos": pos,
                    "name": f"{fname} {lname}",
                }
            )
            log.info("Parsed %s %s %s %s", rank, pos, fname, lname)

        # click "Next" or break if disabled
        next_button = driver.find_element(By.CLASS_NAME, "rt-next-button")
        if not next_button.is_enabled():
            break
        driver.execute_script("arguments[0].click();", next_button)
        time.sleep(_rng.uniform(SLEEP_MIN, SLEEP_MAX))

    return out


def main():
    """Scrape the big board data from Jacklich10's website."""
    thread = WebdriverThread()
    driver = get_webdriver(thread)

    for year in range(constants.START_YEAR, constants.END_YEAR + 1):
        recs = fetch_and_parse(driver, year)
        file_name = f"jlbb_big_board_{year}.csv"
        save_csv(file_name, recs)
        log.info("Saved %s records to %s", len(recs), file_name)
        time.sleep(_rng.uniform(SLEEP_MIN, SLEEP_MAX))

    stop_webdriver(thread)


if __name__ == "__main__":
    main()
