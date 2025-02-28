import sys
import signal
import logging
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException


class Scraper:
    def __init__(self, headless=False):
        self.main_url = "https://summerofcode.withgoogle.com/programs/2022/projects"
        self.data_list = []
        self.driver = None
        self.logger = self.setup_logging()
        self.setup_signal_handler()
        self.initialize_driver(headless=headless)

    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            filename="scraper.log",
            filemode="w",
        )
        return logging.getLogger(__name__)

    def setup_signal_handler(self):
        signal.signal(signal.SIGINT, self.signal_handler)

    def signal_handler(self, sig, frame):
        self.logger.warning("Interrupt received. Saving data and exiting...")
        self.save_data_to_csv()
        if self.driver:
            self.driver.quit()
        sys.exit(0)

    def initialize_driver(self, headless=False):
        try:
            options = webdriver.ChromeOptions()
            if headless:
                options.add_argument("--headless")
            self.driver = webdriver.Chrome(options=options)
            self.logger.info("Chrome driver initialized successfully.")
        except Exception as e:
            self.logger.error(f"Failed to initialize Chrome driver: {e}")
            sys.exit(1)

    def load_main_page(self):
        try:
            self.driver.get(self.main_url)
            self.logger.info(f"Navigated to {self.main_url}")
        except Exception as e:
            self.logger.error(f"Failed to load main page: {e}")
            self.driver.quit()
            sys.exit(1)

    def wait_for_project_links(self):
        try:
            WebDriverWait(self.driver, 20).until(
                EC.presence_of_element_located(
                    (
                        By.CSS_SELECTOR,
                        "a.mdc-button.mdc-button--unelevated.mat-mdc-unelevated-button.mat-primary.mat-mdc-button-base",
                    )
                )
            )
            self.logger.info("Project links loaded successfully.")
        except TimeoutException:
            self.logger.error("Timeout waiting for project links to load.")
            self.driver.quit()
            sys.exit(1)

    def get_project_urls(self):
        try:
            project_links = self.driver.find_elements(
                By.CSS_SELECTOR,
                "a.mdc-button.mdc-button--unelevated.mat-mdc-unelevated-button.mat-primary.mat-mdc-button-base",
            )
            project_urls = [
                link.get_attribute("href")
                for link in project_links
                if link.get_attribute("href")
            ]
            self.logger.info(f"Found {len(project_urls)} project URLs on current page.")
            return project_urls
        except Exception as e:
            self.logger.error(f"Error finding project URLs: {e}")
            self.driver.quit()
            sys.exit(1)

    def getURLs(self):
        project_urls = []
        print("Manual pagination for collecting project URLs.")
        print("Navigate manually through the pages in the browser.")
        print(
            "When a page is loaded, press Enter to capture the project links from that page."
        )
        print("Type 'done' and press Enter when you are finished capturing pages.\n")
        while True:
            user_input = input(
                "Press Enter to capture project links on the current page (or type 'done' to finish): "
            )
            if user_input.strip().lower() == "done":
                break
            self.wait_for_project_links()
            page_urls = self.get_project_urls()
            project_urls.extend(page_urls)
            print(f"Captured {len(page_urls)} project URLs from this page.")
            print(
                "Now navigate manually (if needed) to the next page and then press Enter again.\n"
            )
        print(f"Total project URLs collected: {len(project_urls)}")
        return project_urls

    def scrape_projects(self, project_urls):
        for url in project_urls:
            self.logger.info(f"Scraping project: {url}")
            try:
                self.driver.get(url)
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "dl.h-list"))
                )

                title = ""
                contributor_name = ""
                code_link = ""
                mentors = ""
                organization = ""
                technologies = ""
                topics = ""
                project_details = ""

                try:
                    title_element = self.driver.find_element(
                        By.CSS_SELECTOR, ".text--hdg-3"
                    )
                    title = title_element.text.strip()
                except Exception as e:
                    self.logger.warning(f"Title not found on {url}: {e}")

                try:
                    contributor_element = self.driver.find_element(
                        By.CSS_SELECTOR, ".contributor__name"
                    )
                    contributor_name = contributor_element.text.strip()
                except Exception as e:
                    self.logger.warning(f"Contributor name not found on {url}: {e}")

                try:
                    code_link_element = self.driver.find_element(
                        By.CSS_SELECTOR, "a.mdc-button--outlined.mat-primary"
                    )
                    code_link = code_link_element.get_attribute("href")
                except Exception as e:
                    self.logger.warning(f"Code link not found on {url}: {e}")

                try:
                    dl_elements = self.driver.find_elements(
                        By.CSS_SELECTOR, "dl.h-list"
                    )
                    if len(dl_elements) > 1:
                        dl_element = dl_elements[1]
                        items = dl_element.find_elements(
                            By.CSS_SELECTOR, "div.h-list__item"
                        )
                        for item in items:
                            dt_text = (
                                item.find_element(By.TAG_NAME, "dt")
                                .text.strip()
                                .lower()
                            )
                            dd_text = item.find_element(By.TAG_NAME, "dd").text.strip()
                            if "mentors" in dt_text:
                                mentors = dd_text
                            elif "organization" in dt_text:
                                organization = dd_text
                            elif "technologies" in dt_text:
                                technologies = dd_text
                            elif "topics" in dt_text:
                                topics = dd_text
                except Exception as e:
                    self.logger.warning(f"Error extracting dl items on {url}: {e}")

                try:
                    details_element = self.driver.find_element(
                        By.CLASS_NAME, "project-details-content"
                    )
                    project_details = details_element.text.strip()
                except Exception as e:
                    self.logger.warning(f"Project details not found on {url}: {e}")

                self.data_list.append(
                    {
                        "title": title,
                        "contributor_name": contributor_name,
                        "code_link": code_link,
                        "mentors": mentors,
                        "organization": organization,
                        "technologies": technologies,
                        "topics": topics,
                        "project_details": project_details,
                        "url": url,
                    }
                )

            except Exception as e:
                self.logger.error(f"Failed to scrape project {url}: {e}")

    def save_data_to_csv(self):
        try:
            df = pd.DataFrame(self.data_list)
            df.to_csv("GSoC-2022-projects.csv", index=False, encoding="utf-8")
            self.logger.info("Data saved to scraped_projects.csv")
        except Exception as e:
            self.logger.error(f"Error saving to CSV: {e}")

    def close_driver(self):
        if self.driver:
            self.driver.quit()
            self.logger.info("Browser closed.")

    def run(self):
        self.load_main_page()
        project_urls = self.getURLs()
        self.scrape_projects(project_urls)
        self.save_data_to_csv()
        self.close_driver()
        self.logger.info("Scraping complete!")


if __name__ == "__main__":
    scraper = Scraper()
    scraper.run()
