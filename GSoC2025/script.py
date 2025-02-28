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
        self.main_url = (
            "https://summerofcode.withgoogle.com/programs/2025/organizations"
        )
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

    def wait_for_org_links(self):
        try:
            WebDriverWait(self.driver, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "a.content"))
            )
            self.logger.info("Organization links loaded successfully.")
        except TimeoutException:
            self.logger.error("Timeout waiting for organization links to load.")
            self.driver.quit()
            sys.exit(1)

    def get_org_urls(self):
        try:
            org_links = self.driver.find_elements(By.CSS_SELECTOR, "a.content")
            org_urls = [
                link.get_attribute("href")
                for link in org_links
                if link.get_attribute("href")
            ]
            self.logger.info(f"Found {len(org_urls)} organizations on current page.")
            return org_urls
        except Exception as e:
            self.logger.error(f"Error finding organization links: {e}")
            self.driver.quit()
            sys.exit(1)

    def getURLs(self):
        lazy_org_urls = []

        # TODO: Make this automatic
        print("Manual pagination, because I am lazy lol.")
        print("Navigate manually through the pages in the browser.")
        print("When a page is loaded, press Enter to capture the links from that page.")
        print("Type 'done' and press Enter when you are finished capturing pages.\n")
        while True:
            user_input = input(
                "Press Enter to capture links on the current page (or type 'done' to finish): "
            )
            if user_input.strip().lower() == "done":
                break
            self.wait_for_org_links()
            org_urls = self.get_org_urls()
            lazy_org_urls.extend(org_urls)
            print(f"Captured {len(org_urls)} links from this page.")
            print(
                "Now navigate manually (if needed) to the next page and then press Enter again.\n"
            )
        print(f"Total organization URLs collected: {len(lazy_org_urls)}")
        return lazy_org_urls

    def scrape_orgs(self, org_urls):
        for url in org_urls:
            self.logger.info(f"Scraping organization: {url}")
            try:
                self.driver.get(url)
                project_title = ""
                description = ""
                ideas_list = ""
                technologies = ""
                topics = ""
                main_description = ""
                contributor_guidance = ""

                # Wait for page load
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "span.title"))
                )

                # Project title
                try:
                    title_element = self.driver.find_element(
                        By.CSS_SELECTOR, "span.title"
                    )
                    project_title = title_element.text.strip()
                except Exception as e:
                    self.logger.warning(f"Project title not found on {url}: {e}")

                # Description
                try:
                    desc_element = self.driver.find_element(
                        By.CSS_SELECTOR,
                        "p[_ngcontent-serverapp-c2231669147]:not([class])",
                    )
                    description = desc_element.text.strip()
                except Exception as e:
                    self.logger.warning(f"Description not found on {url}: {e}")

                # Ideas list
                try:
                    ideas_elements = self.driver.find_elements(
                        By.CSS_SELECTOR,
                        "a.mdc-button.mdc-button--unelevated.mat-mdc-unelevated-button.mat-primary.mat-mdc-button-base",
                    )
                    ideas_list = [
                        elem.get_attribute("href")
                        for elem in ideas_elements
                        if elem.get_attribute("href")
                    ]
                    ideas_list = ", ".join(ideas_list)
                except Exception as e:
                    self.logger.warning(f"Ideas list not found on {url}: {e}")

                # Contributor Guidance link
                try:
                    guidance_section = self.driver.find_element(
                        By.XPATH,
                        "//div[contains(@class, 'title') and contains(text(), 'Contributor Guidance')]"
                        "/following-sibling::div[contains(@class, 'link-wrapper')]//a",
                    )
                    contributor_guidance = guidance_section.get_attribute("href")
                except Exception as e:
                    self.logger.warning(f"Contributor Guidance not found on {url}: {e}")

                # Technologies
                try:
                    tech_element = self.driver.find_element(
                        By.CSS_SELECTOR, ".tech__content"
                    )
                    technologies = tech_element.text.strip()
                except Exception as e:
                    self.logger.warning(f"Technologies not found on {url}: {e}")

                # Topics
                try:
                    topics_element = self.driver.find_element(
                        By.CSS_SELECTOR, ".topics__content"
                    )
                    topics = topics_element.text.strip()
                except Exception as e:
                    self.logger.warning(f"Topics not found on {url}: {e}")

                # Main description
                try:
                    main_desc_element = self.driver.find_element(By.CSS_SELECTOR, ".bd")
                    main_description = main_desc_element.text.strip()
                except Exception as e:
                    self.logger.warning(f"Main description not found on {url}: {e}")

                self.data_list.append(
                    {
                        "project title": project_title,
                        "tagline": description,
                        "description": main_description,
                        "ideas": ideas_list,
                        "technologies": technologies,
                        "topics": topics,
                        "contributor_guidance": contributor_guidance,
                        "url": url,
                    }
                )
            except Exception as e:
                self.logger.error(f"Failed to load organization page {url}: {e}")

    def save_data_to_csv(self):
        try:
            df = pd.DataFrame(self.data_list)
            df.to_csv("scraped_data.csv", index=False, encoding="utf-8")
            self.logger.info("Data saved to scraped_data.csv")
        except Exception as e:
            self.logger.error(f"Error saving to CSV: {e}")

    def close_driver(self):
        if self.driver:
            self.driver.quit()
            self.logger.info("Browser closed.")

    def run(self):
        self.load_main_page()
        lazy_org_urls = self.getURLs()
        self.scrape_orgs(lazy_org_urls)
        self.save_data_to_csv()
        self.close_driver()
        self.logger.info("Scraping complete!")


if __name__ == "__main__":
    scraper = Scraper()
    scraper.run()
