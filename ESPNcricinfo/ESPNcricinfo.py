import requests
import datetime
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from threading import Lock
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from cachetools import TTLCache
import concurrent.futures
import argparse

BASE_URL = "https://stats.espncricinfo.com/ci/engine/stats/index.html"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
}


TEAM_CODES = {
    "Afghanistan": "40",
    "Australia": "2",
    "Bangladesh": "25",
    "Canada": "17",
    "England": "1",
    "India": "6",
    "Ireland": "29",
    "Jersey": "4083",
    "Kenya": "26",
    "Namibia": "28",
    "Netherlands": "15",
    "New Zealand": "5",
    "Pakistan": "7",
    "South Africa": "3",
    "Sri Lanka": "8",
    "West Indies": "4",
    "Zimbabwe": "9",
}


class ParallelScrapper:
    def __init__(
        self,
        team_codes=TEAM_CODES,
        base_url=BASE_URL,
        headers=HEADERS,
        max_workers=16,
        rate_limit=0.1,
        cache_ttl=3600,
        chunk_size=50,
    ):
        self.team_codes = team_codes
        self.base_url = base_url
        self.headers = headers
        self.max_workers = max_workers
        self.rate_limit = rate_limit
        self.last_request_time = 0
        self.total_downloaded_bytes = 0
        self.download_lock = Lock()
        self.downloaded_bytes = {
            "batting": 0,
            "bowling": 0,
            "fielding": 0,
        }
        self.format_mapping = {
            "1": "Test",
            "2": "ODI",
            "3": "T20I",
            "6": "Twenty20",
            "8": "Womens Test",
            "9": "Womens ODI",
            "23": "Womens T20",
        }

        self.cache = TTLCache(maxsize=1000, ttl=cache_ttl)

        self.session = requests.Session()
        retries = Retry(
            total=5,
            backoff_factor=0.1,
            status_forcelist=[500, 502, 503, 504, 429],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(
            max_retries=retries,
            pool_connections=max_workers * 2,
            pool_maxsize=max_workers * 4,
        )
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        self.session.headers.update(headers)
        self.chunk_size = chunk_size

    def generate_time_spans(self, start_year, end_year):
        """Generate a list of (start_date, end_date) tuples for each month between start_year and end_year.
        For the current month, the end date is set to yesterday's date."""
        current_date = datetime.datetime.now()
        today = current_date.date()
        current_year = current_date.year
        current_month = current_date.month
        time_spans = []

        for year in range(start_year, end_year + 1):
            if year == end_year:
                months = current_month if end_year == current_year else 12
            else:
                months = 12

            for month in range(1, months + 1):
                try:
                    start_date = datetime.date(year, month, 1)
                    if year == current_year and month == current_month:
                        end_date = today - datetime.timedelta(days=1)
                    else:
                        if month == 12:
                            end_date = datetime.date(year + 1, 1, 1)
                        else:
                            end_date = datetime.date(year, month + 1, 1)
                    time_spans.append(
                        (start_date.strftime("%d+%b+%Y"), end_date.strftime("%d+%b+%Y"))
                    )
                except Exception as e:
                    print(f"Error generating dates for {year}-{month}: {e}")
        return time_spans

    def extract_player_data(self, html):
        """Extract player data using lxml parser for better performance."""
        soup = BeautifulSoup(html, "lxml")

        def caption_match(tag_text):
            return tag_text and "overall figures" in tag_text.lower()

        target_table = soup.find(
            "table",
            class_="engineTable",
            caption=lambda x: x and caption_match(x.get_text(strip=True)),
        )

        if not target_table:
            return None

        try:
            thead = target_table.find("thead")
            tbody = target_table.find("tbody")
            if not thead or not tbody:
                return None

            headers = [th.get_text(strip=True) for th in thead.find_all("th")]
            rows = []
            for row in tbody.find_all("tr", class_="data1"):
                cells = [td.get_text(strip=True) for td in row.find_all("td")]
                rows.append(cells)

            if not rows:
                return None

            return pd.DataFrame(rows, columns=headers)
        except Exception as e:
            print(f"Error parsing table: {e}")
            return None

    def _format_bytes(self, num_bytes):
        """Format bytes into a human-readable string (B, KB, or MB)."""
        if num_bytes < 1024:
            return f"{num_bytes} B"
        elif num_bytes < 1024 * 1024:
            return f"{num_bytes / 1024:.2f} KB"
        else:
            return f"{num_bytes / (1024 * 1024):.2f} MB"

    def fetch_data(
        self, player_type, params, team_name, format_name, start_date, end_date
    ):
        """Optimized fetch with better error handling and caching"""
        cache_key = f"{player_type}_{team_name}_{format_name}_{start_date}_{end_date}"

        try:
            if cache_key in self.cache:
                return self.cache[cache_key]

            with self.download_lock:
                current_time = time.time()
                elapsed = current_time - self.last_request_time
                if elapsed < self.rate_limit:
                    sleep_time = min(self.rate_limit - elapsed, 1.0)
                    time.sleep(sleep_time)
                self.last_request_time = time.time()

            response = self.session.get(
                self.base_url,
                params=dict(params),
                timeout=(5, 15),
            )
            response.raise_for_status()

            content_length = len(response.content)
            with self.download_lock:
                self.downloaded_bytes[player_type] += content_length
                self.total_downloaded_bytes += content_length

            data = self.extract_player_data(response.text)

            if data is not None and not data.empty:
                data["Team"] = team_name
                data["Start Date"] = start_date
                data["End Date"] = end_date
                data["Format"] = format_name

                self.cache[cache_key] = data
                return data

            return None

        except requests.exceptions.RequestException as re:
            if re.response is not None and re.response.status_code == 429:
                time.sleep(5)
                return self.fetch_data(
                    player_type, params, team_name, format_name, start_date, end_date
                )
            print(f"Request error: {re}")
            return None
        except Exception as e:
            print(f"Unexpected error: {e}")
            return None

    def scrape_player_data(self, player_type, time_spans):
        """Optimized version with chunk processing and improved error handling"""
        all_tasks = []

        for start_date, end_date in time_spans:
            for class_code, format_name in self.format_mapping.items():
                for team_name, team_code in self.team_codes.items():
                    params = [
                        ("class", class_code),
                        ("filter", "advanced"),
                        (
                            "orderby",
                            (
                                "runs"
                                if player_type == "batting"
                                else ("wickets" if player_type == "bowling" else "dis")
                            ),
                        ),
                        ("spanmin1", start_date),
                        ("spanmax1", end_date),
                        ("spanval1", "span"),
                        ("team", team_code),
                        ("template", "results"),
                        ("type", player_type),
                    ]

                    all_tasks.append(
                        (
                            player_type,
                            params,
                            team_name,
                            format_name,
                            start_date,
                            end_date,
                        )
                    )

        results = []
        total_tasks = len(all_tasks)
        chunks = [
            all_tasks[i : i + self.chunk_size]
            for i in range(0, len(all_tasks), self.chunk_size)
        ]

        with tqdm(
            total=total_tasks,
            desc=f"Scraping {player_type}",
            unit="req",
            colour="magenta",
        ) as pbar:
            self.downloaded_bytes[player_type] = 0
            pbar.set_postfix_str(f"Data Fetched: {self._format_bytes(0)}")

            for chunk in chunks:
                chunk_results = []
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    future_to_task = {
                        executor.submit(self.fetch_data, *task): task for task in chunk
                    }

                    for future in as_completed(future_to_task):
                        try:
                            data = future.result(timeout=30)
                            if data is not None:
                                chunk_results.append(data)
                        except concurrent.futures.TimeoutError:
                            print("Request timed out, continuing...")
                        except Exception as e:
                            print(f"Error processing request: {e}")
                        finally:
                            pbar.update(1)
                            pbar.set_postfix_str(
                                f"Data Fetched: {self._format_bytes(self.downloaded_bytes[player_type])}"
                            )

                if chunk_results:
                    chunk_df = pd.concat(chunk_results, ignore_index=True)
                    results.append(chunk_df)

                time.sleep(0.5)

        if results:
            return pd.concat(results, ignore_index=True)
        return pd.DataFrame()

    def clean_data(self, df, data_type):
        """Clean and convert the data types of the DataFrame based on data_type."""
        if df is None or df.empty:
            return df

        try:
            df.replace("-", np.nan, inplace=True)

            if data_type == "batting":
                if "HS" in df.columns:
                    df["HS"] = df["HS"].str.replace("*", "", regex=False)
                int_cols = [
                    "Mat",
                    "Inns",
                    "NO",
                    "Runs",
                    "BF",
                    "100",
                    "50",
                    "0",
                    "4s",
                    "6s",
                ]
                float_cols = ["Ave", "SR"]
            elif data_type == "bowling":
                int_cols = ["Mat", "Inns", "Mdns", "Runs", "Wkts", "4", "5"]
                float_cols = ["Ave", "Econ", "SR"]
            elif data_type == "fielding":
                int_cols = ["Mat", "Inns", "Dis", "Ct", "St", "Wk", "Fi"]
                float_cols = ["D/I"]
            else:
                return df

            for col in int_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

            for col in float_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce").astype(float)

            date_cols = ["Start Date", "End Date"]
            for col in date_cols:
                if col in df.columns:
                    df[col] = pd.to_datetime(
                        df[col], format="%d+%b+%Y", errors="coerce"
                    )
        except Exception as e:
            print(f"Error cleaning data for {data_type}: {e}")

        return df


def main():
    parser = argparse.ArgumentParser(
        description="Scrape cricket statistics from ESPNcricinfo"
    )
    parser.add_argument(
        "--type",
        choices=["batting", "bowling", "fielding"],
        required=True,
        help="Type of data to scrape (batting, bowling, or fielding)",
    )
    args = parser.parse_args()

    scrapper = ParallelScrapper(max_workers=16, rate_limit=0.1)
    time_spans = scrapper.generate_time_spans(2024, 2025)

    data_type = args.type
    print(f"\n=== Processing {data_type} data ===")
    df = scrapper.scrape_player_data(data_type, time_spans)
    process_total = scrapper._format_bytes(scrapper.downloaded_bytes[data_type])

    if df is not None and not df.empty:
        df = scrapper.clean_data(df, data_type)
        print(f"Collected {len(df)} {data_type} records")

        filename = f"{data_type}_data.csv"
        df.to_csv(filename, index=False)
        print(f"Saved to {filename}")

    else:
        print(f"No {data_type} data collected")

    print(f"{data_type.capitalize()} data downloaded: {process_total}")
    print("\n=== Scraping Complete ===")
    print(
        f"Total data downloaded: {scrapper._format_bytes(scrapper.total_downloaded_bytes)}"
    )


if __name__ == "__main__":
    main()
