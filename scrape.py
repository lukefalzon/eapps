import os
import time
from datetime import datetime
import pandas as pd
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By


class EAPPScraper:
    YEARS = ["24", "23", "22", "21", "20", "19", "18", "17", "16"]
    APPLICATION_TYPES = ["PA", "RG", "DN", "DS", "PC"]

    def __init__(self, output_dir=".", headless=False):
        self.output_dir = output_dir
        self.headless = headless
        self.driver = self.activate_selenium_driver()
        self.results_df = self.create_dataframe()
        self.last_state_file = os.path.join(self.output_dir, "last_state.txt")
        self.starting_app_index = 0
        self.starting_no = 1
        self.starting_year_index = 0
        self.latest_details = None
        self.fails = 0

    @staticmethod
    def create_dataframe():
        """Create a fresh pandas dataframe to store scraped data."""
        columns = [
            "case_status",
            "case_number",
            "location",
            "description",
            "applicant",
            "architect",
            "reception_date",
            "application_type",
            "case_category",
        ]
        return pd.DataFrame(columns=columns)

    def activate_selenium_driver(self):
        """Initialize and return the Selenium WebDriver instance."""
        options = webdriver.ChromeOptions()
        if self.headless:
            options.add_argument("--headless")
        driver = webdriver.Chrome(options=options)
        return driver

    def initialise_state(self):
        """Load the last processed application type, case, and year."""
        if os.path.exists(self.last_state_file):
            with open(self.last_state_file, "r") as f:
                app_type, app_no, app_year = f.read().strip().split("/")
                if (
                    app_type in self.APPLICATION_TYPES
                    and app_no.isdigit()
                    and app_year in self.YEARS
                ):
                    self.starting_app_index = self.APPLICATION_TYPES.index(app_type)
                    self.starting_no = int(app_no) + 1
                    self.starting_year_index = self.YEARS.index(app_year)

    def scrape_application_type(self, application_type_index, case, year_index):
        """Scrape data for a specific application type and year."""
        application_type = self.APPLICATION_TYPES[
            application_type_index % len(self.APPLICATION_TYPES)
        ]
        year = self.YEARS[year_index % len(self.YEARS)]
        while True:
            print(f"Scraping: {application_type}/{case:05}/{year}")
            case_url = f"https://www.pa.org.mt/{application_type}casedetails?CaseType={application_type}/{case:05}/{year}"
            self.driver.get(case_url)

            details = self.extract_case_details()
            if self.fails >= 5:
                break
            # Save data incrementally after each page
            self.save_row(details)
            # Save the last state after each successful scrape
            self.save_last_state(application_type, case, year)

            case += 1

    def extract_case_details(self):
        """Extract case details from the currently loaded page."""
        details = {}
        try:
            # Find the table with the "Application Status" section
            application_status_table = self.driver.find_element(
                By.XPATH,
                "//table[@class='formTable' and .//th[contains(text(), 'Application Status (Does not reflect any appeal decisions on the case)')]]",
            )

            # Now, you can iterate through the rows of the table and extract the information
            for row in application_status_table.find_elements(By.TAG_NAME, "tr"):
                cells = row.find_elements(By.TAG_NAME, "td")
                if len(cells) == 2:
                    label = cells[0].text.strip()
                    value = cells[1].text.strip()
                    details[label] = value
            self.fails = 0
        except NoSuchElementException:
            try:
                # Find the table with the "Application Status" section
                application_status_table = self.driver.find_element(
                    By.XPATH,
                    "//table[@class='formTable' and .//th[contains(text(),'Application Status Application Status (Does not reflect any Appeals Decisions on the case)')]]",
                )

                # Now, you can iterate through the rows of the table and extract the information
                for row in application_status_table.find_elements(By.TAG_NAME, "tr"):
                    cells = row.find_elements(By.TAG_NAME, "td")
                    if len(cells) == 2:
                        label = cells[0].text.strip()
                        value = cells[1].text.strip()
                        details[label] = value
                self.fails = 0
            except NoSuchElementException:
                try:
                    result = self.driver.find_element(
                        By.CSS_SELECTOR,
                        "#main-content > div.clear-both.container > div",
                    ).text
                    if result == "This Application Number does not exist":
                        self.fails += 1
                    elif (
                        result
                        == "The application has not yet been fully submitted. Once the application is considered complete, all relevant details will be made available online."
                    ):
                        self.fails = 0
                        pass
                except NoSuchElementException:
                    try:
                        result = self.driver.find_element(
                            By.CSS_SELECTOR, "#Table1 > tbody > tr > td > div"
                        ).text
                        if result == "This Application Number does not exist":
                            self.fails += 1
                        elif (
                            result
                            == "The application has not yet been fully submitted. Once the application is considered complete, all relevant details will be made available online."
                        ):
                            self.fails = 0
                            pass
                    except NoSuchElementException:
                        pass

        try:
            # Find the table with the "Application Details" section
            application_details_table = self.driver.find_element(
                By.XPATH,
                "//table[@class='formTable' and .//th[contains(text(), 'Application Details')]]",
            )

            # Now, you can iterate through the rows of the table and extract the information
            for row in application_details_table.find_elements(By.TAG_NAME, "tr"):
                cells = row.find_elements(By.TAG_NAME, "td")
                if len(cells) == 2:
                    label = cells[0].text.strip()
                    value = cells[1].text.strip()
                    details[label] = value
        except NoSuchElementException:
            pass

        try:
            processing_details_table = self.driver.find_element(
                By.XPATH,
                "//table[@class='formTable' and .//th[contains(text(), 'Initial Processing')]]",
            )

            for row in processing_details_table.find_elements(By.TAG_NAME, "tr"):
                cells = row.find_elements(By.TAG_NAME, "td")
                if len(cells) == 2:
                    label = cells[0].text.strip()
                    value = cells[1].text.strip()
                    if label == "Application Type:" or "Case Category:":
                        details[label] = value
                    else:
                        pass
        except NoSuchElementException:
            pass

        return details

    def save_row(self, details):
        """Save a row of details to the CSV file incrementally."""
        row = {
            "case_status": details.get("Case Status"),
            "case_number": details.get("Case Number:"),
            "location": details.get("Location of development:"),
            "description": details.get("Description of works:"),
            "applicant": details.get("Current Applicant:"),
            "architect": details.get("Current Architect:"),
            "reception_date": details.get("Reception date:"),
            "application_type": details.get("Application Type:"),
            "case_category": details.get("Case Category:"),
        }
        self.latest_details = pd.DataFrame([row])

    def save_last_state(self, application_type, case, year):
        """Save the last processed application type, case, and year."""
        with open(self.last_state_file, "w") as f:
            f.write(f"{application_type}/{case}/{year}")
        output_file = os.path.join(self.output_dir, "raw.csv")
        if not os.path.exists(output_file):
            self.latest_details.to_csv(output_file, mode="w", header=True, index=False)
        else:
            self.latest_details.to_csv(output_file, mode="a", header=False, index=False)

    def run(self):
        """Run the scraping process."""

        # Start timer
        start_time = time.time()
        print(f"Script started at {datetime.now()}")

        # Determine initial state
        self.initialise_state()

        for year_index, _ in enumerate(self.YEARS):
            for app_type_index, _ in enumerate(self.APPLICATION_TYPES):

                # Calculate the actual application type index and year index
                current_app_index = (app_type_index + self.starting_app_index) % len(
                    self.APPLICATION_TYPES
                )
                current_year_index = year_index + self.starting_year_index

                self.scrape_application_type(
                    current_app_index,
                    self.starting_no,
                    current_year_index,
                )

                self.starting_no = 1
                self.fails = 0

        print(f"Total run time: {int((time.time() - start_time) / 60)} minutes")


if __name__ == "__main__":
    scraper = EAPPScraper(output_dir=os.path.dirname(os.path.abspath(__file__)))
    scraper.run()
