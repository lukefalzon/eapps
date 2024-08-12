"""SCRIPT TO SCRAPE EAPPS MALTA"""
import os
import time
import threading
from datetime import datetime
import pandas as pd
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By


YEARS = ["24", "23", "22", "21", "20", "19", "18", "17", "16"]
APPLICATION_TYPES = ["PA", "RG", "DN", "DS", "PC"]

RESULTS_DF = None

driver_lock = threading.Lock()  # Lock for driver creation
results_lock = threading.Lock()


def create_dataframe():
    """
    Create a fresh pandas dataframe to store scraped data
    """
    new_df = pd.DataFrame(columns=[
        "case_status",
        "case_number",
        "location",
        "description",
        "applicant",
        "architect",
        "reception_date",
        "application_type",
        "case_category"
    ])
    return new_df


def append_row(df, details):
    """
    Append a row to the dataframe
    """
    row_to_append = pd.DataFrame([{
        "case_status": details.get("Case Status", details.get("Case Status:", None)),
        "case_number": details.get("Case Number:", None),
        "location": details.get("Location of development:", None),
        "description": details.get("Description of works:", None),
        "applicant": details.get("Current Applicant:", None),
        "architect": details.get("Current Architect:", None),
        "reception_date": details.get("Reception date:", None),
        "application_type": details.get("Application Type:", None),
        "case_category": details.get("Case Category:", None)
    }])
    df = pd.concat([df, row_to_append], ignore_index=True)
    return df


def activate_selenium_driver():
    """Accesses Chrome webdriver and initializes instance"""
    driver = webdriver.Chrome()
    return driver


def cycle(driver, df, app_type, year):
    """
    Script to scrape page
    """
    run = True
    case = 1
    while run:
        print(f"Case number {app_type}/{case:05}/{year}")
        driver.get(
            f"https://www.pa.org.mt/{app_type}casedetails?CaseType={app_type}/{case:05}/{year}")
        if case == 1:
            time.sleep(1)
        details = {}

        try:
            # Find the table with the "Application Status" section
            application_status_table = driver.find_element(
                By.XPATH, "//table[@class='formTable' and .//th[contains(text(), 'Application Status (Does not reflect any appeal decisions on the case)')]]")

            # Now, you can iterate through the rows of the table and extract the information
            for row in application_status_table.find_elements(By.TAG_NAME, 'tr'):
                cells = row.find_elements(By.TAG_NAME, 'td')
                if len(cells) == 2:
                    label = cells[0].text.strip()
                    value = cells[1].text.strip()
                    details[label] = value
        except NoSuchElementException:
            try:
                # Find the table with the "Application Status" section
                application_status_table = driver.find_element(
                    By.XPATH, "//table[@class='formTable' and .//th[contains(text(),'Application Status Application Status (Does not reflect any Appeals Decisions on the case)')]]")

                # Now, you can iterate through the rows of the table and extract the information
                for row in application_status_table.find_elements(By.TAG_NAME, 'tr'):
                    cells = row.find_elements(By.TAG_NAME, 'td')
                    if len(cells) == 2:
                        label = cells[0].text.strip()
                        value = cells[1].text.strip()
                        details[label] = value
            except NoSuchElementException:
                try:
                    result = driver.find_element(
                        By.CSS_SELECTOR, "#main-content > div.clear-both.container > div").text
                    if result == "This Application Number does not exist":
                        run = False
                    elif result == 'The application has not yet been fully submitted. Once the application is considered complete, all relevant details will be made available online.':
                        case += 1
                except NoSuchElementException:
                    try:
                        result = driver.find_element(
                            By.CSS_SELECTOR, "#Table1 > tbody > tr > td > div").text
                        if result == "This Application Number does not exist":
                            run = False
                        elif result == 'The application has not yet been fully submitted. Once the application is considered complete, all relevant details will be made available online.':
                            case += 1
                    except NoSuchElementException:
                        pass

        try:
            # Find the table with the "Application Details" section
            application_details_table = driver.find_element(
                By.XPATH, "//table[@class='formTable' and .//th[contains(text(), 'Application Details')]]")

            # Now, you can iterate through the rows of the table and extract the information
            for row in application_details_table.find_elements(By.TAG_NAME, 'tr'):
                cells = row.find_elements(By.TAG_NAME, 'td')
                if len(cells) == 2:
                    label = cells[0].text.strip()
                    value = cells[1].text.strip()
                    details[label] = value
        except NoSuchElementException:
            pass

        try:
            processing_details_table = driver.find_element(
                By.XPATH, "//table[@class='formTable' and .//th[contains(text(), 'Initial Processing')]]")

            for row in processing_details_table.find_elements(By.TAG_NAME, 'tr'):
                cells = row.find_elements(By.TAG_NAME, 'td')
                if len(cells) == 2:
                    label = cells[0].text.strip()
                    value = cells[1].text.strip()
                    if label == "Application Type:" or "Case Category:":
                        details[label] = value
                    else:
                        pass
        except NoSuchElementException:
            pass

        df = append_row(df, details)

        case += 1

    return df


def scrape_application_type(driver, application_type, year):
    global RESULTS_DF
    df = create_dataframe()
    df = cycle(driver, df, application_type, year)

    driver.quit()

    with results_lock:
        if RESULTS_DF is None:
            RESULTS_DF = df
        else:
            RESULTS_DF = pd.concat([RESULTS_DF, df], ignore_index=True)


def main():
    start_time = time.time()
    print(f"\nScript started at {datetime.now()}")

    """Determine file path"""
    path = os.path.dirname(os.path.abspath("eapps_scrape.py"))
    print(path)

    threads = []
    for application_type in APPLICATION_TYPES:
        for year in YEARS:
            driver = activate_selenium_driver()
            scrape_application_type(driver, application_type, year)

    #         thread = threading.Thread(
    #             target=scrape_application_type, args=(driver, application_type, year))
    #         threads.append(thread)
    #         thread.start()

    # for thread in threads:
    #     thread.join()

    RESULTS_DF.to_csv(fr"{path}\raw.csv", encoding='utf-8', index=False)

    # Get the current date and format it
    current_date = datetime.now()
    formatted_date = current_date.strftime("%d/%m/%Y")

    # Write the formatted date to a txt file
    txt_file_path = f"{path}\\extraction_date.txt"  # Use double backslashes for Windows paths

    with open(txt_file_path, "w") as txt_file:
        txt_file.write(formatted_date)

    print(f"Formatted date '{formatted_date}' has been written to '{txt_file_path}'.")

    print("\nTotal run time:", int((time.time() - start_time) / 60), "minutes")
    print("\nCompleted")

if __name__ == "__main__":
    main()
