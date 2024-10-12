import os
import time

import pandas as pd
import requests
from bs4 import BeautifulSoup
from httpcore import TimeoutException
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from utils.utils import categories, parse_uk_date


def quick_parse() -> None:
    def fetch_job_data(category: str, headers: dict) -> list:
        url = f"https://jobs.dou.ua/vacancies/?category={category}"
        response = requests.get(url, headers=headers, timeout=10)
        soup = BeautifulSoup(response.text, "html.parser")
        jobs = soup.find_all("li", class_="l-vacancy")
        data = []

        for job in jobs:
            date_posted = job.find("div", class_="date").text.strip()
            parsed_date = parse_uk_date(date_posted)
            title_tag = job.find("a", class_="vt")
            job_title = title_tag.text.strip()
            job_url = title_tag["href"]
            company_tag = job.find("a", class_="company")
            company_name = company_tag.text.strip()
            company_url = company_tag["href"]
            salary_tag = job.find("span", class_="salary")
            salary = salary_tag.text.strip() if salary_tag else "Not specified"
            location_tag = job.find("span", class_="cities")
            location = location_tag.text.strip() if location_tag else "Location not specified"
            info_tag = job.find("div", class_="sh-info")
            short_info = info_tag.text.strip() if info_tag else "No description provided"

            # Request to fetch full job description
            job_response = requests.get(job_url, headers=headers, timeout=10)
            job_soup = BeautifulSoup(job_response.text, "html.parser")
            full_description_element = job_soup.find("div", class_="b-typo vacancy-section")
            full_description = (
                full_description_element.text.strip() if full_description_element else "No full description"
            )

            data.append(
                {
                    "Date Posted": parsed_date,
                    "Job Title": job_title,
                    "Job URL": job_url,
                    "Company Name": company_name,
                    "Company URL": company_url,
                    "Salary": salary,
                    "Location": location,
                    "Short Info": short_info,
                    "Full Description": full_description,
                },
            )
            time.sleep(3)  # Sleep to avoid rate limiting
        return data

    headers = {
        "User-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:103.0) Gecko/20100101 Firefox/103.0",
    }

    csv_filename = "all_job_listings.csv"
    existing_df = pd.read_csv(csv_filename) if os.path.exists(csv_filename) else pd.DataFrame()

    all_data = []
    for category in categories:
        category_data = fetch_job_data(category, headers)
        all_data.extend(category_data)
        time.sleep(3)  # Sleep to avoid rate limiting
    new_df = pd.DataFrame(all_data)

    # Combine new data with existing data and remove duplicates
    combined_df = pd.concat([existing_df, new_df])
    combined_df = combined_df.drop_duplicates(subset="Job URL", keep="last")

    # Save to CSV
    combined_df.to_csv(csv_filename, index=False, encoding="utf-8")


def full_parse() -> None:
    # Setup WebDriver
    service = Service(executable_path="chromedriver.exe")
    driver = webdriver.Chrome(service=service)

    def fetch_job_data_with_clicks(category: str):
        url = f"https://jobs.dou.ua/vacancies/?category={category}"
        driver.get(url)

        try:
            while True:  # Continuously attempt to click the "Load More" button
                # Use CSS Selector to find the "Load More Vacancies" link
                load_more_button = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, ".more-btn a")),
                )
                load_more_button.click()
                # Wait for the page to load more content (adjust time as necessary)
                time.sleep(2)
        except TimeoutException:
            pass

        soup = BeautifulSoup(driver.page_source, "html.parser")
        jobs = soup.find_all("li", class_="l-vacancy")
        data = []

        for job in jobs:
            date_posted = job.find("div", class_="date").text.strip()
            parsed_date = parse_uk_date(date_posted)
            title_tag = job.find("a", class_="vt")
            job_title = title_tag.text.strip()
            job_url = title_tag["href"]
            company_tag = job.find("a", class_="company")
            company_name = company_tag.text.strip()
            company_url = company_tag["href"]
            salary_tag = job.find("span", class_="salary")
            salary = salary_tag.text.strip() if salary_tag else "Not specified"
            location_tag = job.find("span", class_="cities")
            location = location_tag.text.strip() if location_tag else "Location not specified"
            info_tag = job.find("div", class_="sh-info")
            short_info = info_tag.text.strip() if info_tag else "No description provided"
            time.sleep(2)
            job_response = requests.get(job_url)
            job_soup = BeautifulSoup(job_response.text, "html.parser")
            full_description_elements = job_soup.find_all("div", class_="b-typo vacancy-section")
            time.sleep(2)
            # Process each paragraph and add newline characters
            full_description = "\n".join([elem.text.strip() + "\n" for elem in full_description_elements])

            data.append(
                {
                    "Date Posted": parsed_date,
                    "Job Title": job_title,
                    "Job URL": job_url,
                    "Company Name": company_name,
                    "Company URL": company_url,
                    "Salary": salary,
                    "Location": location,
                    "Short Info": short_info,
                    "Full Description": full_description,
                    "source": "dou.ua",
                },
            )

        return data

    all_data = []

    for category in categories:
        category_data = fetch_job_data_with_clicks(category)
        all_data.extend(category_data)

    driver.quit()  # Close the WebDriver
    existing_df = pd.read_csv("all_job_listings.csv") if os.path.exists("all_job_listings.csv") else pd.DataFrame()

    new_df = pd.DataFrame(all_data)
    combined_df = pd.concat([existing_df, new_df]).drop_duplicates(subset=["Job URL"])  # Assuming 'Job URL' is unique
    combined_df.to_csv("all_job_listings.csv", index=False, encoding="utf-8")
