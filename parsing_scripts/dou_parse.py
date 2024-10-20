import logging
import os
import time

import openai
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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("job_parsing.log"),
        logging.StreamHandler(),
    ],
)


def quick_parse() -> None:
    logging.info("Starting job parsing process.")

    def fetch_job_data(category: str, headers: dict) -> list:
        logging.info(f"Fetching job data for category: {category}")
        url = f"https://jobs.dou.ua/vacancies/?category={category}"
        try:
            response = requests.get(url, headers=headers, timeout=10)
            soup = BeautifulSoup(response.text, "html.parser")
            jobs = soup.find_all("li", class_="l-vacancy")
            logging.info(f"Found {len(jobs)} jobs in category {category}.")
        except Exception as e:
            logging.exception(f"Error fetching jobs from {url}: {e!s}")
            return []

        data = []

        for job in jobs:
            try:
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

                # Запрос для получения полного описания вакансии
                job_response = requests.get(job_url, headers=headers, timeout=10)
                job_soup = BeautifulSoup(job_response.text, "html.parser")
                full_description_element = job_soup.find("div", class_="b-typo vacancy-section")
                full_description = (
                    full_description_element.text.strip() if full_description_element else "No full description"
                )

                logging.info(f"Parsed job: {job_title} from {company_name}.")

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
                        "Category": category,  # Добавляем категорию из цикла
                    },
                )
            except Exception as e:
                logging.exception(f"Error parsing job: {e!s}")

            time.sleep(3)  # Sleep to avoid rate limiting
        return data

    headers = {
        "User-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:103.0) Gecko/20100101 Firefox/103.0",
    }

    csv_filename = "all_job_listings.csv"
    logging.info(f"Loading existing job data from {csv_filename}.")

    try:
        existing_df = pd.read_csv(csv_filename) if os.path.exists(csv_filename) else pd.DataFrame()
    except Exception as e:
        logging.exception(f"Error reading CSV file: {e!s}")
        existing_df = pd.DataFrame()

    all_data = []
    for category in categories:
        category_data = fetch_job_data(category, headers)
        all_data.extend(category_data)
        time.sleep(3)

    new_df = pd.DataFrame(all_data)

    logging.info("Combining new data with existing data and removing duplicates.")
    combined_df = pd.concat([existing_df, new_df])
    combined_df = combined_df.drop_duplicates(subset="Job URL", keep="last")

    logging.info("Starting LLM processing of job postings.")
    processed_df = process_jobs_with_llm(combined_df)

    logging.info(f"Saving processed data to processed_jobs.csv.")
    try:
        processed_df.to_csv('processed_jobs.csv', index=False, encoding="utf-8")
        logging.info("Data successfully saved to CSV.")
    except Exception as e:
        logging.exception(f"Error saving data to CSV: {e!s}")

    logging.info("Job parsing and processing completed successfully.")


def process_jobs_with_llm(df):
    import asyncio
    import aiohttp
    # Функция для разбора ответа модели
    def parse_response(content):
        lines = content.strip().split('\n')
        data = {
            'Date': '',
            'Location': '',
            'Role': '',
            'Project description': '',
            'Responsibilities': '',
            'Requirements': '',
            'Additional points': '',
            'Category': '',
        }
        current_field = None
        for line in lines:
            line = line.strip()
            if line.startswith('Date:'):
                data['Date'] = line.replace('Date:', '').strip()
                current_field = 'Date'
            elif line.startswith('Location:'):
                data['Location'] = line.replace('Location:', '').strip()
                current_field = 'Location'
            elif line.startswith('Role:'):
                data['Role'] = line.replace('Role:', '').strip()
                current_field = 'Role'
            elif line.startswith('Project description:'):
                data['Project description'] = line.replace('Project description:', '').strip()
                current_field = 'Project description'
            elif line.startswith('Responsibilities:'):
                data['Responsibilities'] = line.replace('Responsibilities:', '').strip()
                current_field = 'Responsibilities'
            elif line.startswith('Requirements:'):
                data['Requirements'] = line.replace('Requirements:', '').strip()
                current_field = 'Requirements'
            elif line.startswith('Additional points:'):
                data['Additional points'] = line.replace('Additional points:', '').strip()
                current_field = 'Additional points'
            elif current_field and line:
                data[current_field] += ' ' + line.strip()
        return data

    # Глобальный счетчик для логирования первых N ответов
    logged_responses_count = 0
    max_logged_responses = 3  # Измените это значение, чтобы логировать больше или меньше ответов

    # Функция для создания запроса к модели
    async def get_job_details(session, row):
        nonlocal logged_responses_count
        # Проверка и обработка полей
        def safe_get(value):
            return '' if pd.isna(value) else str(value)

        date_posted = safe_get(row.get('Date Posted'))
        location = safe_get(row.get('Location'))
        job_title = safe_get(row.get('Job Title'))
        full_description = safe_get(row.get('Full Description'))[:1500]  # Ограничиваем длину

        prompt = f"""
Пожалуйста, проанализируй следующую вакансию и извлеки ключевые моменты. Представь их в следующем формате:

Date: {date_posted}
Location: {location}
Role: {job_title}
Project description: [Подробное описание проекта]
Responsibilities: [Основные обязанности]
Requirements: [Требования к кандидату]
Additional points: [Дополнительные плюсы]

Текст вакансии:
{full_description}
"""

        try:
            logging.info(f"Обработка вакансии: {job_title}")
            async with session.post(
                'https://api.openai.com/v1/chat/completions',
                headers={
                    'Authorization': f'Bearer {openai.api_key}',
                    'Content-Type': 'application/json'
                },
                json={
                    'model': 'gpt-3.5-turbo',
                    'messages': [{'role': 'user', 'content': prompt}],
                    'max_tokens': 700,
                    'temperature': 0.2,
                }
            ) as resp:
                result = await resp.json()
                if resp.status != 200:
                    logging.error(f"Ошибка API: {result}")
                    return None
                content = result['choices'][0]['message']['content']
                # Логируем первые несколько ответов модели для диагностики
                if logged_responses_count < max_logged_responses:
                    logging.info(f"Ответ модели для вакансии '{job_title}':\n{content}")
                    logged_responses_count += 1
                logging.info(f"Получен ответ для вакансии: {job_title}")
                parsed_data = parse_response(content)
                # Добавляем категорию из исходного DataFrame
                parsed_data['Category'] = row.get('Category', '')
                return parsed_data
        except Exception as e:
            logging.error(f"Ошибка при обработке вакансии '{job_title}': {e}")
            return None

    # Асинхронная обработка всех вакансий
    async def process_jobs(df):
        results = []
        semaphore = asyncio.Semaphore(5)  # Ограничиваем количество одновременных запросов
        async with aiohttp.ClientSession() as session:
            tasks = []
            for _, row in df.iterrows():
                tasks.append(process_job_with_semaphore(session, row, semaphore))
            responses = await asyncio.gather(*tasks)
            results.extend(responses)
        return results

    async def process_job_with_semaphore(session, row, semaphore):
        async with semaphore:
            return await get_job_details(session, row)

    # Главная часть функции
    logging.info("Начало обработки вакансий с помощью LLM")
    # Разбиваем DataFrame на батчи для обработки
    batch_size = 20
    batches = [df[i:i + batch_size] for i in range(0, df.shape[0], batch_size)]
    all_results = []
    total_batches = len(batches)
    for idx, batch in enumerate(batches, 1):
        logging.info(f"Обработка батча {idx}/{total_batches} с {len(batch)} вакансиями")
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        start_time = pd.Timestamp.now()
        results = loop.run_until_complete(process_jobs(batch))
        end_time = pd.Timestamp.now()
        elapsed_time = (end_time - start_time).total_seconds()
        logging.info(f"Завершен батч {idx}/{total_batches} за {elapsed_time:.2f} секунд")
        all_results.extend(results)

    # Удаляем None результаты (если возникли ошибки)
    all_results = [res for res in all_results if res is not None]

    # Конвертируем результаты в DataFrame
    df_results = pd.DataFrame(all_results)
    logging.info("Обработка вакансий с помощью LLM завершена")
    return df_results


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
