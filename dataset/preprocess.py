import pandas as pd
import asyncio
import aiohttp
import openai
import logging
import os

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')


# Загрузка данных
df = pd.read_csv('all_job_listings.csv')

categories = [
    ".NET", "AI/ML", "Analyst", "Android", "Animator", "Architect", "Artist",
    "Big Data", "Blockchain", "C++", "C-level", "Copywriter", "Data Engineer",
    "Data Science", "DBA", "Design", "DevOps", "Embedded", "Engineering Manager",
    "Erlang", "ERP/CRM", "Finance", "Flutter", "Front End", "Golang", "Hardware",
    "HR", "iOS/macOS", "Java", "Legal", "Marketing", "Node.js", "Office Manager",
    "Other", "PHP", "Product Manager", "Project Manager", "Python", "QA",
    "React Native", "Ruby", "Rust", "Sales", "Salesforce", "SAP", "Scala",
    "Scrum Master", "Security", "SEO", "Support", "SysAdmin", "Technical Writer",
    "Unity", "Unreal Engine",
]


# Функция для разбора ответа модели
def parse_response(content):
    lines = content.strip().split('\n')
    data = {'Date': '', 'Location': '', 'Role': '', 'Project description': '', 'Responsibilities': '',
            'Requirements': '', 'Additional points': '', 'Category': ''}
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
        elif line.startswith('Category:'):
            data['Category'] = line.replace('Category:', '').strip()
            current_field = 'Category'
        elif current_field and line:
            data[current_field] += ' ' + line.strip()
    return data


# Глобальный счетчик для логирования первых N ответов
logged_responses_count = 0
max_logged_responses = 3  # Измените это значение, чтобы логировать больше или меньше ответов


# Функция для создания запроса к модели
async def get_job_details(session, row):
    global logged_responses_count

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
Category: [Только одно слово из списка категорий]

Категория должна быть выбрана из следующего списка и содержать только одно слово: {', '.join(categories)}.

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
            return parse_response(content)
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


# Главная функция
def main():
    logging.info("Начало обработки вакансий")
    # Разбиваем DataFrame на части для обработки (например, по 20 вакансий)
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
    df_results.to_csv('processed_jobs.csv', index=False)
    logging.info("Обработка вакансий завершена, результаты сохранены")


if __name__ == '__main__':
    main()
