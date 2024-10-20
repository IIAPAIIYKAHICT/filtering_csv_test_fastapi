from pathlib import Path

import pandas as pd


def load_job_data(file_path: Path):
    job_data = pd.read_csv(file_path)
    job_data["Date"] = pd.to_datetime(job_data["Date"], format="%d.%m.%Y", errors='coerce')
    # Удаляем строки с NaT в столбце Date
    job_data = job_data.dropna(subset=["Date"])
    return job_data



def filter_and_sort_jobs(data, search=None, sort_by=None, order="desc", page=1, page_size=10):
    filtered_data = data.copy()

    # Фильтрация по ключевому слову (если есть)
    if search:
        filtered_data = filtered_data[filtered_data.apply(lambda row: search.lower() in str(row).lower(), axis=1)]

    # Сортировка данных
    if sort_by:
        ascending = order == "asc"
        filtered_data = filtered_data.sort_values(by=sort_by, ascending=ascending)

    # Пагинация
    total_records = len(filtered_data)
    total_pages = (total_records + page_size - 1) // page_size
    start = (page - 1) * page_size
    end = start + page_size

    # Возвращаем нужную страницу данных
    paginated_data = filtered_data.iloc[start:end]

    return paginated_data, total_pages
