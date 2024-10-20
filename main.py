import concurrent.futures
import logging
import os
from pathlib import Path
from typing import Optional

import markdown
import openai
import pandas as pd
from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
from starlette.responses import RedirectResponse

from model.chat import data_chat
from model.qdrant_data_insertion import qdrant_insert
from parsing_scripts.dou_parse import quick_parse
from utils.data_operations import filter_and_sort_jobs, load_job_data
from utils.db_operations import add_message_to_db, delete_room, get_all_rooms, get_chat_history
from utils.utils import initialize_qdrant_client, categories

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


templates = Jinja2Templates(directory="templates")
dataset_path = Path("dataset/processed_jobs.csv")
job_data = load_job_data(dataset_path)

scheduler = BackgroundScheduler()
job = None
qdrant_client = initialize_qdrant_client()
openai.api_key = os.getenv('OPENAI_API_KEY')
executor = concurrent.futures.ThreadPoolExecutor()


# Функция для парсинга данных и вставки в Qdrant
def run_parser() -> None:
    logging.info("Starting immediate quick_parse.")
    quick_parse()  # Парсинг данных
    qdrant_insert(qdrant_client)  # Вставка в Qdrant
    logging.info("Completed immediate quick_parse and insertion.")


# Функция для планирования ежедневного запуска quick_parse

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
app.add_middleware(SessionMiddleware, secret_key="your_secret_key")


@app.on_event("startup")
async def on_startup() -> None:
    """Функция запускается при старте приложения."""
    executor.submit(run_parser)


@app.get("/", response_class=HTMLResponse)
async def base_page(request: Request):
    rooms = get_all_rooms()
    if rooms:
        first_room = rooms[0].room_name
        return templates.TemplateResponse(
            "chat.html",
            {"request": request, "history": get_chat_history(first_room), "room_name": first_room, "rooms": rooms},
        )
    return templates.TemplateResponse("create_room.html", {"request": request})


@app.post("/create_room", response_class=HTMLResponse)
async def create_room(request: Request, room_name: str = Form(...)):
    add_message_to_db(room_name, "Добро пожаловать в новый чат!", "Чат успешно создан.")
    return RedirectResponse(url=f"/chat/{room_name}", status_code=303)


@app.post("/delete_room", response_class=HTMLResponse)
async def delete_room_view(request: Request, room_name: str = Form(...)):
    delete_room(room_name)
    return RedirectResponse(url="/", status_code=303)


@app.get("/chat/{room_name}", response_class=HTMLResponse)
async def get_chat_page(request: Request, room_name: str):
    history = get_chat_history(room_name)
    rooms = get_all_rooms()
    return templates.TemplateResponse(
        "chat.html", {"request": request, "history": history, "room_name": room_name, "rooms": rooms}
    )


@app.post("/chat/{room_name}", response_class=HTMLResponse)
async def post_chat_message(request: Request, room_name: str, message: str = Form(...)):
    response, _ = data_chat(message)
    formatted_response = markdown.markdown(response, extensions=["extra"], output_format="html5")

    add_message_to_db(room_name=room_name, user_message=message, bot_response=formatted_response)

    history = get_chat_history(room_name)
    rooms = get_all_rooms()

    return templates.TemplateResponse(
        "chat.html",
        {
            "request": request,
            "history": history,
            "room_name": room_name,
            "rooms": rooms,
        },
    )


from utils.utils import categories  # Импорт списка категорий

from utils.utils import categories  # Импорт списка категорий

@app.get("/vacancies", response_class=HTMLResponse)
async def vacancies_page(
    request: Request,
    sort_by: Optional[str] = None,
    order: Optional[str] = "desc",
    page: int = 1,
    page_size: int = 10,
    search: Optional[str] = None,
    category: Optional[str] = None,
    location: Optional[str] = None,
):
    if not search or search.lower() == "none":
        search = None
    if not sort_by or sort_by.lower() == "none":
        sort_by = None
    if not order or order.lower() == "none":
        order = "desc"

    # Фильтрация по категории
    if category and category.lower() != "none":
        filtered_data = job_data[job_data["Category"] == category]
    else:
        filtered_data = job_data

    # Фильтрация по локации
    if location and location.lower() != "none":
        filtered_data = filtered_data[filtered_data["Location"].apply(lambda x: location in x.split(','))]

    # Преобразуем столбец "Date" в datetime для сортировки
    filtered_data["Date"] = pd.to_datetime(filtered_data["Date"], format="%d.%m.%Y", errors='coerce')

    # Фильтрация и сортировка по ключевому слову
    if search:
        records, total_pages = filter_and_sort_jobs(filtered_data, search, sort_by, order, page, page_size)
    else:
        records, total_pages = filter_and_sort_jobs(filtered_data, None, sort_by, order, page, page_size)

    message = "No job listings match your search criteria." if records.empty else None

    # Получаем уникальные локации для выпадающего списка
    all_locations = set()
    for loc_list in job_data["Location"].dropna().str.split(','):
        all_locations.update([loc.strip() for loc in loc_list])
    all_locations = sorted(all_locations, key=lambda x: (0 if 'Україна' in x else 1, x))

    return templates.TemplateResponse(
        "vacancies.html",
        {
            "request": request,
            "records": records.to_dict(orient="records"),
            "sort_by": sort_by,
            "order": order,
            "page": page,
            "total_pages": total_pages,
            "page_size": page_size,
            "search": search,
            "message": message,
            "categories": categories,  # Передаем список категорий в шаблон
            "selected_category": category,  # Передаем выбранную категорию
            "locations": all_locations,  # Передаем список локаций в шаблон
            "selected_location": location,  # Передаем выбранную локацию
        },
    )

