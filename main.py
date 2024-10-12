from pathlib import Path
from typing import Optional

import pandas as pd
from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware

from model.chat import data_chat

app = FastAPI()

app.add_middleware(SessionMiddleware, secret_key="your_secret_key")


templates = Jinja2Templates(directory="templates")

dataset_path = Path("dataset/shortened_job_listings.csv")
job_data = pd.read_csv(dataset_path)
job_data["Date Posted"] = pd.to_datetime(job_data["Date Posted"], format="%d.%m.%Y")


@app.get("/", response_class=HTMLResponse)
async def base_page(request: Request):
    return templates.TemplateResponse("base.html", {"request": request})


@app.get("/vacancies", response_class=HTMLResponse)
async def index(
    request: Request,
    sort_by: Optional[str] = None,
    order: Optional[str] = "desc",
    page: int = 1,
    page_size: int = 10,
) -> templates.TemplateResponse:
    data = job_data.copy()

    if sort_by:
        ascending = order == "asc"
        data = data.sort_values(by=sort_by, ascending=ascending)

    total_records = len(data)
    total_pages = (total_records + page_size - 1) // page_size
    start = (page - 1) * page_size
    end = start + page_size
    paginated_data = data.iloc[start:end]

    records = paginated_data.to_dict(orient="records")

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "records": records,
            "columns": data.columns,
            "sort_by": sort_by,
            "order": order,
            "page": page,
            "total_pages": total_pages,
            "page_size": page_size,
        },
    )


@app.get("/chat", response_class=HTMLResponse)
async def get_chat_page(request: Request):
    history = request.session.get("chat_history", [])  # Получаем историю чата из сессии
    return templates.TemplateResponse("chat.html", {"request": request, "history": history})


@app.post("/chat", response_class=HTMLResponse)
async def post_chat_message(request: Request, message: str = Form(...)):
    history = request.session.get("chat_history", [])

    response, source_documents = data_chat(message)

    history.append(
        {
            "message": message,
            "response": response,
            "source_documents": source_documents,
        },
    )
    request.session["chat_history"] = history

    return templates.TemplateResponse("chat.html", {"request": request, "history": history})
