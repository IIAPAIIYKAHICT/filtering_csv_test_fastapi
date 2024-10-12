import os
from datetime import datetime

import pandas as pd
from pydantic import SecretStr
from qdrant_client import QdrantClient

uk_month_to_number = {
    "січня": "01",
    "лютого": "02",
    "березня": "03",
    "квітня": "04",
    "травня": "05",
    "червня": "06",
    "липня": "07",
    "серпня": "08",
    "вересня": "09",
    "жовтня": "10",
    "листопада": "11",
    "грудня": "12",
}

openai_key: SecretStr = "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"
qdrant_api_key = "9iL5Cn02vtqg63UhdyDurBzqUSpY5QGaUBooVE3xc0Ix9cNWyQhuRg"
categories = [
    ".NET",
    "AI/ML",
    "Analyst",
    "Android",
    "Animator",
    "Architect",
    "Artist",
    "Big Data",
    "Blockchain",
    "C++",
    "C-level",
    "Copywriter",
    "Data Engineer",
    "Data Science",
    "DBA",
    "Design",
    "DevOps",
    "Embedded",
    "Engineering Manager",
    "Erlang",
    "ERP/CRM",
    "Finance",
    "Flutter",
    "Front End",
    "Golang",
    "Hardware",
    "HR",
    "iOS/macOS",
    "Java",
    "Legal",
    "Marketing",
    "Node.js",
    "Office Manager",
    "Other",
    "PHP",
    "Product Manager",
    "Project Manager",
    "Python",
    "QA",
    "React Native",
    "Ruby",
    "Rust",
    "Sales",
    "Salesforce",
    "SAP",
    "Scala",
    "Scrum Master",
    "Security",
    "SEO",
    "Support",
    "SysAdmin",
    "Technical Writer",
    "Unity",
    "Unreal Engine",
]


def parse_uk_date(date_str) -> str:
    day, month_name = date_str.split()
    month = uk_month_to_number[month_name]
    year = str(datetime.now().year)  # Assumes the current year
    return f"{day.zfill(2)}.{month}.{year}"


def get_shortened_data():
    current_dir = os.path.dirname(__file__)
    csv_file_path = os.path.join(current_dir, "..", "all_job_listings.csv")

    data = pd.read_csv(csv_file_path, encoding="utf-8")

    columns_to_exclude = ["Job URL", "Company URL", "source", "Short Info"]

    data = data.drop(columns=columns_to_exclude)
    data = data.dropna()
    data.to_csv("shortened_job_listings.csv", index=False)

    return data


def initialize_qdrant_client():
    """Инициализирует подключение к Qdrant."""
    return QdrantClient(
        url="https://385364fe-fdc4-4307-8f0b-6aec99351ede.us-east4-0.gcp.cloud.qdrant.io:6333",
        api_key=qdrant_api_key,
    )
