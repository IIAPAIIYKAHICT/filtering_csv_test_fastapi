import contextlib
import re
import os
import pandas as pd
from langchain_openai import OpenAIEmbeddings
from qdrant_client import QdrantClient
from qdrant_client.http import models

from utils.utils import get_shortened_data, openai_key


def qdrant_insert(qdrant_client: QdrantClient, collection_name: str = "csv-collection1") -> None:
    """Insert data into Qdrant."""
    vectors_config = models.VectorParams(size=1536, distance=models.Distance.COSINE)
    embeddings = OpenAIEmbeddings(openai_api_key=openai_key)

    documents, payloads = [], []
    file_path = os.path.join(os.path.dirname(__file__), '..', 'dataset', 'processed_jobs.csv')
    data = pd.read_csv(file_path)  # Загрузите ваш обновленный CSV-файл

    for _, row in data.iterrows():
        # Получаем полное описание вакансии
        project_description = row.get("Project description", "").strip()
        responsibilities = row.get("Responsibilities", "").strip()
        requirements = row.get("Requirements", "").strip()
        additional_points = row.get("Additional points", "").strip()

        # Объединяем все части описания
        full_description = "\n".join(
            [
                f"Project Description: {project_description}",
                f"Responsibilities: {responsibilities}",
                f"Requirements: {requirements}",
                f"Additional Points: {additional_points}",
            ]
        ).strip()

        if not full_description:
            continue

        # Очистка и форматирование текста
        full_description = full_description.replace("\n", " ").replace("\r", " ").strip()
        sentences = re.split(r"(?<=[.!?])\s+", full_description)
        sentences = [sentence.strip() for sentence in sentences if sentence.strip()]
        full_description_formatted = "\n".join(sentences)

        # Формируем метаданные
        metadata_str = (
            f"\n\nDate: {row.get('Date', 'Unknown')}\n"
            f"Role: {row.get('Role', 'Unknown')}\n"
            f"Location: {row.get('Location', 'Unknown')}\n"
            f"Category: {row.get('Category', 'Unknown')}\n"
        )
        full_description_with_metadata = full_description_formatted + metadata_str
        documents.append(full_description_with_metadata)
        payload = {
            "Date": row.get("Date", "Unknown"),
            "Role": row.get("Role", "Unknown"),
            "Location": row.get("Location", "Unknown"),
            "Category": row.get("Category", "Unknown"),
            "Project description": project_description,
            "Responsibilities": responsibilities,
            "Requirements": requirements,
            "Additional points": additional_points,
            "page_content": full_description_with_metadata,  # Include page_content here
        }
        payloads.append(payload)

    # Получаем эмбеддинги для документов
    embeddings_list = embeddings.embed_documents(documents)

    # Формируем точки для вставки в Qdrant
    points = [
        models.PointStruct(
            id=i,
            vector=embedding,
            payload=payload,
        )
        for i, (embedding, payload) in enumerate(zip(embeddings_list, payloads))
    ]

    # Проверяем, существует ли коллекция, и удаляем ее при необходимости
    if qdrant_client.collection_exists(collection_name):
        qdrant_client.delete_collection(collection_name)

    # Создаем новую коллекцию
    qdrant_client.create_collection(
        collection_name=collection_name,
        vectors_config=vectors_config,
    )

    # Вставляем данные батчами
    batch_size = 30
    for i in range(0, len(points), batch_size):
        batch = points[i: i + batch_size]
        with contextlib.suppress(Exception):
            qdrant_client.upsert(collection_name=collection_name, points=batch)
