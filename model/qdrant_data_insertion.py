import contextlib
import re

from langchain_openai import OpenAIEmbeddings
from qdrant_client.http import models

from utils.utils import get_shortened_data, openai_key


def qdrant_insert(qdrant_client, collection_name="csv-collection1") -> None:
    """Insert data into Qdrant."""
    vectors_config = models.VectorParams(size=1536, distance=models.Distance.COSINE)
    embeddings = OpenAIEmbeddings(openai_api_key=openai_key)

    documents, payloads = [], []
    data = get_shortened_data()

    for _, row in data.iterrows():
        full_description = row.get("Full Description", "").strip()
        if not full_description:
            continue

        # Clean and format the text
        full_description = full_description.replace("\n", " ").replace("\r", " ").strip()
        sentences = re.split(r"(?<=[.!?])\s+", full_description)
        sentences = [sentence.strip() for sentence in sentences if sentence.strip()]
        full_description_formatted = "\n".join(sentences)

        metadata_str = (
            f"\n\nDate Posted: {row['Date Posted']}\n"
            f"Job Title: {row['Job Title']}\n"
            f"Company Name: {row['Company Name']}\n"
            f"Salary: {row['Salary']}\n"
            f"Location: {row['Location']}\n"
        )
        full_description_with_metadata = full_description_formatted + metadata_str
        documents.append(full_description_with_metadata)
        payload = {
            "Date Posted": row["Date Posted"],
            "Job Title": row["Job Title"],
            "Company Name": row["Company Name"],
            "Salary": row["Salary"],
            "Location": row["Location"],
            "page_content": full_description_with_metadata,  # Include page_content here
        }
        payloads.append(payload)

    embeddings_list = embeddings.embed_documents(documents)

    points = [
        models.PointStruct(
            id=i,
            vector=embedding,
            payload=payload,
        )
        for i, (embedding, payload) in enumerate(zip(embeddings_list, payloads))
    ]

    if qdrant_client.collection_exists(collection_name):
        qdrant_client.delete_collection(collection_name)

    qdrant_client.create_collection(
        collection_name=collection_name,
        vectors_config=vectors_config,
    )

    batch_size = 30
    for i in range(0, len(points), batch_size):
        batch = points[i : i + batch_size]
        with contextlib.suppress(Exception):
            qdrant_client.upsert(collection_name=collection_name, points=batch)
