import logging

from langchain.chains import RetrievalQA
from langchain.prompts import PromptTemplate
from langchain_openai import ChatOpenAI, OpenAIEmbeddings
from langchain_qdrant import Qdrant

from utils.utils import initialize_qdrant_client, openai_key

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_last_100_documents(qdrant_client, collection_name: str = "csv-collection1", limit: int = 100):
    """Извлечение последних 100 документов из коллекции."""
    try:
        logger.info(f"Извлечение последних {limit} документов из коллекции: {collection_name}.")
        search_result = qdrant_client.scroll(
            collection_name=collection_name,
            limit=limit,
        )

        points, next_offset = search_result
        if not points:
            logger.warning("Нет документов в коллекции.")
            return []

        return points

    except Exception as e:
        logger.exception(f"Ошибка при извлечении документов: {e}")
        return []


def create_retrieval_qa(qdrant_client: Qdrant, collection_name: str = "csv-collection1") -> RetrievalQA:
    logger.info("Инициализация эмбеддингов OpenAI.")
    embeddings = OpenAIEmbeddings(openai_api_key=openai_key)

    logger.info(f"Подключение к Qdrant коллекции: {collection_name}.")
    vectorstore = Qdrant(
        client=qdrant_client,
        collection_name=collection_name,
        embeddings=embeddings,
        content_payload_key="page_content",
    )

    logger.info("Инициализация модели ChatOpenAI.")
    llm = ChatOpenAI(
        openai_api_key=openai_key,
        model="gpt-4",
        temperature=0.3,
        request_timeout=60,
    )

    # Обновленный и улучшенный промпт
    prompt_template = """
    Ты — AI-ассистент, который предоставляет краткие, точные и информативные ответы на основе предоставленных данных.
    Даты указаны в формате ДД.ММ.ГГГГ.

    Вопрос: {question}

    Контекст данных:
    {context}

    Предоставь краткий и точный ответ на основе контекста.
    Ответ:
    """

    prompt = PromptTemplate(
        template=prompt_template,
        input_variables=["context", "question"],
    )

    logger.info("Создание RetrievalQA цепочки с типом 'stuff'.")
    return RetrievalQA.from_chain_type(
        llm=llm,
        chain_type="stuff",
        retriever=vectorstore.as_retriever(),
        chain_type_kwargs={"prompt": prompt},
        return_source_documents=True,
    )


def data_chat(question: str):
    """Запуск цепочки QA для ответа на вопрос с использованием последних 100 документов."""
    try:
        logger.info(f"Получен вопрос: {question}")
        qdrant_client = initialize_qdrant_client()

        # Извлечение последних 100 документов
        documents = get_last_100_documents(qdrant_client, "csv-collection1", limit=100)

        if not documents:
            logger.warning("Нет документов в коллекции.")
            return "Нет документов для обработки.", []

        logger.info(f"Количество извлеченных документов: {len(documents)}")

        qa_chain = create_retrieval_qa(qdrant_client)
        logger.info("QA цепочка успешно создана.")

        # Контекст для модели на основе последних 100 документов
        context = "\n".join([doc.payload["page_content"] for doc in documents])

        logger.info("Выполнение запроса к QA цепочке.")
        response = qa_chain.invoke({"query": question, "context": context})
        result = response["result"]

        logger.info(f"Ответ получен: {result}")

        return result, documents
    except Exception as e:
        logger.exception(f"Ошибка в data_chat: {e}")
        raise
