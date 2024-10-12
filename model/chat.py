from langchain.chains import RetrievalQA
from langchain.prompts import PromptTemplate
from langchain_openai import OpenAIEmbeddings
from langchain_openai.chat_models.base import ChatOpenAI
from langchain_qdrant import Qdrant

from utils.utils import initialize_qdrant_client, openai_key


def create_retrieval_qa(qdrant_client: Qdrant, collection_name: str = "csv-collection1") -> RetrievalQA:
    embeddings = OpenAIEmbeddings(openai_api_key=openai_key)

    vectorstore = Qdrant(
        client=qdrant_client,
        collection_name=collection_name,
        embeddings=embeddings,
        content_payload_key="page_content",
    )

    retriever = vectorstore.as_retriever(search_kwargs={"k": 4})

    llm = ChatOpenAI(openai_api_key=openai_key, model="gpt-4o", temperature=0.5)

    prompt_template = """
    Используя предоставленные данные, ответь на следующий вопрос максимально подробно и точно.
    Даты указаны в формате ДД.ММ.ГГГГ.

    Вопрос: {question}

    Данные:
    {context}

    Ответ:
    """

    prompt = PromptTemplate(
        template=prompt_template,
        input_variables=["context", "question"],
    )

    return RetrievalQA.from_chain_type(
        llm=llm,
        chain_type="stuff",
        retriever=retriever,
        chain_type_kwargs={"prompt": prompt},
        return_source_documents=True,
    )


def data_chat(question: str) -> str:
    qdrant_client = initialize_qdrant_client()
    qa_chain = create_retrieval_qa(qdrant_client)

    response = qa_chain.invoke({"query": question})
    result = response["result"]
    response["source_documents"]

    return result
