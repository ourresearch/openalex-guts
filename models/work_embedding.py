import os

import requests
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
from elasticsearch import Elasticsearch, helpers

from app import db, logger, ELASTIC_EMBEDDINGS_URL


class WorkEmbedding(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "work_embedding"

    work_id = db.Column(db.BigInteger, db.ForeignKey("mid.work.paper_id"), primary_key=True)
    embedding = db.Column(db.ARRAY(db.Float), nullable=False)


def get_and_save_embeddings(work):
    logger.info(f"adding embeddings for {work.id}")
    abstract = clean_text(work.abstract.abstract) if work.abstract else None
    title = clean_text(work.work_title)
    if title and abstract:
        text_to_process = f"Title: {title}\nAbstract: {abstract}"
    elif work.work_title:
        text_to_process = f"Title: {title}"
    else:
        text_to_process = None

    if not text_to_process:
        logger.info(f"error processing title embeddings for {work.id} - no text to process")
        return None
    elif text_too_short(text_to_process):
        logger.info(f"error processing title embeddings for {work.id} - text too short")
        return None

    max_characters = 25000
    if len(text_to_process) > max_characters:
        print(f"truncating text for {work.id} from {len(text_to_process)} to {max_characters} characters")
        text_to_process = text_to_process[:max_characters]

    response = call_embeddings_api(text_to_process)

    if response.status_code == 200:
        result = get_embeddings_from_response(response)
        save_embeddings_to_db(work.id, result)
    else:
        logger.warn(f"error processing title embeddings for {work.id} - other than 200 response from classifier")


def text_too_short(text):
    word_minimum = 2
    character_minimum = 20
    if len(text.split()) < word_minimum or len(text) < character_minimum:
        return True
    else:
        return False


def clean_text(text):
    if not text:
        return text

    # remove extra whitespace
    text = ' '.join(text.split())

    # remove brackets around title []
    text = text.strip()
    if text.startswith("[") and text.endswith("]"):
        text = text[1:-1]

    return text


class APIError(Exception):
    pass


@retry(
    wait=wait_exponential(multiplier=1, min=2, max=60),
    stop=stop_after_attempt(5),
    retry=retry_if_exception_type(APIError)
)
def call_embeddings_api(text):
    api_key = os.getenv('OPENAI_API_KEY')

    url = "https://api.openai.com/v1/embeddings"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}"
    }

    data = {
        "input": text,
        "model": "text-embedding-3-large",
        "dimensions": 256
    }

    response = requests.post(url, headers=headers, json=data)

    if response.status_code != 200:
        error_message = f"API request failed with status code {response.status_code}: {response.text}"
        print(error_message)
        raise APIError(error_message)
    return response


def get_embeddings_from_response(response):
    result = response.json()["data"][0]["embedding"]
    return result


def save_embeddings_to_db(work_id, result):
    new_record = WorkEmbedding(work_id=work_id, embedding=result)
    db.session.add(new_record)


def generate_actions(works):
    for work in works:
        if work.embeddings and work.embeddings.embeddings:
            action = {
                "_index": "work-vectors-v1",
                "_id": work.openalex_id,
                "_source": {
                    "id": work.openalex_id,
                    "display_name": work.work_title,
                    "cited_by_count": work.counts.citation_count if work.counts else 0,
                    "embeddings": work.embeddings.embeddings
                }
            }
            yield action


def store_embeddings(works):
    es = Elasticsearch(ELASTIC_EMBEDDINGS_URL)
    actions = generate_actions(works)
    helpers.bulk(es, actions)
