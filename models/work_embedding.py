import os

import requests
from elasticsearch import Elasticsearch, helpers

from app import db, logger, ELASTIC_EMBEDDINGS_URL


class WorkEmbedding(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "work_embedding"

    work_id = db.Column(db.BigInteger, db.ForeignKey("mid.work.paper_id"), primary_key=True)
    embedding = db.Column(db.ARRAY(db.Float), nullable=False)


def get_and_save_embeddings(work):
    logger.info(f"adding embeddings for {work.id}")
    if work.work_title and work.abstract:
        text_to_process = f"title: {work.work_title} abstract: {work.abstract}"
    elif work.work_title:
        text_to_process = f"title: {work.work_title}"
    else:
        text_to_process = None

    if not text_to_process:
        logger.info(f"error processing title embeddings for {work.id} - no text to process")
        return None
    elif text_too_short(text_to_process):
        logger.info(f"error processing title embeddings for {work.id} - text too short")
        return None

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
