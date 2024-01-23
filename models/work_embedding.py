import requests
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy import Float
from elasticsearch import Elasticsearch, helpers

from app import db, logger, ELASTIC_URL, ELASTIC_EMBEDDINGS_URL


class WorkEmbedding(db.Model):
    """
    This model stores the dense vector embeddings that power semantic search.
    """
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "work_vector_embedding"

    paper_id = db.Column(db.BigInteger, db.ForeignKey("mid.work.paper_id"), primary_key=True)
    embedding = db.Column(ARRAY(Float))


def get_and_save_embeddings(work):
    logger.info(f"adding title embeddings for {work.id}")
    text_to_process = work.work_title

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
    """
    Use the minilm-l12-v2 model to create embeddings.
    """
    url = f"{ELASTIC_URL}/_ml/trained_models/sentence-transformers__all-minilm-l12-v2/_infer"
    data = {"docs": [{"text_field": text}]}
    response = requests.post(url, json=data)
    return response


def get_embeddings_from_response(response):
    result = response.json()["inference_results"][0]["predicted_value"]
    return result


def save_embeddings_to_db(paper_id, result):
    existing_record = WorkEmbedding.query.filter_by(paper_id=paper_id).first()

    if existing_record:
        existing_record.embeddings = result
    else:
        new_record = WorkEmbedding(paper_id=paper_id, embedding=result)
        db.session.add(new_record)

    db.session.commit()


def generate_actions(works):
    for work in works:
        if work.embeddings and work.embeddings.embedding:
            action = {
                "_index": "work-vectors-v1",
                "_id": work.openalex_id,
                "_source": {
                    "id": work.openalex_id,
                    "display_name": work.work_title,
                    "cited_by_count": work.counts.citation_count if work.counts else 0,
                    "vector_embedding": work.embeddings.embedding
                }
            }
            yield action


def store_embeddings(works):
    es = Elasticsearch(ELASTIC_EMBEDDINGS_URL)
    actions = generate_actions(works)
    helpers.bulk(es, actions)
