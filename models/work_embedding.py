import requests

from app import db, logger, ELASTIC_URL


class WorkEmbedding(db.Model):
    """
    This model stores the sparse vector embeddings that power semantic search.
    """
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "work_embedding"

    paper_id = db.Column(db.BigInteger, db.ForeignKey("mid.work.paper_id"), primary_key=True)
    embeddings = db.Column(db.JSON)


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
        result = response.json()["predicted_value"]
        save_embeddings_to_db(work.id, result)
    else:
        logger.warn(f"error processing title embeddings for {work.id} - other than 200 response from classifier")


def text_too_short(text):
    word_minimum = 5
    character_minimum = 25
    if len(text.split()) < word_minimum or len(text) < character_minimum:
        return True
    else:
        return False


def call_embeddings_api(text):
    """
    Use the ELSER v2 model from elasticsearch to create embeddings.
    """
    url = f"{ELASTIC_URL}/_inference/sparse_embedding/.elser_model_2_linux-x86_64"
    data = {"input": text}
    response = requests.post(url, json=data)
    return response


def save_embeddings_to_db(paper_id, result):
    existing_record = WorkEmbedding.query.filter_by(paper_id=paper_id).first()

    if existing_record:
        existing_record.embeddings = result
    else:
        new_record = WorkEmbedding(paper_id=paper_id, embeddings=result)
        db.session.add(new_record)

    db.session.commit()
