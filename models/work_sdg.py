import requests
from app import db, logger, SDG_CLASSIFIER_URL


class WorkSDG(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "work_sdg"

    paper_id = db.Column(db.BigInteger, db.ForeignKey("mid.work.paper_id"), primary_key=True)
    predictions = db.Column(db.JSON)


def get_and_save_sdgs(work):
    logger.info(f"adding sdgs for {work.id}")
    text_to_process = get_text_for_sdg_classification(work)

    if not text_to_process:
        logger.info(f"error processing sdgs for {work.id} - no text to process")
        return None

    # call the API
    url = SDG_CLASSIFIER_URL
    data = {"text": text_to_process}
    response = requests.post(url, json=data)

    if response.status_code == 200:
        result = response.json()
        result_modified = process_api_response(result)
        save_sdgs_to_db(work.id, result_modified)
    else:
        logger.warn(f"error processing sdgs for {work.id} - other than 200 response from classifier")


def get_text_for_sdg_classification(work):
    if work.abstract and work.abstract.abstract and work.work_title:
        return work.work_title + " " + work.abstract.abstract
    elif not work.abstract and work.work_title:
        return work.work_title
    elif work.abstract and work.abstract.abstract and work.work_title is None:
        return work.abstract.abstract
    else:
        return None


def process_api_response(result):
    modified = []
    for item in result:
        item["sdg"]["id"] = item["sdg"]["id"].replace("http://", "https://")
        modified.append(item)
    return sorted(modified, key=lambda x: x["prediction"], reverse=True)


def save_sdgs_to_db(paper_id, result_sorted):
    existing_record = WorkSDG.query.filter_by(paper_id=paper_id).first()

    if existing_record:
        existing_record.predictions = result_sorted
    else:
        new_record = WorkSDG(paper_id=paper_id, predictions=result_sorted)
        db.session.add(new_record)

    db.session.commit()
