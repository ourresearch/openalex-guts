from typing import List, Union
from app import db
from sqlalchemy import text
from sqlalchemy.engine.row import Row


def query_recordthresher_record_by_doi(doi: str) -> List[Row]:
    sq = """select * from ins.recordthresher_record where doi = :doi order by id"""
    params = {
        "doi": doi,
    }
    result = db.session.execute(text(sq), params).all()
    return result


def query_work_by_work_id(work_id: int) -> Union[Row, None]:
    sq = """select * from mid.work where paper_id = :work_id"""
    params = {
        "work_id": work_id,
    }
    return db.session.execute(text(sq), params).one_or_none()


def work_id_from_recordthresher_result(r: List[Row]) -> Union[int, None]:
    if not r:
        return None
    for row in r:
        if row["work_id"] and row["work_id"] > 0:
            return row["work_id"]
    return None
