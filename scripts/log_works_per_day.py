import datetime
import os

import requests
from app import db, logger
from sqlalchemy import text

api_key = os.getenv('OPENALEX_API_KEY')

yesterday = (
    datetime.datetime.utcnow().date() - datetime.timedelta(days=1)
).isoformat()

num_created = requests.get(
    f'https://api.openalex.org/works?filter=from_created_date:{yesterday}&api_key={api_key}'
).json().get('meta').get('count')

num_updated = requests.get(
    f'https://api.openalex.org/works?filter=from_updated_date:{yesterday}&api_key={api_key}'
).json().get('meta').get('count')

logger.info({'day': yesterday, 'created': num_created, 'updated': num_updated})

db.engine.execute(
    text('insert into log.works_per_day (day, num_created, num_updated) values (:day, :created, :updated)').bindparams(
        day=yesterday, created=num_created, updated=num_updated
    ).execution_options(autocommit=True)
)
