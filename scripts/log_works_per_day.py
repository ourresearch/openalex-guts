import datetime
import os

import requests
from app import db, logger
from sqlalchemy import text

def send_email(to_address, subject, body):
    mailgun_api_key = os.getenv('MAILGUN_API_KEY')
    mailgun_url = f"https://api.mailgun.net/v3/ourresearch.org/messages"
    mailgun_auth = ("api", mailgun_api_key)
    mailgun_data = {
        "from": "OurResearch Mailgun <mailgun@ourresearch.org>",
        "to": [to_address],
        "subject": subject,
        "text": body,
    }
    requests.post(mailgun_url, auth=mailgun_auth, data=mailgun_data)


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

if int(num_created) == 0 or int(num_updated) == 0:
    # send warning email
    to_address = "dev@ourresearch.org"
    logger.info(f"sending email alert to {to_address}")
    subject = f"ALERT OpenAlex No Created or Updated Works ({num_created} created, {num_updated} updated yesterday)"
    body = '\n'.join([f'Filtering by created_date:{yesterday}, there were {num_created} works\n',
            f'Filtering by updated_date:{yesterday}, there were {num_updated} works\n',
            '\nThis is an automated alert sent out by the `log_works_per_day.py` script if there are either no created works or no updated works.\n',
        ])
    send_email(to_address, subject, body)
    logger.info('sent email')