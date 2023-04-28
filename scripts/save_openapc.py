import codecs
import csv

from currency_converter import CurrencyConverter
import requests
from sqlalchemy.sql import text

from app import db
from models.work_openapc import WorkOpenAPC


"""
Ingest data from OpenAPC (https://github.com/OpenAPC/openapc-de) into the mid.work_openapc table.
run with: python -m scripts.save_openapc
"""


def get_existing_dois():
    existing_dois_query = WorkOpenAPC.query.with_entities(WorkOpenAPC.doi).all()
    existing_dois = [doi[0] for doi in existing_dois_query]
    return existing_dois


def fetch_openapc_csv_data():
    url = "https://raw.githubusercontent.com/OpenAPC/openapc-de/master/data/apc_de.csv"
    response = requests.get(url)
    lines = codecs.iterdecode(response.iter_lines(), 'utf-8')
    return csv.DictReader(lines, delimiter=',', quotechar='"')


def get_paper_id(doi):
    paper_id = None
    doi_lower = doi.lower()
    sql_command = text("""
            SELECT paper_id FROM mid.work WHERE doi_lower = :doi_lower LIMIT 1
        """)
    result = db.engine.execute(sql_command, doi_lower=doi_lower)
    for row in result:
        paper_id = row[0]
    return paper_id


def convert_euro_to_usd(apc_in_euro):
    c = CurrencyConverter()
    apc_in_usd_converted = c.convert(apc_in_euro, 'EUR', 'USD')
    apc_in_usd = int(apc_in_usd_converted)
    return apc_in_usd


def commit_every_1000(count):
    if count != 0 and count % 1000 == 0:
        db.session.commit()
        print(f"committed at {count}")


def save_new_apcs():
    count = 0
    existing_dois = get_existing_dois()
    reader = fetch_openapc_csv_data()

    for row in reader:
        doi = row['doi']
        if not doi or doi in existing_dois or not doi.startswith('10.'):
            continue

        year = row['period']
        apc_in_euro = int(float(row['euro']))
        paper_id = get_paper_id(doi)
        apc_in_usd = convert_euro_to_usd(apc_in_euro)

        if paper_id:
            apc = WorkOpenAPC(
                paper_id=paper_id,
                doi=doi,
                year=year,
                apc_in_euro=apc_in_euro,
                apc_in_usd=apc_in_usd
            )
            db.session.add(apc)
            print(f"inserted {paper_id} with values {doi}, {year}, {apc_in_euro}, {apc_in_usd}")
            count += 1

        commit_every_1000(count)

    db.session.commit()
    print(f"saved {count} new apcs")


if __name__ == '__main__':
    save_new_apcs()
