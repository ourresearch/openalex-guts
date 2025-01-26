import time
from elasticsearch import Elasticsearch
from app import ELASTIC_URL

es = Elasticsearch([ELASTIC_URL])

""""
Run daily to clean up deleted documents in ElasticSearch.
Command is: heroku local:run -- python -m scripts.clean_elastic_deleted_docs
"""

response = es.cat.indices(format='json', h='index,docs.deleted', s='docs.deleted:desc')

indices_to_clean = [
    i['index'] for i in response if (
        (i['index'].startswith('authors') and int(i['docs.deleted']) > 15000000) or
        (not i['index'].startswith('authors') and int(i['docs.deleted']) > 4000000)
    )
]

for i in range(0, len(indices_to_clean), 3):
    """
    Clean up to 3 indeces per day, waiting one hour between each batch.
    """
    batch = indices_to_clean[i:i + 3]

    for index in batch:
        print(f"Cleaning index {index}")
        es.indices.forcemerge(
            index=index,
            only_expunge_deletes=True,
            wait_for_completion=False
        )
    time.sleep(3600)
