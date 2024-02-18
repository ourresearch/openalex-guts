import datetime

from app import logger
from util import entity_md5


def create_bulk_actions(entity, index_name):
    bulk_actions = []

    my_dict = entity.to_dict()
    my_dict['updated'] = my_dict.get('updated_date')
    my_dict['@timestamp'] = datetime.datetime.utcnow().isoformat()
    new_entity_hash = entity_md5(my_dict)
    old_entity_hash = entity.json_entity_hash

    if new_entity_hash != old_entity_hash:
        logger.info(f"dictionary for {entity.id} new or changed, so save again")
        index_record = {
            "_op_type": "index",
            "_index": index_name,
            "_id": entity.id,
            "_source": my_dict
        }
        bulk_actions.append(index_record)
    else:
        logger.info(f"dictionary not changed, don't save again {entity.id}")
    return bulk_actions, new_entity_hash
