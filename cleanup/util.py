# -*- coding: utf-8 -*-

DESCRIPTION = """util for cleanup"""

import sys, os, time
import re
import json
from pathlib import Path
from datetime import datetime
from typing import Union, Optional
from timeit import default_timer as timer

try:
    from humanfriendly import format_timespan
except ImportError:

    def format_timespan(seconds):
        return "{:.2f} seconds".format(seconds)


import logging

root_logger = logging.getLogger()
logger = root_logger.getChild(__name__)

import requests
import backoff
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search


ENTITY_TYPES = {
    "W": "work",
    "A": "author",
    "S": "source",
    "P": "publisher",
    "F": "funder",
    "I": "institution",
    "C": "concept",
    "V": "venue",  # deprecated
}


class OpenAlexID:
    def __init__(
        self, id_in_unknown_form: Union[str, int], entity_type: Optional[str] = None
    ) -> None:
        if hasattr(id_in_unknown_form, "openalex_id"):
            # pass through if OpenAlexID is initialized with an instance of OpenAlexID already
            return id_in_unknown_form
        self.ENTITY_TYPES_PREFIX_TO_NAME = ENTITY_TYPES
        self.ENTITY_TYPES_NAME_TO_PREFIX = {
            v: k for k, v in self.ENTITY_TYPES_PREFIX_TO_NAME.items()
        }
        id_in_unknown_form = str(id_in_unknown_form)
        if id_in_unknown_form.isnumeric():
            if entity_type is None:
                raise ValueError("Numeric IDs must specify an entity_type")
            self.validate_entity_type()
            self.id_int = int(id_in_unknown_form)
            self.entity_type = self.normalize_entity_type(entity_type)
        else:
            if entity_type is not None:
                logger.warning(f"ignoring specified entity_type: {entity_type}")
            self.entity_type, self.id_int = self.normalize_openalex_id(
                id_in_unknown_form
            )

    def __str__(self) -> str:
        return self.id

    def __repr__(self) -> str:
        return f'OpenAlexID("{self.id})"'

    @property
    def entity_prefix(self) -> str:
        return self.ENTITY_TYPES_NAME_TO_PREFIX[self.entity_type]

    @property
    def openalex_id(self) -> str:
        return f"https://openalex.org/{self.id_short}"

    @property
    def id_short(self) -> str:
        return f"{self.entity_prefix}{self.id_int}"

    @property
    def id(self) -> str:
        return self.openalex_id

    # Inspired by openalex-elastic-api/core/utils.py
    def normalize_openalex_id(self, openalex_id):
        if not openalex_id:
            raise ValueError
        openalex_id = openalex_id.strip().upper()
        valid_prefixes = "".join(self.ENTITY_TYPES_PREFIX_TO_NAME.keys())
        p = re.compile(f"([{valid_prefixes}]\d{{2,}})")
        matches = re.findall(p, openalex_id)
        if len(matches) == 0:
            raise ValueError
        clean_openalex_id = matches[0]
        clean_openalex_id = clean_openalex_id.replace("\0", "")
        prefix = clean_openalex_id[0]
        id_int = int(clean_openalex_id[1:])
        return self.normalize_entity_type(prefix), id_int

    def validate_entity_type(self, entity_type: str):
        entity_type_prefixes = [
            e.upper() for e in self.ENTITY_TYPES_PREFIX_TO_NAME.keys()
        ]
        entity_type_names = [
            e.upper() for e in self.ENTITY_TYPES_PREFIX_TO_NAME.values()
        ]
        valid_entity_types = entity_type_prefixes + entity_type_names
        if not entity_type or entity_type.upper() not in valid_entity_types:
            raise ValueError(f"{entity_type} is not a valid entity type")

    def normalize_entity_type(self, entity_type: str):
        self.validate_entity_type(entity_type)
        try:
            return self.ENTITY_TYPES_PREFIX_TO_NAME[entity_type.upper()]
        except KeyError:
            return entity_type.lower()


def paginate_es(s: Search, page_size=1000):
    s = s.extra(size=page_size)
    response = s.execute()
    while len(response.hits) > 0:
        for hit in response:
            yield hit
        last_hit_sort = response.hits.hits[-1]["sort"]
        s = s.extra(size=page_size, search_after=last_hit_sort)
        response = s.execute()


@backoff.on_exception(backoff.expo, requests.exceptions.RequestException, max_time=30)
@backoff.on_predicate(backoff.expo, lambda x: x.status_code >= 429, max_time=30)
def make_request(url, params=None, debug=False):
    if debug:
        print(url, params)
    if params is None:
        return requests.get(url)
    else:
        return requests.get(url, params=params)


def paginate_openalex(url, params=None, per_page=200, debug=False):
    if params is None:
        params = {}
    if "per-page" not in params and per_page:
        params["per-page"] = per_page
    cursor = "*"
    while cursor:
        params["cursor"] = cursor
        r = make_request(url, params, debug=debug)
        yield r

        page_with_results = r.json()
        # update cursor to meta.next_cursor
        cursor = page_with_results["meta"]["next_cursor"]


def entities_by_ids(
    id_list, api_endpoint="works", filterkey="openalex", chunksize=100, params=None, debug=False
):
    if params is None:
        params = {}
    params["per-page"] = chunksize
    existing_filter = params.get("filter")
    for i in range(0, len(id_list), chunksize):
        chunk = id_list[i : i + chunksize]
        url = f"https://api.openalex.org/{api_endpoint}"
        chunk_str = "|".join(chunk)
        if existing_filter:
            params["filter"] = existing_filter + f",{filterkey}:{chunk_str}"
        else:
            params["filter"] = f"{filterkey}:{chunk_str}"
        yield make_request(url, params=params, debug=debug)


def openalex_entities_by_ids(id_list, chunksize=100, params=None):
    id_list = [OpenAlexID(oid) for oid in id_list]
    if params is None:
        params = {}
    params["per-page"] = chunksize
    entity_type = set([item.entity_type for item in id_list])
    if not len(entity_type) == 1:
        raise RuntimeError("all ids in in id_list must be the same entity type")
    entity_type = list(entity_type)[0]
    api_endpoint = f"{entity_type}s"
    ids_short = [item.id_short for item in id_list]
    for r in entities_by_ids(
        ids_short,
        api_endpoint,
        filterkey="openalex",
        chunksize=chunksize,
        params=params,
    ):
        yield r


def change_source_type(
    source_id,
    new_source_type,
    old_source_type="journal",
    note="",
    session=None,
    commit=True,
):
    if session is None:
        from app import db

        session = db.session
    from sqlalchemy import text

    if type(source_id) != int:
        source_id = OpenAlexID(source_id).id_int
    now = datetime.utcnow().isoformat()
    sq = "UPDATE mid.journal SET type = :new_source_type, updated_date = :now WHERE journal_id = :journal_id"
    session.execute(
        text(sq),
        {"journal_id": source_id, "new_source_type": new_source_type, "now": now},
    )

    sq = """
    INSERT INTO logs.source_type_rename
    (source_id, type_old, type_new, updated_at, note)
    VALUES(:source_id, :old_source_type, :new_source_type, :now, :note);
    """
    session.execute(
        text(sq),
        {
            "source_id": source_id,
            "old_source_type": old_source_type,
            "new_source_type": new_source_type,
            "now": now,
            "note": note,
        },
    )

    if commit is True:
        session.commit()


def get_publisher_id(original_publisher):
    if not original_publisher:
        return None
    original_publisher = original_publisher.replace("&", "").replace(",", "")
    r = make_request(f"https://api.openalex.org/publishers?search={original_publisher}")
    results = r.json()["results"]
    if results:
        return int(results[0]["id"].replace("https://openalex.org/P", ""))
    else:
        return None


def get_apc_usd(apc_prices):
    from currency_converter import CurrencyConverter
    # first check if USD is already there
    for item in apc_prices:
        if item.get('currency') and item['currency'].upper() == 'USD':
            if 'price' in item:
                return item['price']
    # otherwise, convert first item
    c = CurrencyConverter()
    currency = apc_prices[0].get('currency', None)
    price = apc_prices[0].get('price', None)
    if currency is not None and price is not None:
        apc_in_usd_converted = c.convert(price, currency.upper(), 'USD')
        apc_in_usd = int(apc_in_usd_converted)
        return apc_in_usd
    else:
        return None

def add_new_source_to_db(
    issn_list,
    display_name,
    source_type="journal",
    publisher_str=None,
    publisher_id=None,
    webpage=None,
    alternate_titles=None,
    country_code=None,
    apc_prices=None,
    apc_usd=None,
    session=None,
    commit=True,
    check_for_existing=True,
):
    # adds a new source to mid.journal table (the table that contains Sources)
    # issn_list is a list of strings
    if session is None:
        from app import db

        session = db.session
    import models
    from sqlalchemy.orm import Load

    for issn in issn_list:
        if check_for_existing is True:
            existing_journals = (
                session.query(models.Source)
                .options(Load(models.Source).lazyload('*'))
                .filter(models.Source.issns.contains(issn))
                .all()
            )
            if existing_journals:
                raise RuntimeError(f"Source with issn {issn} already exists in database")
    if apc_usd is None and apc_prices is not None and len(apc_prices) > 0:
        apc_usd = get_apc_usd(apc_prices)
    now = datetime.utcnow().isoformat()
    new_db_source = models.Source(
        display_name=display_name,
        issns=json.dumps(issn_list),
        type=source_type,
        issns_text_array=issn_list,
        issn=issn_list[0],
        webpage=webpage,
        alternate_titles=alternate_titles,
        country_code=country_code,
        apc_prices=apc_prices,
        apc_usd=apc_usd,
        created_date=now,
        updated_date=now,
    )
    if publisher_str:
        new_db_source.publisher = publisher_str
        new_db_source.original_publisher = publisher_str
    new_db_source.publisher_id = publisher_id or get_publisher_id(publisher_str)
    session.add(new_db_source)
    if commit is True:
        session.commit()
