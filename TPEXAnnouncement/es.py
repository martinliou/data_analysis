from typing import Sequence, Callable, TypeVar
from elasticsearch import helpers, Elasticsearch, ElasticsearchException
from env import ES_HOSTS

_T = TypeVar('_T')

es = Elasticsearch(ES_HOSTS)


def update(index: str, pker: Callable[[_T], str], items: Sequence[_T]):
    try:
        helpers.bulk(es, [
            {
                '_op_type': 'update',
                '_index': index,
                '_id': pker(d),
                'doc_as_upsert': True,
                "retry_on_conflict": 3,
                'doc': d
            } for d in items
        ])
    except ElasticsearchException as e:
        print("es error: ", e)
