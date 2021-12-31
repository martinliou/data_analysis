from typing import Sequence, Any
import logging

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError


# # for development
# _session = boto3.Session(profile_name='local')
# _db = _session.resource(
#     'dynamodb', endpoint_url='http://localhost:8000')

# for production
_db = boto3.resource('dynamodb')


def query_items_by_pkey(table_name: str, key_name: str, target: str) -> Sequence[dict]:
    table = _db.Table(table_name)

    response = table.query(
        KeyConditionExpression=Key(key_name).eq(target)
    )

    return response['Items']


def query_items_by_pkey_n_sortkey_range(
    table_name: str,
    pkey_name: str,
    pkey_value: str,
    sortkey_name: str,
    sortkey_range: tuple
) -> Sequence[dict]:
    table = _db.Table(table_name)

    (start, end) = sortkey_range

    response = table.query(
        KeyConditionExpression=Key(pkey_name).eq(pkey_value) & Key(
            sortkey_name).between(start, end)
    )

    return response['Items']


def create_table(
    table_name: str,
    key_schema: Sequence[dict],
    attr_defs: Sequence[dict],
    throughput: dict,
    second_ids: Sequence[dict] = None
) -> Any:
    if second_ids is None:
        second_ids = list()

    return _db.create_table(
        TableName=table_name,
        KeySchema=key_schema,
        AttributeDefinitions=attr_defs,
        LocalSecondaryIndexes=second_ids,
        ProvisionedThroughput=throughput
    )


def delete_table(table_name: str) -> None:
    table = _db.Table(table_name)
    table.delete()


def put_items(table_name: str, items: Sequence[dict]) -> int:
    table = _db.Table(table_name)
    count = 0

    with table.batch_writer() as batch:
        for item in items:
            try:
                batch.put_item(item)

                count += 1
            except ClientError as e:
                logging.exception(f'db)put_items error = {e}')

    return count


def delete_items(table_name: str, pkey_name: str, sortkey_name: str, items: Sequence[dict]) -> int:
    table = _db.Table(table_name)
    count = 0

    with table.batch_writer() as batch:
        for item in items:
            try:
                pkey_value = item.get(pkey_name)
                sortkey_value = item.get(sortkey_name)

                batch.delete_item(
                    Key={pkey_name: pkey_value, sortkey_name: sortkey_value})

                count += 1
            except ClientError as e:
                logging.exception(f'db)delete_items error = {e}')

    return count
