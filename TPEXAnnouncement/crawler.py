# -*- coding: utf-8 -*-
from urllib.request import Request, urlopen
from urllib.parse import urlencode
from urllib.error import URLError, HTTPError
from datetime import date, datetime, timedelta
from typing import Optional

import json
import hashlib
import es

import logging
import math
import os
import ssl
import pytz
import re
import sys
import time

import db

from bs4 import BeautifulSoup
from bs4.element import Tag

# disable ssl verification
if (not os.environ.get('PYTHONHTTPSVERIFY', '') and
        getattr(ssl, '_create_unverified_context', None)):
    ssl._create_default_https_context = ssl._create_unverified_context

# constants and utility objects
PKEY_NAME = 'COID'
SORTKEY_NAME = 'reference_no'
PKEY_VALUE = 'TPEX'
TABLE_NAME = 'market_announcement'

REQUEST_CODING = 'utf-8'
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.87 Safari/537.36'}
BASE_URL = 'https://www.tpex.org.tw/web/bulletin/announcement/'
QUERY_URL = BASE_URL + 'announcement_result.php?l=zh-tw'
MAX_QUERY_DAYS = 60

SLEEP_GAP = 2

LOG_FORMAT = '%(asctime)s %(levelname)s %(lineno)s %(message)s'

QUERY_DATE_FORMAT = '{:03d}/{:02d}/{:02d}'

RE_CONTROL_CHARACTERS = re.compile(r'[\n\r\t]')
RE_DATE_FORMAT = re.compile(r'^(\d{4})(\d{2})(\d{2})$')
RE_ROC_DATE_FORMAT = re.compile(
    r'^(\d{3})/(\d{2})/(\d{2})$')
RE_QUERY_DATE_FORMAT = re.compile(r'^(\d{3})/(\d{2})/(\d{2})$')
RE_PDF_FILE_PATH = re.compile(r'^/js/pdf/viewer\.html\?file=(.*)$')
RE_LEVEL_FORMAT = re.compile(r'^eb_level_(\d{1})$')

TAIPEI_TIMEZONE = pytz.timezone("Asia/Taipei")

RULE = {
    'table': TABLE_NAME,
    'keys': ['COID', 'timestamp', 'reference_no']
}


def date_to_query_date_str(d: date) -> str:
    return QUERY_DATE_FORMAT.format(d.year - 1911, d.month, d.day)


def arg_to_query_date_str(text: str) -> str:
    found = RE_DATE_FORMAT.findall(text)
    assert len(found) == 1, f'arg_to_date_str)Unknown date format {text}'

    (year, month, day) = found[0]

    return date_to_query_date_str(
        TAIPEI_TIMEZONE.localize(
            datetime(
                int(year),
                int(month),
                int(day))))


def query_date_str_to_datetime(text: str) -> datetime:
    found = RE_QUERY_DATE_FORMAT.findall(text)
    assert len(
        found) == 1, f'query_date_str_to_datetime)Unknown date format {text}'

    (year, month, day) = found[0]

    return TAIPEI_TIMEZONE.localize(
        datetime(
            int(year) + 1911,
            int(month),
            int(day)))


def roc_to_datetime(roc: str) -> datetime:
    found = RE_ROC_DATE_FORMAT.findall(roc)
    assert len(found) == 1, f'roc_to_datetime)Unknown roc format {roc}'

    (year, month, day) = found[0]

    return TAIPEI_TIMEZONE.localize(datetime(int(year) + 1911, int(month),
                                             int(day)))


def datetime_to_timestamp(dt: datetime) -> int:
    return int(dt.timestamp() * 1000000)


def parse_html(text: str) -> BeautifulSoup:
    return BeautifulSoup(text, 'html.parser')


def parse_summary(html: BeautifulSoup) -> list:
    items = []

    table_tag = html.find('table', {'class': 'table-board'})
    assert table_tag, 'parse_summary)cannot get summary table tag'

    trs = table_tag.find_all('tr')
    assert trs and len(trs) > 0, 'parse_summary)summary table tag struct error'

    for tr in trs[1:]:  # bypass table header
        tds = tr.find_all('td')
        assert tds and len(tds) == 5, f'parse_summary)tr structure error, {tr}'

        roc = tds[1].get_text()
        assert roc, 'parse_summary)text of roc is empty'
        timestamp = datetime_to_timestamp(roc_to_datetime(roc))

        reference_no = tds[2].get_text()
        assert roc, 'parse_summary)text of reference no is empty'

        a_tag = tds[4].find('a')
        assert a_tag, f'parse_summary)td structure error, {tds[4]}'
        href = a_tag.get('href')
        assert href, f'parse_summary)link tag struct error, {a_tag}'

        items.append([timestamp, reference_no, BASE_URL + href])

    return items


def find_tag_content(tag: Tag, path: str = None) -> Optional[str]:
    if not path:
        return tag.get_text().strip()

    target = tag.find(path)
    assert target, f'find_tag_content)Cannot find {path} in {tag}'

    return target.get_text().strip()


def parse_paragraphs(ps: Tag) -> list:
    paragraphs = []

    paras = ps.find_all('p')
    if paras and len(paras) > 0:
        for paragraph in paras:
            c = paragraph.get('class')
            assert c and len(
                c) > 0, f'parse_paragraphs)Unknown tag structure {paragraph}'

            c = c[0]
            found = RE_LEVEL_FORMAT.findall(c)
            assert len(
                found) == 1, f'parse_paragraphs)Unknown level format {paragraph}'

            (level) = found[0]

            paragraphs.append(
                (int(level), find_tag_content(paragraph)))
    else:
        # only one main paragraph
        paragraphs.append((0, find_tag_content(ps)))

    return paragraphs


def parse_html_content_tag(html: BeautifulSoup) -> tuple:
    title = None
    basis = None
    paragraphs = None

    tag = html.find('table', {'class': 'table-board', 'summary': '內容列表'})
    assert tag, f'parse_html_content_tag)failed to get html content tag'

    trs = tag.find_all('tr')
    assert trs and len(
        trs) >= 2, 'parse_html_content_tag)table tag struct error'

    # parse title, basis, paragraphs
    for tr in trs:
        key = find_tag_content(tr, 'th')
        assert key, 'parse_html_content_tag)key is empty'

        if key == '主　　　旨：':
            title = find_tag_content(tr, 'td')
            assert title, 'parse_html_content_tag)title is empty'
        elif key == '依　　　據：':
            basis = find_tag_content(tr, 'td')
            assert basis, 'parse_html_content_tag)basis is empty'
        elif key == '公 告 事 項：':
            ps = tr.find('td')
            assert ps, 'parse_html_content_tag)tr tag struct error'

            paragraphs = parse_paragraphs(ps)
        else:
            continue  # bypass other row detail

    return (title, basis, paragraphs)


def do_request(url: str, data: Optional[dict]) -> Optional[BeautifulSoup]:
    time.sleep(SLEEP_GAP)

    encoded_data = urlencode(data).encode(REQUEST_CODING) if data else None

    req = Request(url, encoded_data, headers=HEADERS)
    try:
        response = urlopen(req)

        return parse_html(response.read().decode(REQUEST_CODING))
    except HTTPError as e:
        logging.exception(
            f'The server couldn\'t fulfill the request. Error code: {e.code}')
    except URLError as e:
        logging.exception(f'We failed to reach a server. Reason: {e.reason}')
    except Exception as e:
        logging.exception(f'Unknown error: {e}')


def query_content(url) -> tuple:
    html = do_request(url, None)
    assert html, f'query_content)failed to get html, {url}'

    return parse_html_content_tag(html)


def query_summary(start: str, end: str) -> list:
    rows = []

    start_dt = query_date_str_to_datetime(start)
    end_dt = query_date_str_to_datetime(end)
    max_end_dt = start_dt + timedelta(days=(MAX_QUERY_DAYS - 1))

    if end_dt > max_end_dt:
        rows += query_summary(date_to_query_date_str(max_end_dt +
                                                     timedelta(days=1)), end)

        end = date_to_query_date_str(max_end_dt)

    conditions = {
        'input_date_start': start,
        'input_date_end': end,
        'p_type': 'all'}
    logging.info(f'query_summary)conditions = {conditions} ')

    html = do_request(QUERY_URL, conditions)
    assert html, f'query_summary)failed to get html, {QUERY_URL} conditions = {conditions}'

    # parse this page
    rows = parse_summary(html)

    # query content by rows
    for row in rows:
        row.append(query_content(row[2]))

    return rows


def paragraph_to_list(t: tuple) -> list:
    (level, content) = t

    return [level, content]


def put_items(items, client=None):
    if not client:
        client = boto3.resource('dynamodb')
    table = client.Table(TABLE_NAME)

    failed_count = 0
    with table.batch_writer() as batch:
        for item in items:
            (timestamp, reference_no, _link, detail) = item
            (title, basis, paragraphs) = detail

            item = {
                PKEY_NAME: PKEY_VALUE,
                SORTKEY_NAME: reference_no,
                'timestamp': timestamp,
                'title': title,
                'basis': basis
            }
            # paragraphs is optional
            if paragraphs:
                item['paragraphs'] = list(map(paragraph_to_list, paragraphs))

            try:
                batch.put_item(Item=item)
                logging.warning(
                    f'insert to {TABLE_NAME}){PKEY_NAME} = [{PKEY_VALUE}], {SORTKEY_NAME} = [{reference_no}], timestamp = [{timestamp}], title = [{title}]')
            except ClientError as e:
                failed_count += 1
                logging.exception(f'put_items)update item error = {e}')

    logging.warning(
        f'put_items)update row count = {len(items)}({failed_count})')


def transform(type, rule, row, author, source):
    basis = ''
    if 'basis' in row.keys():
        basis = row['basis']

    url = ''
    if 'url' in row.keys():
        url = row['url']

    detail_item = ''
    if 'paragraphs' in row.keys():
        for p in row['paragraphs']:
            if isinstance(p, str):
                detail_item += '<p>' + p + '</p>'
            else:
                detail_item += '<p>' + p[1] + '</p>'

    return {
        'id': encode_key(type, rule, row),
        'count_numbers': 0,
        'guid': '',
        'title': row['title'],
        'create_time': datetime.utcfromtimestamp(int(row['timestamp'] / 1000000)).strftime('%Y-%m-%dT%H:%M:%S.000'),
        'publish_time': datetime.utcfromtimestamp(int(row['timestamp'] / 1000000)).strftime('%Y-%m-%dT%H:%M:%S.000'),
        'cover_item': '',
        'author': author,
        'basis': basis,
        'type': type,
        'status': 1,
        'source': source,
        'url': url,
        'detail_item': detail_item,
    }


def encode_key(type, rule, row) -> str:
    q = '_'.join([str(row[key]) for key in rule['keys']])
    return 'T' + str(type) + hashlib.md5(q.encode()).hexdigest()[0:22]


def get_id(row):
    return row['id']


def deserializer(row):
    return json.dumps(row).encode()


if __name__ == '__main__':
    logging.basicConfig(level=logging.WARNING, format=LOG_FORMAT)

    # parse date range from arguments
    start = None
    end = None
    if len(sys.argv) > 2:
        start = arg_to_query_date_str(sys.argv[1])
        end = arg_to_query_date_str(sys.argv[2])
    else:
        today = date.today()
        yesterday = today - timedelta(days=1)

        start = date_to_query_date_str(yesterday)
        end = date_to_query_date_str(today)

    items = []
    for row in query_summary(start, end):
        (timestamp, reference_no, link, detail) = row
        (title, basis, paragraphs) = detail

        item = {
            PKEY_NAME: PKEY_VALUE,
            SORTKEY_NAME: reference_no,
            'timestamp': timestamp,
            'title': title,
            'url': link,
            'basis': basis
        }
        # paragraphs is optional
        if paragraphs:
            item['paragraphs'] = list(map(paragraph_to_list, paragraphs))

        items.append(item)
    logging.warning(items)

    db.put_items(TABLE_NAME, items)
    logging.warning('db uploaded.')

    # upload to es
    es_items = []
    for item in items:
        es_items.append(transform(8, RULE, item, '證券櫃檯買賣中心', 10))

    es.update('news', get_id, es_items)
    logging.warning('es uploaded.')
