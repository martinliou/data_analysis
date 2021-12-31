from pprint import pprint

import db

TABLE_NAME = 'market_announcement'
KEY_NAME = 'COID'
KEY_VALUE = 'TPEX'


if __name__ == '__main__':
    print(f"{TABLE_NAME} from {KEY_VALUE}")
    for item in db.query_items_by_pkey(TABLE_NAME, KEY_NAME, KEY_VALUE):
        pprint(item)
