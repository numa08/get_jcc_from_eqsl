import sqlite3
import requests
import ssl
import urllib3
from bs4 import BeautifulSoup
import json
from config import DATABASE_PATH
import pandas as pd
import re
import time

class CustomHttpAdapter (requests.adapters.HTTPAdapter):
    def __init__(self, ssl_context=None, **kwargs):
        self.ssl_context = ssl_context
        super().__init__(**kwargs)
 
    def init_poolmanager(self, connections, maxsize, block=False):
        self.poolmanager = urllib3.poolmanager.PoolManager(
            num_pools=connections, maxsize=maxsize,
            block=block, ssl_context=self.ssl_context)
 
def get_url(url='https://www.google.co.jp/'):
    session = requests.session()
    ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
    ctx.options |= 0x4
    session.mount('https://', CustomHttpAdapter(ctx))
    res = session.get(url)
    return res

# データベースへの接続を設定する関数
def connect_db(db_path):
    conn = sqlite3.connect(db_path)
    return conn

# 条件に合う行を取得する関数
def fetch_rows(db_conn):
    cursor = db_conn.cursor()
    query = """
    SELECT qsoid, callsign, contactreferences
    FROM Log
    WHERE qsoconfirmations LIKE '%\"CT\":\"EQSL\",\"S\":\"Yes\",\"R\":\"Yes\"%' AND
    contactreferences NOT LIKE '%\"AC\":\"JCC\"%' AND
    callsign NOT LIKE '%/%' AND
    country LIKE '%Japan%'
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    return rows

# 総務省APIから市区町村名を抽出し、JCCを検索する関数
def fetch_jcc_number(callsign):
    url = f'https://www.tele.soumu.go.jp/musen/list?ST=1&DA=1&SC=1&DC=1&OF=2&OW=AT&MA={callsign}'
    response = get_url(url)
    # 200OK以外の場合はエラーを出力する
    if response.status_code != 200:
        return None
    # レスポンスのjsonに変換する
    json_data = json.loads(response.text)
    if json_data['musen'] is None or json_data['musen'] == []:
        return None
    station = json_data['musen'][0]
    location = station['detailInfo']['radioEuipmentLocation']
    # 県名と市名を分離して、市名を抽出する
    pattern = r'([一-龥]+[都道府県])([一-龥]+(市|区|町|村))'
    match = re.search(pattern, location)
    if match is None:
        return None
    city = match.group(2)
    # jccのcsvをロード
    jcc_df = pd.read_csv('jcc-list-utf8.csv', encoding='utf-8', dtype={'JCC': str})
    # 市名で検索 JCCを文字列として取得する
    jcc_row = jcc_df[jcc_df['Name2'] == city]
    if len(jcc_row) == 0:
        return None
    jcc_number = jcc_row['JCC'].values[0]
    return jcc_number

# contactreferencesカラムを更新する関数
def update_contactreferences(db_conn, id, contactreferences, jcc_number):
    new_ref = {"AC": "JCC", "R": jcc_number}
    contactreferences.append(new_ref)
    updated_json = json.dumps(contactreferences, separators=(',', ':'))
    print(f'update {id} {updated_json}')
    cursor = db_conn.cursor()
    query = f"UPDATE Log SET contactreferences = ? WHERE qsoid = ?"
    cursor.execute(query, (updated_json, id))
    db_conn.commit()

# メイン関数
def main(db_path):
    db_conn = connect_db(db_path)
    rows = fetch_rows(db_conn)
    for row in rows:
        # API呼び出しを抑えるため、10秒スリープする
        time.sleep(10)
        id, callsign, contactreferences_json = row
        print(callsign)
        contactreferences = json.loads(contactreferences_json)
        jcc_number = fetch_jcc_number(callsign)
        if jcc_number is None:
            continue
        update_contactreferences(db_conn, id, contactreferences, jcc_number)
    db_conn.close()

if __name__ == "__main__":
    main(DATABASE_PATH)
