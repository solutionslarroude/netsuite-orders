import time
from requests_oauthlib import OAuth1Session
import json
import pandas as pd
import requests
import re  # Biblioteca para trabalhar com expressões regulares
from google.oauth2 import service_account
import pandas_gbq

session = OAuth1Session(
    client_key='d8949c6f33a49c9cb2904f36f96e6938499be0a4cc5eab7dca3b76381b204fe6',
    client_secret='7306698c254733d5de67ae2f3ba089cfc177cb9efcd041a525c7061ea8b0c19a',
    resource_owner_key='f5569aaa49891793603b473a996c6f546a112c306f581f6a90f6e06964278ad7',
    resource_owner_secret="761967a44e385d9d2b796988884f6b5cae232d0ffe3eb37ff8e370afa59f69c4",
    realm='9468787',
    signature_method='HMAC-SHA256',
)

url = "https://9468787.suitetalk.api.netsuite.com/services/rest/query/v1/suiteql"

body = json.dumps({
    "q": """


    SELECT
        tl.custcol_buma_celigo_item_sku AS sku,
        tl.inventoryreportinglocation,
        t.tranid AS order_number
    FROM
        TransactionLine tl
    JOIN
        Transaction t ON t.id = tl.transaction
    WHERE
        tl.quantitybackordered = 0 
        AND (
            (tl.inventoryreportinglocation = 207 AND t.createddate > TO_DATE('2024-11-11', 'YYYY-MM-DD'))
            OR tl.inventoryreportinglocation = 212
        );




    """
})

headers = {
    "Prefer": "transient",
    "Content-Type": "application/json"
}

all_items = []
next_url = url
retry_count = 0
max_retries = 5  

def send_to_bigquery(df):

    credentials_path = "/home/samuel_alexandre/shopify_orders/credentials/credentials.json" 
    credentials = service_account.Credentials.from_service_account_file(credentials_path)

    project_id = 'larroude-data-prod'
    table_id = 'ODS_Netsuite.ODS_Orders_Commited'
    
    pandas_gbq.to_gbq(df, table_id, project_id=project_id, credentials=credentials, if_exists='replace')

def remove_tamanho_sku(sku):
    # Expressão regular para capturar e remover tamanhos no formato '6/7', '7.0', etc., sem afetar outros números
    return re.sub(r'-\d+(/\d+|\.\d+)?-', '-', sku)

def fetch_and_send_data():
    global next_url, retry_count
    while next_url:
        try:
            response = session.post(url=next_url, headers=headers, data=body)
            response.raise_for_status()

            query_results = response.json()
            items = query_results.get('items', [])
            
            # Remover tamanhos dos SKUs
            for item in items:
                if 'sku' in item:
                    item['sku'] = remove_tamanho_sku(item['sku'])

            all_items.extend(items)
            next_link = next((link['href'] for link in query_results.get('links', []) if link['rel'] == 'next'), None)
            next_url = next_link if query_results.get('hasMore', False) else None
            retry_count = 0

        except requests.exceptions.HTTPError:
            break
        except requests.exceptions.ConnectionError:
            retry_count += 1
            if retry_count <= max_retries:
                time.sleep(5)
            else:
                break
        except requests.exceptions.Timeout:
            retry_count += 1
            if retry_count <= max_retries:
                time.sleep(60)
            else:
                break
        except requests.exceptions.RequestException:
            break

    if all_items:
        df = pd.DataFrame(all_items)
        send_to_bigquery(df)

fetch_and_send_data()
