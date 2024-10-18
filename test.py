import unittest
from unittest.mock import patch, MagicMock
import requests
import pandas as pd
from google.oauth2 import service_account
import pandas_gbq
from requests_oauthlib import OAuth1Session

# Função principal para ser testada (assume que está definida no mesmo arquivo ou importada)
def fetch_suiteql_data(session, url, headers, body, all_items):
    try:
        response = session.post(url, headers=headers, data=body)
        response.raise_for_status()
        if response.status_code == 200:
            query_results = response.json()
            all_items.extend(query_results.get('items', []))
    except requests.exceptions.ConnectionError as conn_err:
        raise conn_err

def send_to_bigquery(df, credentials_path, project_id, table_id):
    credentials = service_account.Credentials.from_service_account_file(credentials_path)
    pandas_gbq.to_gbq(df, table_id, project_id=project_id, credentials=credentials, if_exists='append')

class TestSuiteQLBigQuery(unittest.TestCase):

    @patch('requests_oauthlib.OAuth1Session.post')
    def test_successful_request(self, mock_post):
        # Simula uma resposta bem-sucedida (200)
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "items": [{"order_number": "1001", "sku": "ABC123", "committed_quantity": 1}],
            "hasMore": False
        }
        mock_post.return_value = mock_response

        all_items = []
        session = OAuth1Session(
            client_key='client_key',
            client_secret='client_secret',
            resource_owner_key='resource_owner_key',
            resource_owner_secret='resource_owner_secret',
        )
        url = "https://example.com"
        headers = {}
        body = {}

        # Chama a função que faz a requisição
        fetch_suiteql_data(session, url, headers, body, all_items)

        # Testa se o resultado é o esperado
        self.assertEqual(len(all_items), 1)

    @patch('pandas_gbq.to_gbq')
    @patch('google.oauth2.service_account.Credentials.from_service_account_file')
    def test_send_to_bigquery(self, mock_credentials, mock_to_gbq):
        # Simula o comportamento da função de credenciais e da função to_gbq
        mock_credentials.return_value = MagicMock()
        mock_to_gbq.return_value = None  # Simula uma execução bem-sucedida

        df = pd.DataFrame([{"order_number": "1001", "sku": "ABC123", "committed_quantity": 1}])
        credentials_path = "credentials/credentials.json"
        project_id = "larroude-data-prod"
        table_id = "ODS_Netsuite"

        # Chama a função que envia os dados ao BigQuery
        send_to_bigquery(df, credentials_path, project_id, table_id)

        # Verifica se as funções foram chamadas corretamente
        mock_credentials.assert_called_once_with(credentials_path)
        mock_to_gbq.assert_called_once_with(df, table_id, project_id=project_id, credentials=mock_credentials.return_value, if_exists='append')

    @patch('requests_oauthlib.OAuth1Session.post')
    def test_connection_error(self, mock_post):
        # Simula um erro de conexão
        mock_post.side_effect = requests.exceptions.ConnectionError

        session = OAuth1Session(
            client_key='client_key',
            client_secret='client_secret',
            resource_owner_key='resource_owner_key',
            resource_owner_secret='resource_owner_secret',
        )
        url = "https://example.com"
        headers = {}
        body = {}
        all_items = []

        # Verifica se o erro de conexão é levantado
        with self.assertRaises(requests.exceptions.ConnectionError):
            fetch_suiteql_data(session, url, headers, body, all_items)

if __name__ == '__main__':
    unittest.main()
