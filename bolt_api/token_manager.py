import time
import requests
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Constants for the Bolt API
TOKEN_URL = os.getenv('BOLT_TOKEN_URL')
headers = {
    'Content-Type': 'application/x-www-form-urlencoded'
}

CLIENT_ID = os.getenv('BOLT_CLIENT_ID')
CLIENT_SECRET = os.getenv('BOLT_CLIENT_SECRET')
GRANT_TYPE = 'client_credentials'
SCOPE = 'fleet-integration:api'

class BoltTokenManager:
    def __init__(self):
        self._access_token = None
        self._token_expiry = 0
        self._token_url = TOKEN_URL
        self._headers = {
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        # Get credentials from environment variables
        self._data = {
            'client_id': CLIENT_ID,
            'client_secret': CLIENT_SECRET,
            'grant_type': 'client_credentials',
            'scope': 'fleet-integration:api'
        }

    def _fetch_access_token(self):
        try:
            response = requests.post(
                self._token_url, 
                headers=self._headers, 
                data=self._data
            )
            token_data = response.json()
            self._access_token = token_data["access_token"]
            # Refresh one minute early
            self._token_expiry = time.time() + token_data["expires_in"] - 60
        except requests.RequestException as e:
            raise Exception(f"Failed to fetch access token: {e}")

    def get_access_token(self):
        if self._access_token is None or time.time() > self._token_expiry:
            self._fetch_access_token()
        return self._access_token
