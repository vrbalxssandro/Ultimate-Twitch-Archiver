import logging
import json
import requests
import asyncio
from datetime import datetime, timedelta, timezone
import time
import threading

from uta_bot import config_manager # This import is fine and necessary

logger = logging.getLogger(__name__)

class TwitchAPIHelper:
    def __init__(self, client_id, client_secret):
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token = None
        self.token_expiry = datetime.now()

    def _log_api_error(self, e, response_obj, context_msg):
        logger.error(f"{context_msg}: {e}")
        if response_obj and hasattr(response_obj, 'text'):
            logger.error(f"Raw response text: {response_obj.text}")
        elif hasattr(e, 'response') and e.response is not None and hasattr(e.response, 'text'):
            logger.error(f"Response content: {e.response.text}")

    async def _get_app_access_token(self):
        if self.access_token and datetime.now() < self.token_expiry:
            return self.access_token

        logger.info("TwitchAPIHelper: Attempting to fetch/refresh App Access Token...")
        url = "https://id.twitch.tv/oauth2/token"
        params = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "client_credentials"
        }
        response_obj = None
        try:
            response_obj = await asyncio.to_thread(requests.post, url, params=params, timeout=10)
            response_obj.raise_for_status()
            data = response_obj.json()
            self.access_token = data['access_token']
            self.token_expiry = datetime.now() + timedelta(seconds=data.get('expires_in', 3600) - 300)
            logger.info("TwitchAPIHelper: Obtained/refreshed Twitch App Access Token.")
            return self.access_token
        except requests.exceptions.RequestException as e:
            self._log_api_error(e, response_obj, "TwitchAPIHelper: Error getting App Token")
            return None
        except (KeyError, json.JSONDecodeError) as e:
            self._log_api_error(e, response_obj, "TwitchAPIHelper: Error parsing App Token response")
            return None

    async def get_user_id(self, username: str):
        token = await self._get_app_access_token()
        if not token: return None
        if not username:
            logger.warning("TwitchAPIHelper: Attempted to get_user_id with None or empty username.")
            return None

        url = f"https://api.twitch.tv/helix/users?login={username.lower()}"
        headers = {"Client-ID": self.client_id, "Authorization": f"Bearer {token}"}
        response_obj = None
        try:
            response_obj = await asyncio.to_thread(requests.get, url, headers=headers, timeout=10)
            response_obj.raise_for_status()
            data = response_obj.json()
            if data.get('data'):
                return data['data'][0]['id']
            logger.warning(f"TwitchAPIHelper: User '{username}' not found or API response malformed: {data}")
            return None
        except requests.exceptions.RequestException as e:
            self._log_api_error(e, response_obj, f"TwitchAPIHelper: Error getting User ID for '{username}'")
            return None
        except (KeyError, IndexError, json.JSONDecodeError) as e:
            self._log_api_error(e, response_obj, f"TwitchAPIHelper: Error parsing User ID response for '{username}'")
            return None

    async def get_follower_count(self, user_id: str):
        token = await self._get_app_access_token()
        if not token or not user_id: return None

        url = f"https://api.twitch.tv/helix/channels/followers?broadcaster_id={user_id}"
        headers = {"Client-ID": self.client_id, "Authorization": f"Bearer {token}"}
        response_obj = None
        try:
            response_obj = await asyncio.to_thread(requests.get, url, headers=headers, timeout=10)
            response_obj.raise_for_status()
            data = response_obj.json()
            return data.get('total')
        except requests.exceptions.RequestException as e:
            self._log_api_error(e, response_obj, f"TwitchAPIHelper: Error getting followers for User ID '{user_id}'")
            return None
        except (KeyError, json.JSONDecodeError) as e:
            self._log_api_error(e, response_obj, f"TwitchAPIHelper: Error parsing followers response for User ID '{user_id}'")
            return None

_uta_token_refresh_lock = threading.Lock()

UTA_TWITCH_API_BASE_URL = "https://api.twitch.tv/helix"
UTA_TWITCH_AUTH_URL = "https://id.twitch.tv/oauth2/token"

def get_uta_twitch_access_token():
    with _uta_token_refresh_lock:
        current_time = time.time()
        if config_manager.uta_shared_access_token and current_time < (config_manager.uta_token_expiry_time - 60):
            return config_manager.uta_shared_access_token

        logger.info("UTA TwitchAPI: Attempting to fetch/refresh Twitch API access token for UTA...")
        params = {
            "client_id": config_manager.TWITCH_CLIENT_ID,
            "client_secret": config_manager.TWITCH_CLIENT_SECRET,
            "grant_type": "client_credentials"
        }
        response_obj = None
        try:
            response_obj = requests.post(UTA_TWITCH_AUTH_URL, params=params, timeout=10)
            response_obj.raise_for_status()
            data = response_obj.json()
            config_manager.uta_shared_access_token = data["access_token"]
            config_manager.uta_token_expiry_time = current_time + data.get("expires_in", 3600)
            logger.info("UTA TwitchAPI: Successfully obtained/refreshed Twitch access token for UTA.")
            return config_manager.uta_shared_access_token
        except requests.exceptions.RequestException as e:
            logger.error(f"UTA TwitchAPI: Error getting Twitch access token: {e}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"UTA TwitchAPI: Response content: {e.response.text}")
            config_manager.uta_shared_access_token = None
            config_manager.uta_token_expiry_time = 0
            return None
        except (KeyError, json.JSONDecodeError) as e:
            logger.error(f"UTA TwitchAPI: Error parsing access token response: {e}")
            if response_obj is not None and hasattr(response_obj, 'text'):
                logger.error(f"UTA TwitchAPI: Raw response text: {response_obj.text}")
            config_manager.uta_shared_access_token = None
            config_manager.uta_token_expiry_time = 0
            return None

def make_uta_twitch_api_request(endpoint: str, params: dict = None, method: str = 'GET', max_retries: int = 1):
    url = f"{UTA_TWITCH_API_BASE_URL}/{endpoint.lstrip('/')}"

    for attempt in range(max_retries + 1):
        access_token = get_uta_twitch_access_token()
        if not access_token:
            logger.error(f"UTA TwitchAPI: No access token available for API request to {url}.")
            return None

        headers = {
            "Client-ID": config_manager.TWITCH_CLIENT_ID,
            "Authorization": f"Bearer {access_token}"
        }

        response_obj = None
        try:
            if method.upper() == 'GET':
                response_obj = requests.get(url, headers=headers, params=params, timeout=10)
            elif method.upper() == 'POST':
                response_obj = requests.post(url, headers=headers, json=params, timeout=10)
            else:
                logger.error(f"UTA TwitchAPI: Unsupported HTTP method: {method}")
                return None

            if response_obj.status_code == 401 and attempt < max_retries:
                logger.warning(f"UTA TwitchAPI: API request to {url} resulted in 401 (Unauthorized). Attempt {attempt + 1}/{max_retries + 1}. Forcing token refresh.")
                with _uta_token_refresh_lock:
                    config_manager.uta_shared_access_token = None
                    config_manager.uta_token_expiry_time = 0
                continue

            response_obj.raise_for_status()
            return response_obj.json()

        except requests.exceptions.HTTPError as http_err:
            logger.error(f"UTA TwitchAPI: HTTP error on API request to {url} (Attempt {attempt + 1}): {http_err}")
            if hasattr(http_err, 'response') and http_err.response is not None:
                logger.error(f"UTA TwitchAPI: Response content: {http_err.response.text}")
            if attempt >= max_retries:
                return None
            time.sleep(1 * (2**attempt))
        except requests.exceptions.RequestException as req_e:
            logger.error(f"UTA TwitchAPI: Request error on API request to {url} (Attempt {attempt + 1}): {req_e}")
            if response_obj is not None and hasattr(response_obj, 'text'):
                logger.error(f"UTA TwitchAPI: Response content: {response_obj.text}")
            if attempt >= max_retries:
                return None
            time.sleep(1 * (2**attempt))
        except (KeyError, IndexError, json.JSONDecodeError) as parse_e:
            logger.error(f"UTA TwitchAPI: Error parsing API response from {url}: {parse_e}")
            if response_obj is not None and hasattr(response_obj, 'text'):
                logger.error(f"UTA TwitchAPI: Raw response content that failed parsing: {response_obj.text}")
            return None

    logger.error(f"UTA TwitchAPI: Failed API request to {url} after {max_retries + 1} attempts.")
    return None

def get_uta_broadcaster_id(channel_name: str):
    if not channel_name:
        logger.warning("UTA: Attempted to get broadcaster ID with no channel name specified.")
        return None
    if config_manager.uta_broadcaster_id_cache:
        return config_manager.uta_broadcaster_id_cache

    data = make_uta_twitch_api_request("/users", params={"login": channel_name})
    if data and data.get("data"):
        config_manager.uta_broadcaster_id_cache = data["data"][0]["id"]
        logger.info(f"UTA: Found and cached broadcaster ID for {channel_name}: {config_manager.uta_broadcaster_id_cache}")
        return config_manager.uta_broadcaster_id_cache

    logger.error(f"UTA: Could not find broadcaster ID for: {channel_name}")
    return None