import discord
from discord.ext import commands, tasks
import requests
import json
import asyncio
import logging
from datetime import datetime, timedelta, timezone, date
import struct
import os
import re # For parsing duration strings
import time # UTA
import subprocess # UTA
import shutil # UTA
import signal # UTA
import threading # UTA
import functools # For twitchinfo command helper
import csv # For !exportdata
import io # For sending CSV data as file
import random # For YouTube Live Stream resource title generation
import sys

# --- Google API Client (Optional for YouTube API integration) ---
try:
    from google.oauth2.credentials import Credentials as GoogleCredentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from google.auth.transport.requests import Request as GoogleAuthRequest
    from googleapiclient.discovery import build as google_build
    from googleapiclient.errors import HttpError as GoogleHttpError
    GOOGLE_API_AVAILABLE = True
    logger_google = logging.getLogger('googleapiclient.discovery_cache')
    logger_google.setLevel(logging.ERROR) # Suppress noisy cache discovery logs
except ImportError:
    GOOGLE_API_AVAILABLE = False
    GoogleCredentials, InstalledAppFlow, GoogleAuthRequest, google_build, GoogleHttpError = None, None, None, None, None

# --- Streamlink (Optional for YouTube Playability Check) ---
try:
    import streamlink
    STREAMLINK_LIB_AVAILABLE = True
except ImportError:
    STREAMLINK_LIB_AVAILABLE = False
    streamlink = None


# --- Matplotlib (Optional for plotting) ---
try:
    import matplotlib
    matplotlib.use('Agg') # Non-interactive backend
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    MATPLOTLIB_AVAILABLE = True
    logger_mpl = logging.getLogger('matplotlib')
    logger_mpl.setLevel(logging.WARNING) # Suppress Matplotlib debug logs
except ImportError:
    MATPLOTLIB_AVAILABLE = False
    plt = None
    mdates = None

# --- Bot Start Time ---
bot_start_time = datetime.now(timezone.utc)

# --- Configuration Loading ---
CONFIG_FILE = 'config.json'
config_data = {}

fctd_twitch_api = None

def load_config(initial_load=False):
    global config_data
    try:
        with open(CONFIG_FILE, 'r') as f:
            loaded_json = json.load(f)
        required_keys = ['DISCORD_TOKEN', 'TWITCH_CLIENT_ID', 'TWITCH_CLIENT_SECRET']
        for key in required_keys:
            if not loaded_json.get(key) or "YOUR_" in str(loaded_json.get(key)):
                err_msg = f"ERROR: Essential config key '{key}' is missing or placeholder in {CONFIG_FILE}."
                if initial_load: print(err_msg); exit(1)
                logger.error(f"Reload: {err_msg}"); return False, err_msg
        if initial_load: config_data = loaded_json
        return True, loaded_json
    except FileNotFoundError:
        err_msg = f"ERROR: {CONFIG_FILE} not found. Create based on template."
        if initial_load: print(err_msg); exit(1)
        logger.error(f"Reload: {err_msg}"); return False, err_msg
    except json.JSONDecodeError:
        err_msg = f"ERROR: Error decoding {CONFIG_FILE}. Check syntax."
        if initial_load: print(err_msg); exit(1)
        logger.error(f"Reload: {err_msg}"); return False, err_msg

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(levelname)s:%(name)s:[%(threadName)s]: %(message)s')
logger = logging.getLogger('discord_twitch_bot')

# --- Global Config Variables ---
DISCORD_TOKEN = None
FCTD_TWITCH_USERNAME = None
TWITCH_CLIENT_ID = None
TWITCH_CLIENT_SECRET = None

BINARY_RECORD_FORMAT = '>II'
BINARY_RECORD_SIZE = struct.calcsize(BINARY_RECORD_FORMAT)
STREAM_DURATION_RECORD_FORMAT = '>II'
STREAM_DURATION_RECORD_SIZE = struct.calcsize(STREAM_DURATION_RECORD_FORMAT)

EVENT_TYPE_STREAM_START = 1
EVENT_TYPE_STREAM_END = 2
EVENT_TYPE_GAME_CHANGE = 3
EVENT_TYPE_TITLE_CHANGE = 4
EVENT_TYPE_TAGS_CHANGE = 5

SA_BASE_HEADER_FORMAT = '>BI'
SA_BASE_HEADER_SIZE = struct.calcsize(SA_BASE_HEADER_FORMAT)
SA_STRING_LEN_FORMAT = '>H'
SA_STRING_LEN_SIZE = struct.calcsize(SA_STRING_LEN_FORMAT)
SA_LIST_HEADER_FORMAT = '>H'
SA_LIST_HEADER_SIZE = struct.calcsize(SA_LIST_HEADER_FORMAT)
SA_INT_FORMAT = '>I'
SA_INT_SIZE = struct.calcsize(SA_INT_FORMAT)

BOT_SESSION_LOG_FILE_KEY = "BOT_SESSION_LOG_FILE"
BOT_EVENT_START = 1
BOT_EVENT_STOP = 2
BOT_SESSION_RECORD_FORMAT = '>BI'
BOT_SESSION_RECORD_SIZE = struct.calcsize(BOT_SESSION_RECORD_FORMAT)
BOT_SESSION_LOG_FILE_PATH = None


UTA_STREAM_STATUS_NOTIFICATIONS_ENABLED = False
UTA_STREAM_STATUS_WEBHOOK_URL = None
UTA_STREAM_STATUS_CHANNEL_ID = None
UTA_STREAM_STATUS_POLL_INTERVAL_SECONDS = 60
UTA_STREAM_ACTIVITY_LOG_FILE = "stream_activity.bin"
UTA_VIEWER_COUNT_LOGGING_ENABLED = False
UTA_VIEWER_COUNT_LOG_INTERVAL_SECONDS = 300
UTA_VIEWER_COUNT_LOG_FILE = "viewer_counts.bin"

UTA_ENABLED = False
UTA_TWITCH_CHANNEL_NAME = None
owner_id_from_config = None
UTA_TWITCH_API_BASE_URL = "https://api.twitch.tv/helix"
UTA_TWITCH_AUTH_URL = "https://id.twitch.tv/oauth2/token"
FCTD_TARGET_CHANNEL_ID = None
FCTD_COMMAND_CHANNEL_ID = None
FCTD_COMMAND_PREFIX = None
FCTD_UPDATE_INTERVAL_MINUTES = None
FCTD_CHANNEL_NAME_PREFIX = None
FCTD_CHANNEL_NAME_SUFFIX = None
FCTD_FOLLOWER_DATA_FILE = None
UTA_STREAM_DURATION_LOG_FILE = None
UTA_CLIP_MONITOR_ENABLED = False
UTA_DISCORD_WEBHOOK_URL_CLIPS = None
UTA_CHECK_INTERVAL_SECONDS_CLIPS = None
UTA_CLIP_LOOKBACK_MINUTES = None
UTA_RESTREAMER_ENABLED = False
UTA_DISCORD_WEBHOOK_URL_RESTREAMER = None
UTA_YOUTUBE_RTMP_URL_BASE = None
UTA_YOUTUBE_STREAM_KEY = None
UTA_CHECK_INTERVAL_SECONDS_RESTREAMER = None
UTA_RESTREAM_CHECK_INTERVAL_WHEN_LIVE = None
UTA_POST_RESTREAM_COOLDOWN_SECONDS = None
UTA_STREAMLINK_PATH = None
UTA_FFMPEG_PATH = None

UTA_YOUTUBE_API_ENABLED = False
UTA_YOUTUBE_CLIENT_SECRET_FILE = "client_secret.json"
UTA_YOUTUBE_TOKEN_FILE = "youtube_token.json"
UTA_YOUTUBE_PLAYLIST_ID = None
UTA_YOUTUBE_DEFAULT_PRIVACY = "unlisted"
UTA_YOUTUBE_MAKE_PUBLIC_AFTER_STREAM = False
UTA_YOUTUBE_SCHEDULED_ROLLOVER_HOURS = 0.0
UTA_YOUTUBE_DYNAMIC_TITLE_TEMPLATE = "{twitch_username} - {twitch_title} ({game_name}) - Part {part_num} [{date}]"
UTA_YOUTUBE_DYNAMIC_DESCRIPTION_TEMPLATE = "Originally streamed by {twitch_username} on Twitch: https://twitch.tv/{twitch_username}\nGame: {game_name}\nTitle: {twitch_title}\n\nArchived by UTA."
UTA_YOUTUBE_API_SCOPES = ["https://www.googleapis.com/auth/youtube.force-ssl"]

# New reliability config globals
UTA_RESTREAM_MAX_CONSECUTIVE_FAILURES = 3
UTA_RESTREAM_LONG_COOLDOWN_SECONDS = 300
UTA_YOUTUBE_PLAYABILITY_CHECK_ENABLED = True
UTA_YOUTUBE_PLAYABILITY_CHECK_RETRIES = 2
UTA_YOUTUBE_PLAYABILITY_CHECK_DELAY_SECONDS = 15
UTA_FFMPEG_STARTUP_WAIT_SECONDS = 10
UTA_CURRENT_YT_VIDEO_ID_FOR_LOGGING = None # For GUI to potentially pick up
twitch_session_active_global = False # Use a distinct global name
youtube_api_session_active_global = False # Use a distinct global name
UTA_MANUAL_FFMPEG_RESTART_REQUESTED = False
UTA_MANUAL_NEW_PART_REQUESTED = False
UTA_FFMPEG_PID = None
UTA_STREAMLINK_PID = None
UTA_PIPE_START_TIME_UTC = None # When current ffmpeg/streamlink pipe started
UTA_LAST_PLAYABILITY_CHECK_STATUS = "N/A" # "Pending", "Passed", "Failed", "Skipped"


# --- fctd: Twitch API Helper Class ---
class TwitchAPI:
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
            self.token_expiry = datetime.now() + timedelta(seconds=data['expires_in'] - 300)
            logger.info("fctd.TwitchAPI: Obtained/refreshed Twitch App Access Token.")
            return self.access_token
        except requests.exceptions.RequestException as e:
            self._log_api_error(e, response_obj, "fctd.TwitchAPI: Error getting App Token")
            return None
        except (KeyError, json.JSONDecodeError) as e:
            self._log_api_error(e, response_obj, "fctd.TwitchAPI: Error parsing App Token response")
            return None

    async def get_user_id(self, username):
        token = await self._get_app_access_token()
        if not token: return None
        if not username:
            logger.warning("fctd.TwitchAPI: Attempted to get_user_id with None username.")
            return None
        url = f"https://api.twitch.tv/helix/users?login={username}"
        headers = {"Client-ID": self.client_id, "Authorization": f"Bearer {token}"}
        response_obj = None
        try:
            response_obj = await asyncio.to_thread(requests.get, url, headers=headers, timeout=10)
            response_obj.raise_for_status()
            data = response_obj.json()
            if data.get('data'):
                return data['data'][0]['id']
            logger.warning(f"fctd.TwitchAPI: User '{username}' not found/API malformed: {data}")
            return None
        except requests.exceptions.RequestException as e:
            self._log_api_error(e, response_obj, f"fctd.TwitchAPI: Error getting User ID for '{username}'")
            return None
        except (KeyError, IndexError, json.JSONDecodeError) as e:
            self._log_api_error(e, response_obj, f"fctd.TwitchAPI: Error parsing User ID for '{username}'")
            return None

    async def get_follower_count(self, user_id):
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
            self._log_api_error(e, response_obj, f"fctd.TwitchAPI: Error getting followers for User ID '{user_id}'")
            return None
        except (KeyError, json.JSONDecodeError) as e:
            self._log_api_error(e, response_obj, f"fctd.TwitchAPI: Error parsing followers for User ID '{user_id}'")
            return None

def apply_config_globally(source_config_data):
    logger.info("Applying configuration to global variables...")
    global DISCORD_TOKEN, FCTD_TWITCH_USERNAME, FCTD_TARGET_CHANNEL_ID, \
           FCTD_COMMAND_CHANNEL_ID, FCTD_COMMAND_PREFIX, FCTD_UPDATE_INTERVAL_MINUTES, \
           FCTD_CHANNEL_NAME_PREFIX, FCTD_CHANNEL_NAME_SUFFIX, FCTD_FOLLOWER_DATA_FILE, \
           TWITCH_CLIENT_ID, TWITCH_CLIENT_SECRET, UTA_STREAM_DURATION_LOG_FILE, \
           UTA_ENABLED, UTA_TWITCH_CHANNEL_NAME, UTA_CLIP_MONITOR_ENABLED, \
           UTA_DISCORD_WEBHOOK_URL_CLIPS, UTA_CHECK_INTERVAL_SECONDS_CLIPS, \
           UTA_CLIP_LOOKBACK_MINUTES, UTA_RESTREAMER_ENABLED, \
           UTA_DISCORD_WEBHOOK_URL_RESTREAMER, UTA_YOUTUBE_RTMP_URL_BASE, \
           UTA_YOUTUBE_STREAM_KEY, UTA_CHECK_INTERVAL_SECONDS_RESTREAMER, \
           UTA_RESTREAM_CHECK_INTERVAL_WHEN_LIVE, UTA_POST_RESTREAM_COOLDOWN_SECONDS, \
           UTA_STREAMLINK_PATH, UTA_FFMPEG_PATH, owner_id_from_config, \
           fctd_twitch_api, uta_broadcaster_id_cache, \
           UTA_STREAM_STATUS_NOTIFICATIONS_ENABLED, UTA_STREAM_STATUS_WEBHOOK_URL, \
           UTA_STREAM_STATUS_CHANNEL_ID, UTA_STREAM_STATUS_POLL_INTERVAL_SECONDS, \
           UTA_STREAM_ACTIVITY_LOG_FILE, UTA_VIEWER_COUNT_LOGGING_ENABLED, \
           UTA_VIEWER_COUNT_LOG_INTERVAL_SECONDS, UTA_VIEWER_COUNT_LOG_FILE, \
           BOT_SESSION_LOG_FILE_PATH, \
           UTA_YOUTUBE_API_ENABLED, UTA_YOUTUBE_CLIENT_SECRET_FILE, UTA_YOUTUBE_TOKEN_FILE, \
           UTA_YOUTUBE_PLAYLIST_ID, UTA_YOUTUBE_DEFAULT_PRIVACY, UTA_YOUTUBE_MAKE_PUBLIC_AFTER_STREAM, \
           UTA_YOUTUBE_SCHEDULED_ROLLOVER_HOURS, UTA_YOUTUBE_DYNAMIC_TITLE_TEMPLATE, \
           UTA_YOUTUBE_DYNAMIC_DESCRIPTION_TEMPLATE, \
           UTA_RESTREAM_MAX_CONSECUTIVE_FAILURES, UTA_RESTREAM_LONG_COOLDOWN_SECONDS, \
           UTA_YOUTUBE_PLAYABILITY_CHECK_ENABLED, UTA_YOUTUBE_PLAYABILITY_CHECK_RETRIES, \
           UTA_YOUTUBE_PLAYABILITY_CHECK_DELAY_SECONDS, UTA_FFMPEG_STARTUP_WAIT_SECONDS


    DISCORD_TOKEN = source_config_data.get('DISCORD_TOKEN')
    FCTD_TWITCH_USERNAME = source_config_data.get('FCTD_TWITCH_USERNAME')
    FCTD_TARGET_CHANNEL_ID = int(source_config_data.get('FCTD_TARGET_CHANNEL_ID')) if source_config_data.get('FCTD_TARGET_CHANNEL_ID') else None
    FCTD_COMMAND_CHANNEL_ID = int(source_config_data.get('FCTD_COMMAND_CHANNEL_ID')) if source_config_data.get('FCTD_COMMAND_CHANNEL_ID') else None
    FCTD_COMMAND_PREFIX = source_config_data.get('FCTD_COMMAND_PREFIX', '!')
    FCTD_UPDATE_INTERVAL_MINUTES = source_config_data.get('FCTD_UPDATE_INTERVAL_MINUTES', 2)
    FCTD_CHANNEL_NAME_PREFIX = source_config_data.get('FCTD_CHANNEL_NAME_PREFIX', "Followers: ")
    FCTD_CHANNEL_NAME_SUFFIX = source_config_data.get('FCTD_CHANNEL_NAME_SUFFIX', "")
    FCTD_FOLLOWER_DATA_FILE = source_config_data.get('FCTD_FOLLOWER_DATA_FILE', "follower_counts.bin")
    TWITCH_CLIENT_ID = source_config_data.get('TWITCH_CLIENT_ID'); TWITCH_CLIENT_SECRET = source_config_data.get('TWITCH_CLIENT_SECRET')
    UTA_STREAM_DURATION_LOG_FILE = source_config_data.get('UTA_STREAM_DURATION_LOG_FILE', "stream_durations.bin")
    UTA_ENABLED = source_config_data.get('UTA_ENABLED', False)
    old_uta_twitch_channel_name = UTA_TWITCH_CHANNEL_NAME
    UTA_TWITCH_CHANNEL_NAME = source_config_data.get('UTA_TWITCH_CHANNEL_NAME')
    if old_uta_twitch_channel_name != UTA_TWITCH_CHANNEL_NAME:
        logger.info(f"UTA Twitch channel name changed. Clearing broadcaster ID cache.")
        uta_broadcaster_id_cache = None
    UTA_CLIP_MONITOR_ENABLED = source_config_data.get('UTA_CLIP_MONITOR_ENABLED', False)
    UTA_DISCORD_WEBHOOK_URL_CLIPS = source_config_data.get('UTA_DISCORD_WEBHOOK_URL_CLIPS')
    UTA_CHECK_INTERVAL_SECONDS_CLIPS = source_config_data.get('UTA_CHECK_INTERVAL_SECONDS_CLIPS', 300)
    UTA_CLIP_LOOKBACK_MINUTES = source_config_data.get('UTA_CLIP_LOOKBACK_MINUTES', 5)
    UTA_RESTREAMER_ENABLED = source_config_data.get('UTA_RESTREAMER_ENABLED', False)
    UTA_DISCORD_WEBHOOK_URL_RESTREAMER = source_config_data.get('UTA_DISCORD_WEBHOOK_URL_RESTREAMER')
    UTA_YOUTUBE_RTMP_URL_BASE = source_config_data.get('UTA_YOUTUBE_RTMP_URL_BASE')
    UTA_YOUTUBE_STREAM_KEY = source_config_data.get('UTA_YOUTUBE_STREAM_KEY')
    UTA_CHECK_INTERVAL_SECONDS_RESTREAMER = source_config_data.get('UTA_CHECK_INTERVAL_SECONDS_RESTREAMER', 60)
    UTA_RESTREAM_CHECK_INTERVAL_WHEN_LIVE = source_config_data.get('UTA_RESTREAM_CHECK_INTERVAL_WHEN_LIVE', 300)
    UTA_POST_RESTREAM_COOLDOWN_SECONDS = source_config_data.get('UTA_POST_RESTREAM_COOLDOWN_SECONDS', 60)
    UTA_STREAMLINK_PATH = source_config_data.get('UTA_STREAMLINK_PATH', "streamlink")
    UTA_FFMPEG_PATH = source_config_data.get('UTA_FFMPEG_PATH', "ffmpeg")
    UTA_STREAM_STATUS_NOTIFICATIONS_ENABLED = source_config_data.get('UTA_STREAM_STATUS_NOTIFICATIONS_ENABLED', False)
    UTA_STREAM_STATUS_WEBHOOK_URL = source_config_data.get('UTA_STREAM_STATUS_WEBHOOK_URL')
    UTA_STREAM_STATUS_CHANNEL_ID = int(source_config_data.get('UTA_STREAM_STATUS_CHANNEL_ID')) if source_config_data.get('UTA_STREAM_STATUS_CHANNEL_ID') else None
    UTA_STREAM_STATUS_POLL_INTERVAL_SECONDS = source_config_data.get('UTA_STREAM_STATUS_POLL_INTERVAL_SECONDS', 60)
    UTA_STREAM_ACTIVITY_LOG_FILE = source_config_data.get('UTA_STREAM_ACTIVITY_LOG_FILE', "stream_activity.bin")
    UTA_VIEWER_COUNT_LOGGING_ENABLED = source_config_data.get('UTA_VIEWER_COUNT_LOGGING_ENABLED', False)
    UTA_VIEWER_COUNT_LOG_INTERVAL_SECONDS = source_config_data.get('UTA_VIEWER_COUNT_LOG_INTERVAL_SECONDS', 300)
    UTA_VIEWER_COUNT_LOG_FILE = source_config_data.get('UTA_VIEWER_COUNT_LOG_FILE', "viewer_counts.bin")
    owner_id_from_config = source_config_data.get('DISCORD_BOT_OWNER_ID')
    BOT_SESSION_LOG_FILE_PATH = source_config_data.get(BOT_SESSION_LOG_FILE_KEY, "bot_sessions.bin")
    UTA_YOUTUBE_API_ENABLED = source_config_data.get('UTA_YOUTUBE_API_ENABLED', False)
    UTA_YOUTUBE_CLIENT_SECRET_FILE = source_config_data.get('UTA_YOUTUBE_CLIENT_SECRET_FILE', "client_secret.json")
    UTA_YOUTUBE_TOKEN_FILE = source_config_data.get('UTA_YOUTUBE_TOKEN_FILE', "youtube_token.json")
    UTA_YOUTUBE_PLAYLIST_ID = source_config_data.get('UTA_YOUTUBE_PLAYLIST_ID')
    UTA_YOUTUBE_DEFAULT_PRIVACY = source_config_data.get('UTA_YOUTUBE_DEFAULT_PRIVACY', "unlisted").lower()
    UTA_YOUTUBE_MAKE_PUBLIC_AFTER_STREAM = source_config_data.get('UTA_YOUTUBE_MAKE_PUBLIC_AFTER_STREAM', False)
    UTA_YOUTUBE_SCHEDULED_ROLLOVER_HOURS = source_config_data.get('UTA_YOUTUBE_SCHEDULED_ROLLOVER_HOURS', 0.0)
    UTA_YOUTUBE_DYNAMIC_TITLE_TEMPLATE = source_config_data.get('UTA_YOUTUBE_DYNAMIC_TITLE_TEMPLATE', "{twitch_username} - {twitch_title} ({game_name}) - Part {part_num} [{date}]")
    UTA_YOUTUBE_DYNAMIC_DESCRIPTION_TEMPLATE = source_config_data.get('UTA_YOUTUBE_DYNAMIC_DESCRIPTION_TEMPLATE', "Originally streamed by {twitch_username} on Twitch: https://twitch.tv/{twitch_username}\nGame: {game_name}\nTitle: {twitch_title}\n\nArchived by UTA.")

    UTA_RESTREAM_MAX_CONSECUTIVE_FAILURES = source_config_data.get('UTA_RESTREAM_MAX_CONSECUTIVE_FAILURES', 3)
    UTA_RESTREAM_LONG_COOLDOWN_SECONDS = source_config_data.get('UTA_RESTREAM_LONG_COOLDOWN_SECONDS', 300)
    UTA_YOUTUBE_PLAYABILITY_CHECK_ENABLED = source_config_data.get('UTA_YOUTUBE_PLAYABILITY_CHECK_ENABLED', True)
    UTA_YOUTUBE_PLAYABILITY_CHECK_RETRIES = source_config_data.get('UTA_YOUTUBE_PLAYABILITY_CHECK_RETRIES', 2)
    UTA_YOUTUBE_PLAYABILITY_CHECK_DELAY_SECONDS = source_config_data.get('UTA_YOUTUBE_PLAYABILITY_CHECK_DELAY_SECONDS', 15)
    UTA_FFMPEG_STARTUP_WAIT_SECONDS = source_config_data.get('UTA_FFMPEG_STARTUP_WAIT_SECONDS', 10)

    if fctd_twitch_api is None or fctd_twitch_api.client_id != TWITCH_CLIENT_ID or fctd_twitch_api.client_secret != TWITCH_CLIENT_SECRET:
        logger.info("Re-initializing fctd.TwitchAPI due to credential change or initial setup.")
        fctd_twitch_api = TwitchAPI(TWITCH_CLIENT_ID, TWITCH_CLIENT_SECRET)
    logger.info("Configuration applied globally.")

initial_success, initial_config_data_dict = load_config(initial_load=True)
if not initial_success: exit(1)
apply_config_globally(initial_config_data_dict)
fctd_current_twitch_user_id = None

# --- Data Logging Helpers ---
def _write_binary_data_sync(filepath, data_bytes):
    try:
        with open(filepath, 'ab') as f:
            f.write(data_bytes)
    except Exception as e:
        logger.error(f"Error writing binary data to {filepath}: {e}")

async def log_follower_data_binary(timestamp_dt, count):
    if FCTD_FOLLOWER_DATA_FILE:
        try:
            packed_data = struct.pack(BINARY_RECORD_FORMAT, int(timestamp_dt.timestamp()), int(count))
            await asyncio.to_thread(_write_binary_data_sync, FCTD_FOLLOWER_DATA_FILE, packed_data)
        except Exception as e:
            logger.error(f"Failed to log follower data to {FCTD_FOLLOWER_DATA_FILE}: {e}")

async def log_viewer_data_binary(timestamp_dt, count):
    if UTA_VIEWER_COUNT_LOGGING_ENABLED and UTA_VIEWER_COUNT_LOG_FILE:
        try:
            packed_data = struct.pack(BINARY_RECORD_FORMAT, int(timestamp_dt.timestamp()), int(count))
            await asyncio.to_thread(_write_binary_data_sync, UTA_VIEWER_COUNT_LOG_FILE, packed_data)
            logger.debug(f"UTA: Logged viewer count {count} to {UTA_VIEWER_COUNT_LOG_FILE}")
        except Exception as e:
            logger.error(f"UTA: Failed to log viewer count to {UTA_VIEWER_COUNT_LOG_FILE}: {e}")

async def log_stream_duration_binary(start_ts_unix: int, end_ts_unix: int):
    if UTA_STREAM_DURATION_LOG_FILE and UTA_ENABLED and UTA_RESTREAMER_ENABLED:
        if end_ts_unix <= start_ts_unix:
            logger.warning(f"UTA: Invalid stream duration log: start={start_ts_unix}, end={end_ts_unix}. Skipping.")
            return
        try:
            packed_data = struct.pack(STREAM_DURATION_RECORD_FORMAT, start_ts_unix, end_ts_unix)
            await asyncio.to_thread(_write_binary_data_sync, UTA_STREAM_DURATION_LOG_FILE, packed_data)
            logger.info(f"UTA: Logged restream duration: {datetime.fromtimestamp(start_ts_unix, tz=timezone.utc).isoformat()} to {datetime.fromtimestamp(end_ts_unix, tz=timezone.utc).isoformat()}")
        except Exception as e:
            logger.error(f"UTA: Failed to log stream duration to {UTA_STREAM_DURATION_LOG_FILE}: {e}")

def _pack_string_for_binary_log(s: str) -> bytes:
    s_bytes = s.encode('utf-8')
    len_bytes = struct.pack(SA_STRING_LEN_FORMAT, len(s_bytes))
    return len_bytes + s_bytes

def _pack_tag_list_for_binary_log(tags: list[str]) -> bytes:
    tags_to_pack = tags if tags is not None else []
    num_tags = len(tags_to_pack)
    header_bytes = struct.pack(SA_LIST_HEADER_FORMAT, num_tags)
    tag_bytes_list = [header_bytes]
    for tag in tags_to_pack:
        tag_bytes_list.append(_pack_string_for_binary_log(tag))
    return b"".join(tag_bytes_list)

async def log_stream_activity_binary(event_type: int, timestamp_dt: datetime, **kwargs):
    if not (UTA_STREAM_ACTIVITY_LOG_FILE and UTA_STREAM_STATUS_NOTIFICATIONS_ENABLED):
        return
    try:
        ts_unix = int(timestamp_dt.timestamp())
        log_entry_bytes = struct.pack(SA_BASE_HEADER_FORMAT, event_type, ts_unix)
        
        if event_type == EVENT_TYPE_STREAM_START:
            title = kwargs.get("title", "")
            game = kwargs.get("game", "")
            tags = kwargs.get("tags", []) 
            youtube_video_id = kwargs.get("youtube_video_id") # New optional kwarg

            log_entry_bytes += _pack_string_for_binary_log(title)
            log_entry_bytes += _pack_string_for_binary_log(game)
            log_entry_bytes += _pack_tag_list_for_binary_log(tags)
            if youtube_video_id: # If provided, pack it
                log_entry_bytes += _pack_string_for_binary_log(youtube_video_id)
            else: # If not provided, pack an empty string indicator (length 0)
                log_entry_bytes += struct.pack(SA_STRING_LEN_FORMAT, 0)

        elif event_type == EVENT_TYPE_STREAM_END:
            duration = kwargs.get("duration_seconds", 0); peak_viewers = kwargs.get("peak_viewers", 0)
            log_entry_bytes += struct.pack(SA_INT_FORMAT, duration) + struct.pack(SA_INT_FORMAT, peak_viewers)
        elif event_type == EVENT_TYPE_GAME_CHANGE:
            old_game = kwargs.get("old_game", ""); new_game = kwargs.get("new_game", "")
            log_entry_bytes += _pack_string_for_binary_log(old_game) + _pack_string_for_binary_log(new_game)
        elif event_type == EVENT_TYPE_TITLE_CHANGE:
            old_title = kwargs.get("old_title", ""); new_title = kwargs.get("new_title", "")
            log_entry_bytes += _pack_string_for_binary_log(old_title) + _pack_string_for_binary_log(new_title)
        elif event_type == EVENT_TYPE_TAGS_CHANGE:
            old_tags = kwargs.get("old_tags", []); new_tags = kwargs.get("new_tags", [])
            log_entry_bytes += _pack_tag_list_for_binary_log(old_tags) + _pack_tag_list_for_binary_log(new_tags)
        else: logger.warning(f"UTA: Unknown stream activity event type for binary log: {event_type}"); return
        await asyncio.to_thread(_write_binary_data_sync, UTA_STREAM_ACTIVITY_LOG_FILE, log_entry_bytes)
        logger.info(f"UTA: Logged stream activity (binary): event {event_type}")
    except Exception as e: logger.error(f"UTA: Failed to log stream activity (binary) to {UTA_STREAM_ACTIVITY_LOG_FILE}: {e}", exc_info=True)

async def log_bot_session_event(event_type: int, timestamp_dt: datetime):
    if not BOT_SESSION_LOG_FILE_PATH: logger.warning("Bot session log file path not configured. Skipping log."); return
    try:
        ts_unix = int(timestamp_dt.timestamp())
        packed_data = struct.pack(BOT_SESSION_RECORD_FORMAT, event_type, ts_unix)
        await asyncio.to_thread(_write_binary_data_sync, BOT_SESSION_LOG_FILE_PATH, packed_data)
        event_name = "START" if event_type == BOT_EVENT_START else "STOP"
        logger.info(f"Logged bot session event: {event_name} at {timestamp_dt.isoformat()}")
    except Exception as e: logger.error(f"Failed to log bot session event to {BOT_SESSION_LOG_FILE_PATH}: {e}")

# --- Stream Activity Parsing Helper ---
def _read_string_from_file_handle_sync(file_handle):
    len_bytes = file_handle.read(SA_STRING_LEN_SIZE)
    if len(len_bytes) < SA_STRING_LEN_SIZE: return None, True
    s_len = struct.unpack(SA_STRING_LEN_FORMAT, len_bytes)[0]
    s_bytes = file_handle.read(s_len)
    if len(s_bytes) < s_len: return None, True
    return s_bytes.decode('utf-8', errors='replace'), False

def _read_tag_list_from_file_handle_sync(file_handle):
    num_tags_bytes = file_handle.read(SA_LIST_HEADER_SIZE)
    if len(num_tags_bytes) < SA_LIST_HEADER_SIZE: return [], True
    num_tags = struct.unpack(SA_LIST_HEADER_FORMAT, num_tags_bytes)[0]
    tags_read = []
    for _ in range(num_tags):
        tag_str, incomplete = _read_string_from_file_handle_sync(file_handle)
        if incomplete: return tags_read, True
        tags_read.append(tag_str)
    return tags_read, False

def _consume_activity_event_body_sync(f, event_type):
    try:
        if event_type == EVENT_TYPE_STREAM_START: _, i1 = _read_string_from_file_handle_sync(f); _, i2 = _read_string_from_file_handle_sync(f); _, i3 = _read_tag_list_from_file_handle_sync(f); return i1 or i2 or i3
        elif event_type == EVENT_TYPE_STREAM_END: return len(f.read(SA_INT_SIZE * 2)) < SA_INT_SIZE * 2
        elif event_type == EVENT_TYPE_GAME_CHANGE: _, i1 = _read_string_from_file_handle_sync(f); _, i2 = _read_string_from_file_handle_sync(f); return i1 or i2
        elif event_type == EVENT_TYPE_TITLE_CHANGE: _, i1 = _read_string_from_file_handle_sync(f); _, i2 = _read_string_from_file_handle_sync(f); return i1 or i2
        elif event_type == EVENT_TYPE_TAGS_CHANGE: _, i1 = _read_tag_list_from_file_handle_sync(f); _, i2 = _read_tag_list_from_file_handle_sync(f); return i1 or i2
        else: logger.warning(f"ConsumeHelper: Unknown event type {event_type}."); return True
    except Exception as e: logger.error(f"ConsumeHelper: Error consuming event type {event_type}: {e}"); return True

def _parse_stream_activity_for_game_segments_sync(filepath: str, query_start_unix: int = None, query_end_unix: int = None):
    if not filepath or not os.path.exists(filepath) or os.path.getsize(filepath) < SA_BASE_HEADER_SIZE:
        return []

    all_events = []
    try:
        with open(filepath, 'rb') as f:
            file_size = os.fstat(f.fileno()).st_size
            while True:
                current_pos_event_start = f.tell()
                if current_pos_event_start + SA_BASE_HEADER_SIZE > file_size: # Not enough for a header
                    break
                header_chunk = f.read(SA_BASE_HEADER_SIZE)
                if not header_chunk: break # End of file

                event_type, unix_ts = struct.unpack(SA_BASE_HEADER_FORMAT, header_chunk)
                event_data = {'type': event_type, 'timestamp': unix_ts}
                
                incomplete_body = False
                try:
                    if event_type == EVENT_TYPE_STREAM_START:
                        title, inc1 = _read_string_from_file_handle_sync(f)
                        game, inc2 = _read_string_from_file_handle_sync(f)
                        tags, inc3 = _read_tag_list_from_file_handle_sync(f)
                        
                        youtube_video_id = None
                        inc4 = False # Assume no YT ID field at first
                        current_pos_before_ytid = f.tell()

                        # Check if there's potentially a YT ID field (at least enough bytes for its length header)
                        if file_size - current_pos_before_ytid >= SA_STRING_LEN_SIZE:
                            # Peek at the length
                            len_bytes_peek = f.read(SA_STRING_LEN_SIZE)
                            s_len_peek = struct.unpack(SA_STRING_LEN_FORMAT, len_bytes_peek)[0]
                            f.seek(current_pos_before_ytid) # Rewind after peek

                            # If peeked length + length_header_size is within file bounds, try to read
                            if current_pos_before_ytid + SA_STRING_LEN_SIZE + s_len_peek <= file_size:
                                temp_yt_id, inc4_attempt = _read_string_from_file_handle_sync(f)
                                if not inc4_attempt:
                                    youtube_video_id = temp_yt_id
                                else: # Incomplete read for YT ID, likely old format or corruption
                                    f.seek(current_pos_before_ytid) # Rewind fully
                                    # inc4 remains False, this wasn't a valid YT ID field
                            # else: not enough data for the string itself, assume old format
                        # else: not enough data for YT ID length header, assume old format
                            
                        if inc1 or inc2 or inc3: # Don't include inc4 here as it's optional for old records
                            incomplete_body = True
                            logger.warning(f"GameSegParser: Incomplete STREAM_START base data at {unix_ts}.")
                        else: 
                            event_data.update({'title': title, 'game': game, 'tags': tags, 'youtube_video_id': youtube_video_id})

                    elif event_type == EVENT_TYPE_GAME_CHANGE:
                        old_game, inc1 = _read_string_from_file_handle_sync(f)
                        new_game, inc2 = _read_string_from_file_handle_sync(f)
                        if inc1 or inc2: 
                            incomplete_body = True; logger.warning(f"GameSegParser: Incomplete GAME_CHANGE data at {unix_ts}.")
                        else: event_data.update({'old_game': old_game, 'new_game': new_game})
                    elif event_type == EVENT_TYPE_TITLE_CHANGE:
                        old_title, inc1 = _read_string_from_file_handle_sync(f)
                        new_title, inc2 = _read_string_from_file_handle_sync(f)
                        if inc1 or inc2: 
                            incomplete_body = True; logger.warning(f"GameSegParser: Incomplete TITLE_CHANGE data at {unix_ts}.")
                        else: event_data.update({'old_title': old_title, 'new_title': new_title})
                    else: # For EVENT_TYPE_STREAM_END, EVENT_TYPE_TAGS_CHANGE, or unknown
                        incomplete_body = _consume_activity_event_body_sync(f, event_type)
                        if incomplete_body: 
                            logger.warning(f"GameSegParser: Incomplete or unhandled event body for type {event_type} at {unix_ts}.")
                        
                    if incomplete_body:
                        f.seek(current_pos_event_start) # Rewind to start of this failed event
                        logger.warning(f"GameSegParser: Skipping rest of file due to incomplete event type {event_type} at {unix_ts}.")
                        break 

                    if event_type in [EVENT_TYPE_STREAM_START, EVENT_TYPE_GAME_CHANGE, EVENT_TYPE_TITLE_CHANGE, EVENT_TYPE_STREAM_END]:
                        all_events.append(event_data)

                except struct.error as e:
                    logger.error(f"GameSegParser: Struct error processing event body (type {event_type}) at ts {unix_ts} in {filepath}: {e}"); break
                except Exception as e: 
                    logger.error(f"GameSegParser: Generic error processing event body (type {event_type}) at ts {unix_ts} in {filepath}: {e}"); break
            
    except FileNotFoundError:
        logger.error(f"GameSegParser: File not found: {filepath}"); return []
    except Exception as e:
        logger.error(f"GameSegParser: Error opening or reading {filepath}: {e}"); return []
                
    all_events.sort(key=lambda x: x['timestamp'])

    segments = []
    active_stream_info = None 

    for event in all_events:
        ts = event['timestamp']

        if query_start_unix and ts < query_start_unix:
            if event['type'] == EVENT_TYPE_STREAM_START:
                 active_stream_info = {'game': event.get('game',"N/A"), 'start_ts': ts, 'title': event.get('title',"N/A")}
            elif event['type'] == EVENT_TYPE_GAME_CHANGE and active_stream_info:
                 active_stream_info['game'] = event.get('new_game',"N/A")
                 active_stream_info['start_ts'] = ts 
            elif event['type'] == EVENT_TYPE_TITLE_CHANGE and active_stream_info:
                 active_stream_info['title'] = event.get('new_title',"N/A")
            elif event['type'] == EVENT_TYPE_STREAM_END:
                 active_stream_info = None
            continue

        if query_end_unix and ts > query_end_unix:
            if active_stream_info: 
                 seg_start_ts = max(active_stream_info['start_ts'], query_start_unix) if query_start_unix else active_stream_info['start_ts']
                 if query_end_unix > seg_start_ts:
                    segments.append({
                        'game': active_stream_info['game'], 
                        'start_ts': seg_start_ts,
                        'end_ts': query_end_unix,
                        'title_at_start': active_stream_info['title']
                    })
            active_stream_info = None 
            break 

        if event['type'] == EVENT_TYPE_STREAM_START:
            if active_stream_info: 
                prev_seg_start = max(active_stream_info['start_ts'], query_start_unix) if query_start_unix else active_stream_info['start_ts']
                if ts > prev_seg_start:
                    segments.append({
                        'game': active_stream_info['game'], 
                        'start_ts': prev_seg_start, 
                        'end_ts': ts, 
                        'title_at_start': active_stream_info['title']
                    })
            active_stream_info = {'game': event.get('game',"N/A"), 'start_ts': ts, 'title': event.get('title',"N/A")}

        elif event['type'] == EVENT_TYPE_GAME_CHANGE:
            if active_stream_info:
                seg_start_ts = max(active_stream_info['start_ts'], query_start_unix) if query_start_unix else active_stream_info['start_ts']
                if ts > seg_start_ts:
                    segments.append({
                        'game': active_stream_info['game'], 
                        'start_ts': seg_start_ts, 
                        'end_ts': ts, 
                        'title_at_start': active_stream_info['title']
                    })
                active_stream_info['game'] = event.get('new_game',"N/A")
                active_stream_info['start_ts'] = ts 
            else: 
                logger.debug(f"GameSegParser: GAME_CHANGE event at {ts} without an active stream context.")
                active_stream_info = {'game': event.get('new_game',"N/A"), 'start_ts': ts, 'title': "N/A (Title from before game change)"}

        elif event['type'] == EVENT_TYPE_TITLE_CHANGE:
            if active_stream_info:
                active_stream_info['title'] = event.get('new_title',"N/A")

        elif event['type'] == EVENT_TYPE_STREAM_END:
            if active_stream_info:
                seg_start_ts = max(active_stream_info['start_ts'], query_start_unix) if query_start_unix else active_stream_info['start_ts']
                seg_end_ts = min(ts, query_end_unix) if query_end_unix else ts
                if seg_end_ts > seg_start_ts:
                    segments.append({
                        'game': active_stream_info['game'], 
                        'start_ts': seg_start_ts, 
                        'end_ts': seg_end_ts, 
                        'title_at_start': active_stream_info['title']
                    })
            active_stream_info = None
    
    if active_stream_info: 
        seg_start_ts = max(active_stream_info['start_ts'], query_start_unix) if query_start_unix else active_stream_info['start_ts']
        effective_end_ts = query_end_unix if query_end_unix else (all_events[-1]['timestamp'] if all_events else seg_start_ts)
        
        if effective_end_ts > seg_start_ts:
             segments.append({
                'game': active_stream_info['game'], 
                'start_ts': seg_start_ts, 
                'end_ts': effective_end_ts, 
                'title_at_start': active_stream_info['title']
            })

    valid_segments = [s for s in segments if s.get('game') and s['end_ts'] > s['start_ts']]
    return valid_segments

# --- Duration Parsing & Human Formatting ---
def parse_duration_to_timedelta(duration_str: str):
    if not duration_str: return None, "No duration provided."
    duration_str = duration_str.lower().strip()
    match = re.fullmatch(r"(\d+)\s*(m|min|mins|minute|minutes|h|hr|hrs|hour|hours|d|day|days|w|wk|wks|week|weeks|mo|mon|mth|month|months|y|yr|yrs|year|years)", duration_str)
    if not match: return None, "Invalid duration format. Use N<unit>. Examples: 10m, 2h, 3d, 1w, 1mo, 1y."
    value = int(match.group(1)); unit = match.group(2)
    if value <= 0: return None, "Duration value must be > 0."
    delta, period_name = None, ""
    if unit in ["m", "min", "mins", "minute", "minutes"]: delta, period_name = timedelta(minutes=value), f"last {value} minute{'s' if value > 1 else ''}"
    elif unit in ["h", "hr", "hrs", "hour", "hours"]: delta, period_name = timedelta(hours=value), f"last {value} hour{'s' if value > 1 else ''}"
    elif unit in ["d", "day", "days"]: delta, period_name = timedelta(days=value), f"last {value} day{'s' if value > 1 else ''}"
    elif unit in ["w", "wk", "wks", "week", "weeks"]: delta, period_name = timedelta(weeks=value), f"last {value} week{'s' if value > 1 else ''}"
    elif unit in ["mo", "mon", "mth", "month", "months"]: delta, period_name = timedelta(days=value * 30), f"last {value} month{'s' if value > 1 else ''} (approx. {value*30}d)"
    elif unit in ["y", "yr", "yrs", "year", "years"]: delta, period_name = timedelta(days=value * 365), f"last {value} year{'s' if value > 1 else ''} (approx. {value*365}d)"
    return (delta, period_name) if delta else (None, "Internal error: Unrecognized unit.")

def format_duration_human(total_seconds: int) -> str:
    if total_seconds < 0: total_seconds = 0
    if total_seconds == 0: return "no time"
    days, remainder = divmod(total_seconds, 86400); hours, remainder = divmod(remainder, 3600); minutes, seconds = divmod(remainder, 60)
    parts = []
    if days > 0: parts.append(f"{days} day{'s' if days > 1 else ''}")
    if hours > 0: parts.append(f"{hours} hour{'s' if hours > 1 else ''}")
    if minutes > 0: parts.append(f"{minutes} minute{'s' if minutes > 1 else ''}")
    if seconds > 0 or not parts : parts.append(f"{seconds} second{'s' if seconds > 1 else ''}")
    if not parts: return "less than a second"
    return ", ".join(parts)

# --- fctd: Follower Gain Logic, etc. ---
def _read_and_find_records_sync(filepath, cutoff_timestamp_unix, inclusive_end_ts_for_start_val=None):
    start_count, end_count, first_ts_unix, last_ts_unix = None, None, None, None
    all_records_in_file = []

    if not filepath or not os.path.exists(filepath) or os.path.getsize(filepath) < BINARY_RECORD_SIZE:
        return None, None, None, None, None

    try:
        with open(filepath, 'rb') as f:
            while True:
                chunk = f.read(BINARY_RECORD_SIZE)
                if not chunk: break
                if len(chunk) < BINARY_RECORD_SIZE:
                    logger.warning(f"Incomplete record in {filepath}."); break
                unix_ts, count = struct.unpack(BINARY_RECORD_FORMAT, chunk)
                all_records_in_file.append((unix_ts, count))
    except FileNotFoundError: return None, None, None, None, None
    except Exception as e: logger.error(f"Error reading {filepath}: {e}"); return None, None, None, None, None

    if not all_records_in_file:
        return None, None, None, None, None

    temp_start_c, temp_first_ts = None, None
    for ts, count in reversed(all_records_in_file):
        if ts <= cutoff_timestamp_unix:
            temp_start_c, temp_first_ts = count, ts
            break
    if temp_start_c is None and all_records_in_file:
        temp_first_ts_candidate, temp_start_c_candidate = all_records_in_file[0]
        if inclusive_end_ts_for_start_val is None or temp_first_ts_candidate <= inclusive_end_ts_for_start_val :
           temp_first_ts, temp_start_c = temp_first_ts_candidate, temp_start_c_candidate

    first_ts_unix, start_count = temp_first_ts, temp_start_c

    if inclusive_end_ts_for_start_val is None:
        last_ts_unix, end_count = all_records_in_file[-1]
    else:
        temp_end_c, temp_last_ts = None, None
        for ts, count in reversed(all_records_in_file):
            if ts <= inclusive_end_ts_for_start_val:
                temp_end_c, temp_last_ts = count, ts
                break

        if temp_end_c is None and all_records_in_file:
            if start_count is not None and first_ts_unix <= inclusive_end_ts_for_start_val:
                 temp_last_ts, temp_end_c = first_ts_unix, start_count

        last_ts_unix, end_count = temp_last_ts, temp_end_c

        if last_ts_unix is not None and first_ts_unix is not None and last_ts_unix < first_ts_unix:
            last_ts_unix, end_count = first_ts_unix, start_count

    return start_count, end_count, first_ts_unix, last_ts_unix, all_records_in_file


async def get_follower_gain_for_period(time_delta: timedelta, period_name_full: str):
    now_utc = datetime.now(timezone.utc); cutoff_datetime = now_utc - time_delta; cutoff_timestamp_unix = int(cutoff_datetime.timestamp())
    start_c, end_c, first_ts, last_ts, all_recs_from_file = await asyncio.to_thread(
        _read_and_find_records_sync,
        FCTD_FOLLOWER_DATA_FILE,
        cutoff_timestamp_unix,
        None
    )
    if end_c is None or last_ts is None: return f"Not enough data in `{FCTD_FOLLOWER_DATA_FILE}`."
    if start_c is None or first_ts is None:
        if all_recs_from_file:
            oldest_ts, oldest_count = all_recs_from_file[0]
            current_ts, current_count = all_recs_from_file[-1]
            gain = current_count - oldest_count
            g_msg = f"gained {gain:,}" if gain > 0 else f"lost {-gain:,}" if gain < 0 else "no change in"
            oldest_dt_display = datetime.fromtimestamp(oldest_ts, timezone.utc)
            return (f"Not enough data for the start of {period_name_full}. Oldest available data is from {discord.utils.format_dt(oldest_dt_display, 'R')}.\n"
                    f"Since then, {FCTD_TWITCH_USERNAME} has {g_msg} followers. Current: {current_count:,}")
        return "Could not determine start point from data."

    gain = end_c - start_c
    actual_start_dt = datetime.fromtimestamp(first_ts, timezone.utc)
    actual_end_dt = datetime.fromtimestamp(last_ts, timezone.utc)

    final_period_desc = period_name_full
    if actual_start_dt > cutoff_datetime + timedelta(minutes=max(1, time_delta.total_seconds() * 0.1 / 60)):
        effective_data_span_within_period = actual_end_dt - actual_start_dt
        human_s = format_duration_human(int(effective_data_span_within_period.total_seconds()))
        final_period_desc = f"{period_name_full} (effective data spans ~{human_s} from {discord.utils.format_dt(actual_start_dt,'R')})"

    if gain == 0: return f"{FCTD_TWITCH_USERNAME} follower count stable at {end_c:,} in {final_period_desc}.\n(Data from {discord.utils.format_dt(actual_start_dt, 'R')} to {discord.utils.format_dt(actual_end_dt, 'R')})"
    g_t, c_t = ("gained", f"{abs(gain):,}") if gain > 0 else ("lost", f"{abs(gain):,}")
    return (f"{FCTD_TWITCH_USERNAME} {g_t} {c_t} followers in {final_period_desc}.\n"
            f"From {start_c:,} ({discord.utils.format_dt(actual_start_dt, 'R')}) to {end_c:,} ({discord.utils.format_dt(actual_end_dt, 'R')}).")

def _get_counts_for_day_boundaries_sync(filepath: str, target_date_obj: date):
    day_start_utc = datetime.combine(target_date_obj, datetime.min.time(), tzinfo=timezone.utc)
    day_end_utc = datetime.combine(target_date_obj, datetime.max.time(), tzinfo=timezone.utc)
    day_start_unix, day_end_unix = int(day_start_utc.timestamp()), int(day_end_utc.timestamp())

    all_records = []
    if not os.path.exists(filepath) or os.path.getsize(filepath) < BINARY_RECORD_SIZE:
        return "No data file or too small."

    try:
        with open(filepath, 'rb') as f:
            while True:
                chunk = f.read(BINARY_RECORD_SIZE)
                if not chunk: break
                if len(chunk) < BINARY_RECORD_SIZE: logger.warning(f"Incomplete record in {filepath} for daystats."); break
                unix_ts, count = struct.unpack(BINARY_RECORD_FORMAT, chunk)
                all_records.append({'ts': unix_ts, 'count': count})
    except FileNotFoundError: return f"File {filepath} not found."
    except Exception as e: logger.error(f"Error reading {filepath} for daystats: {e}"); return f"Error reading data file."

    if not all_records: return "Data file empty."

    all_records.sort(key=lambda x: x['ts'])

    if all_records[-1]['ts'] < day_start_unix:
        return f"All data ends before {target_date_obj.isoformat()} (last: {discord.utils.format_dt(datetime.fromtimestamp(all_records[-1]['ts'], tz=timezone.utc), 'f')})."
    if all_records[0]['ts'] > day_end_unix:
        return f"All data begins after {target_date_obj.isoformat()} (first: {discord.utils.format_dt(datetime.fromtimestamp(all_records[0]['ts'], tz=timezone.utc), 'f')})."

    eff_start_r = None
    for rec in reversed(all_records):
        if rec['ts'] <= day_start_unix:
            eff_start_r = rec
            break
    if eff_start_r is None:
        eff_start_r = all_records[0]

    eff_end_r = None
    records_on_day_for_plot = []
    num_recs_on_day = 0

    for rec in all_records:
        if day_start_unix <= rec['ts'] <= day_end_unix:
            records_on_day_for_plot.append(rec)
            num_recs_on_day +=1
            if eff_end_r is None or rec['ts'] >= eff_end_r['ts']:
                eff_end_r = rec

    if eff_end_r is None:
        eff_end_r = eff_start_r

    if not eff_start_r: return f"Could not determine start count for {target_date_obj.isoformat()}."
    if not eff_end_r: return f"Could not determine end count for {target_date_obj.isoformat()}."

    return {
        'start_ts': eff_start_r['ts'], 'start_count': eff_start_r['count'],
        'end_ts': eff_end_r['ts'], 'end_count': eff_end_r['count'],
        'num_records_on_day': num_recs_on_day,
        'day_start_utc': day_start_utc, 'day_end_utc': day_end_utc,
        'records_for_plot': records_on_day_for_plot
    }

# --- Helper for Stream Time Commands ---
def _read_stream_durations_for_period_sync(filepath: str, query_start_unix: int, query_end_unix: int) -> tuple[int, int]:
    if not filepath or not os.path.exists(filepath) or os.path.getsize(filepath) < STREAM_DURATION_RECORD_SIZE: return 0, 0
    total_duration_seconds, num_streams_in_period = 0, 0
    try:
        with open(filepath, 'rb') as f:
            while True:
                chunk = f.read(STREAM_DURATION_RECORD_SIZE)
                if not chunk: break
                if len(chunk) < STREAM_DURATION_RECORD_SIZE: logger.warning(f"Incomplete record in {filepath} reading stream durations."); break
                stream_start_ts, stream_end_ts = struct.unpack(STREAM_DURATION_RECORD_FORMAT, chunk)
                overlap_start, overlap_end = max(stream_start_ts, query_start_unix), min(stream_end_ts, query_end_unix)
                if overlap_start < overlap_end:
                    total_duration_seconds += (overlap_end - overlap_start); num_streams_in_period +=1
    except FileNotFoundError: return 0,0
    except Exception as e: logger.error(f"Error reading {filepath} for stream durations: {e}"); return 0,0
    return total_duration_seconds, num_streams_in_period

# --- Helper for Average Viewers ---
def _get_viewer_stats_for_period_sync(viewer_log_file: str, start_ts_unix: int, end_ts_unix: int) -> tuple[float | None, int, int]:
    """ Returns (average_viewers, peak_viewers_in_period, num_datapoints) """
    if not viewer_log_file or not os.path.exists(viewer_log_file) or \
       not UTA_VIEWER_COUNT_LOGGING_ENABLED or os.path.getsize(viewer_log_file) < BINARY_RECORD_SIZE:
        return None, 0, 0

    viewer_counts_in_period = []
    peak_viewers = 0
    try:
        with open(viewer_log_file, 'rb') as f:
            while True:
                chunk = f.read(BINARY_RECORD_SIZE)
                if not chunk: break
                if len(chunk) < BINARY_RECORD_SIZE: break
                ts, count = struct.unpack(BINARY_RECORD_FORMAT, chunk)
                if start_ts_unix <= ts < end_ts_unix:
                    viewer_counts_in_period.append(count)
                    if count > peak_viewers:
                        peak_viewers = count
    except Exception as e:
        logger.error(f"Error reading viewer log '{viewer_log_file}' for stats: {e}")
        return None, 0, 0

    if not viewer_counts_in_period:
        return None, 0, 0

    avg_viewers = sum(viewer_counts_in_period) / len(viewer_counts_in_period)
    return avg_viewers, peak_viewers, len(viewer_counts_in_period)

# --- Helper for Bot Runtime Command ---
def _calculate_runtime_in_period_sync(filepath: str, query_start_unix: int, query_end_unix: int) -> tuple[int, int]:
    """ Calculates total bot runtime within a query period from session logs.
        Returns (total_uptime_seconds_in_period, number_of_contributing_sessions)
    """
    if not filepath or not os.path.exists(filepath) or os.path.getsize(filepath) < BOT_SESSION_RECORD_SIZE:
        return 0, 0

    records = []
    try:
        with open(filepath, 'rb') as f:
            while True:
                chunk = f.read(BOT_SESSION_RECORD_SIZE)
                if not chunk: break
                if len(chunk) < BOT_SESSION_RECORD_SIZE:
                    logger.warning(f"Incomplete record in bot session log '{filepath}'. Skipping rest."); break
                event_type, ts = struct.unpack(BOT_SESSION_RECORD_FORMAT, chunk)
                records.append({'type': event_type, 'ts': ts})
    except Exception as e:
        logger.error(f"Error reading bot session log '{filepath}': {e}"); return 0, 0

    if not records: return 0, 0
    records.sort(key=lambda r: r['ts'])

    total_uptime_seconds = 0
    num_sessions_contributing = 0
    active_session_start_ts = None

    for record in records:
        event_ts = record['ts']
        event_type = record['type']

        if event_type == BOT_EVENT_START:
            if active_session_start_ts is not None:
                session_end_ts = event_ts
                overlap_start = max(active_session_start_ts, query_start_unix)
                overlap_end = min(session_end_ts, query_end_unix)
                if overlap_end > overlap_start:
                    total_uptime_seconds += (overlap_end - overlap_start)
                    num_sessions_contributing +=1
                logger.warning(f"Bot session log: START event at {event_ts} while a session was already active from {active_session_start_ts}. Previous session considered ended.")
            active_session_start_ts = event_ts

        elif event_type == BOT_EVENT_STOP:
            if active_session_start_ts is not None:
                session_end_ts = event_ts
                overlap_start = max(active_session_start_ts, query_start_unix)
                overlap_end = min(session_end_ts, query_end_unix)
                if overlap_end > overlap_start:
                    total_uptime_seconds += (overlap_end - overlap_start)
                    num_sessions_contributing +=1
                active_session_start_ts = None
            else:
                logger.warning(f"Bot session log: STOP event at {event_ts} without an active session start. Ignoring.")

    if active_session_start_ts is not None:
        session_end_ts = query_end_unix
        overlap_start = max(active_session_start_ts, query_start_unix)
        overlap_end = min(session_end_ts, query_end_unix)
        if overlap_end > overlap_start:
            total_uptime_seconds += (overlap_end - overlap_start)
            num_sessions_contributing +=1

    return total_uptime_seconds, num_sessions_contributing


# =====================================================================================
# --- UTA (Universal Twitch Assistant) Integration ---
# =====================================================================================
uta_shared_access_token, uta_token_expiry_time, uta_token_refresh_lock = None, 0, threading.Lock()
uta_broadcaster_id_cache, uta_sent_clip_ids = None, set()
uta_streamlink_process, uta_ffmpeg_process, uta_is_restreaming_active = None, None, False
shutdown_event = threading.Event()
uta_clip_thread, uta_restreamer_thread, uta_stream_status_thread = None, None, None

uta_yt_service = None
uta_current_youtube_broadcast_id = None
uta_current_youtube_video_id = None
uta_current_youtube_live_stream_id = None
uta_current_youtube_rtmp_url = None
uta_current_youtube_stream_key = None
uta_current_restream_part_number = 1
uta_youtube_next_rollover_time_utc = None
UTA_RESTREAM_CONSECUTIVE_FAILURES = 0

# --- UTA: YouTube API Helper Functions ---
def _uta_youtube_get_service():
    global uta_yt_service
    if not GOOGLE_API_AVAILABLE:
        logger.error("UTA YouTube: Google API client libraries not available. YouTube API features disabled.")
        return None
    if uta_yt_service:
        return uta_yt_service

    creds = None
    if os.path.exists(UTA_YOUTUBE_TOKEN_FILE):
        try:
            creds = GoogleCredentials.from_authorized_user_file(UTA_YOUTUBE_TOKEN_FILE, UTA_YOUTUBE_API_SCOPES)
        except Exception as e:
            logger.error(f"UTA YouTube: Error loading token file {UTA_YOUTUBE_TOKEN_FILE}: {e}")

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                logger.info("UTA YouTube: Refreshing expired YouTube API token.")
                creds.refresh(GoogleAuthRequest())
            except Exception as e:
                logger.error(f"UTA YouTube: Failed to refresh YouTube API token: {e}")
                creds = None
        if not creds:
            if not os.path.exists(UTA_YOUTUBE_CLIENT_SECRET_FILE):
                logger.error(f"UTA YouTube: Client secret file '{UTA_YOUTUBE_CLIENT_SECRET_FILE}' not found. Cannot authenticate.")
                return None
            try:
                logger.info("UTA YouTube: Initiating new YouTube API OAuth flow. Please follow browser instructions.")
                # For Desktop app type, run_local_server is generally preferred if a browser can be opened.
                # It will attempt to open the default web browser.
                flow = InstalledAppFlow.from_client_secrets_file(
                    UTA_YOUTUBE_CLIENT_SECRET_FILE, UTA_YOUTUBE_API_SCOPES
                )
                # The port can be 0 to pick a random available port, or a specific one.
                # If running truly headless where no browser can be launched, this will still be an issue.
                # In such a case, generating youtube_token.json offline and deploying it is the best approach.
                creds = flow.run_local_server(port=0) # Port 0 will find an available port
                logger.info("UTA YouTube: OAuth flow completed through local server.")

            except Exception as e:
                logger.error(f"UTA YouTube: OAuth flow failed: {e}", exc_info=True)
                logger.error(f"UTA YouTube: If you are on a headless server or cannot open a browser, "
                             f"you may need to generate '{UTA_YOUTUBE_TOKEN_FILE}' manually once on a "
                             f"machine with a browser and then copy it to the server.")
                return None

        if creds:
            try:
                with open(UTA_YOUTUBE_TOKEN_FILE, 'w') as token_file:
                    token_file.write(creds.to_json())
                logger.info(f"UTA YouTube: Token saved to {UTA_YOUTUBE_TOKEN_FILE}")
            except Exception as e:
                logger.error(f"UTA YouTube: Error saving token file {UTA_YOUTUBE_TOKEN_FILE}: {e}")

    if creds and creds.valid:
        try:
            uta_yt_service = google_build('youtube', 'v3', credentials=creds, cache_discovery=False)
            logger.info("UTA YouTube: YouTube API service initialized successfully.")
            return uta_yt_service
        except Exception as e:
            logger.error(f"UTA YouTube: Failed to build YouTube service: {e}")
            return None
    else:
        logger.error("UTA YouTube: Failed to obtain valid credentials for YouTube API.")
        return None

async def _uta_youtube_create_live_stream_resource(service, twitch_username):
    if not service: return None, None, None
    stream_title = f"UTA Restream Endpoint for {twitch_username} - {int(time.time())}-{random.randint(1000,9999)}"
    try:
        request = service.liveStreams().insert(
            part="snippet,cdn,status",
            body={
                "snippet": {
                    "title": stream_title,
                    "description": "Reusable stream resource for UTA bot"
                },
                "cdn": {
                    "frameRate": "variable",
                    "ingestionType": "rtmp",
                    "resolution": "variable"
                },
                "status": {"streamStatus": "ready"}
            }
        )
        response = await asyncio.to_thread(request.execute)
        stream_id = response['id']
        ingestion_info = response['cdn']['ingestionInfo']
        rtmp_url = ingestion_info['ingestionAddress']
        stream_key = ingestion_info['streamName']
        logger.info(f"UTA YouTube: Created liveStream resource ID: {stream_id} for {twitch_username}")
        return stream_id, rtmp_url, stream_key
    except GoogleHttpError as e:
        logger.error(f"UTA YouTube: API error creating liveStream resource: {e.content.decode() if e.content else e}", exc_info=True)
    except Exception as e:
        logger.error(f"UTA YouTube: Failed to create liveStream resource: {e}", exc_info=True)
    return None, None, None

async def _uta_youtube_create_broadcast(service, bound_live_stream_id, title, description, privacy_status, start_time_iso):
    if not service: return None
    try:
        request = service.liveBroadcasts().insert(
            part="snippet,status,contentDetails",
            body={
                "snippet": {
                    "title": title,
                    "description": description,
                    "scheduledStartTime": start_time_iso,
                },
                "status": {
                    "privacyStatus": privacy_status,
                    "selfDeclaredMadeForKids": False,
                },
                "contentDetails": {
                    "enableAutoStart": True,
                    "enableAutoStop": True,
                    "latencyPreference": "ultraLow",
                    "enableDvr": True,
                    "boundStreamId": bound_live_stream_id
                }
            }
        )
        response = await asyncio.to_thread(request.execute)
        broadcast_id = response['id']
        logger.info(f"UTA YouTube: Created liveBroadcast ID: {broadcast_id} (Title: {title})")
        return broadcast_id
    except GoogleHttpError as e:
        logger.error(f"UTA YouTube: API error creating liveBroadcast: {e.content.decode() if e.content else e}", exc_info=True)
    except Exception as e:
        logger.error(f"UTA YouTube: Failed to create liveBroadcast: {e}", exc_info=True)
    return None

async def _uta_youtube_transition_broadcast(service, broadcast_id, status):
    if not service or not broadcast_id: return False
    try:
        request = service.liveBroadcasts().transition(
            broadcastStatus=status,
            id=broadcast_id,
            part="id,snippet,contentDetails,status"
        )
        await asyncio.to_thread(request.execute)
        logger.info(f"UTA YouTube: Transitioned broadcast {broadcast_id} to {status}.")
        return True
    except GoogleHttpError as e:
        logger.error(f"UTA YouTube: API error transitioning broadcast {broadcast_id} to {status}: {e.content.decode() if e.content else e}", exc_info=True)
    except Exception as e:
        logger.error(f"UTA YouTube: Failed to transition broadcast {broadcast_id} to {status}: {e}", exc_info=True)
    return False

async def _uta_youtube_update_broadcast_metadata(service, broadcast_id, new_title, new_description):
    if not service or not broadcast_id: return False
    try:
        request_body = {"id": broadcast_id, "snippet": {}}
        if new_title: request_body["snippet"]["title"] = new_title
        if new_description: request_body["snippet"]["description"] = new_description
        if not request_body["snippet"]: return True

        request = service.videos().update(part="snippet", body=request_body)
        await asyncio.to_thread(request.execute)
        logger.info(f"UTA YouTube: Updated metadata for broadcast/video {broadcast_id}.")
        return True
    except GoogleHttpError as e:
        logger.error(f"UTA YouTube: API error updating metadata for {broadcast_id}: {e.content.decode() if e.content else e}", exc_info=True)
    except Exception as e:
        logger.error(f"UTA YouTube: Failed to update metadata for {broadcast_id}: {e}", exc_info=True)
    return False

async def _uta_youtube_add_video_to_playlist(service, video_id, playlist_id):
    if not service or not video_id or not playlist_id: return False
    try:
        request = service.playlistItems().insert(
            part="snippet",
            body={
                "snippet": {
                    "playlistId": playlist_id,
                    "resourceId": {"kind": "youtube#video", "videoId": video_id}
                }
            }
        )
        await asyncio.to_thread(request.execute)
        logger.info(f"UTA YouTube: Added video {video_id} to playlist {playlist_id}.")
        return True
    except GoogleHttpError as e:
        if e.resp.status == 409 and "playlistItemNotUnique" in str(e.content):
            logger.info(f"UTA YouTube: Video {video_id} already in playlist {playlist_id}.")
            return True
        logger.error(f"UTA YouTube: API error adding video {video_id} to playlist {playlist_id}: {e.content.decode() if e.content else e}", exc_info=True)
    except Exception as e:
        logger.error(f"UTA YouTube: Failed to add video {video_id} to playlist {playlist_id}: {e}", exc_info=True)
    return False

async def _uta_youtube_set_video_privacy(service, video_id, privacy_status):
    if not service or not video_id: return False
    try:
        request = service.videos().update(
            part="status",
            body={"id": video_id, "status": {"privacyStatus": privacy_status}}
        )
        await asyncio.to_thread(request.execute)
        logger.info(f"UTA YouTube: Set privacy of video {video_id} to {privacy_status}.")
        return True
    except GoogleHttpError as e:
        logger.error(f"UTA YouTube: API error setting privacy for video {video_id}: {e.content.decode() if e.content else e}", exc_info=True)
    except Exception as e:
        logger.error(f"UTA YouTube: Failed to set privacy for video {video_id}: {e}", exc_info=True)
    return False


def uta_get_twitch_access_token():
    global uta_shared_access_token, uta_token_expiry_time
    with uta_token_refresh_lock:
        current_time = time.time()
        if uta_shared_access_token and current_time < uta_token_expiry_time - 60: return uta_shared_access_token
        logger.info("UTA: Attempting to fetch/refresh Twitch API access token...")
        params = {"client_id": TWITCH_CLIENT_ID, "client_secret": TWITCH_CLIENT_SECRET, "grant_type": "client_credentials"}
        response_obj = None
        try:
            response_obj = requests.post(UTA_TWITCH_AUTH_URL, params=params, timeout=10); response_obj.raise_for_status()
            data = response_obj.json(); uta_shared_access_token = data["access_token"]; uta_token_expiry_time = current_time + data["expires_in"]
            logger.info("UTA: Successfully obtained/refreshed Twitch access token."); return uta_shared_access_token
        except requests.exceptions.RequestException as e:
            logger.error(f"UTA: Error getting Twitch access token: {e}")
            if hasattr(e, 'response') and e.response is not None: logger.error(f"UTA: Response content: {e.response.text}")
            uta_shared_access_token = None; uta_token_expiry_time = 0; return None
        except (KeyError, json.JSONDecodeError) as e:
            logger.error(f"UTA: Error parsing access token response: {e}")
            if response_obj is not None and hasattr(response_obj, 'text'): logger.error(f"UTA: Raw response text: {response_obj.text}")
            uta_shared_access_token = None; uta_token_expiry_time = 0; return None

def _uta_make_twitch_api_request(endpoint, params=None, method='GET', max_retries=1):
    url = f"{UTA_TWITCH_API_BASE_URL}/{endpoint.lstrip('/')}"
    for attempt in range(max_retries + 1):
        access_token = uta_get_twitch_access_token()
        if not access_token: logger.error(f"UTA: No token for API req to {url}."); return None
        headers = {"Client-ID": TWITCH_CLIENT_ID, "Authorization": f"Bearer {access_token}"}
        resp_obj = None
        try:
            if method.upper() == 'GET': resp_obj = requests.get(url, headers=headers, params=params, timeout=10)
            elif method.upper() == 'POST': resp_obj = requests.post(url, headers=headers, json=params, timeout=10)
            else: logger.error(f"UTA: Unsupported HTTP method: {method}"); return None
            if resp_obj.status_code == 401 and attempt < max_retries:
                logger.warning(f"UTA: API 401 for {url}. Retry {attempt + 1}/{max_retries + 1}.")
                global uta_shared_access_token, uta_token_expiry_time
                with uta_token_refresh_lock: uta_shared_access_token = None; uta_token_expiry_time = 0
                continue
            resp_obj.raise_for_status(); return resp_obj.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"UTA: Error API req to {url} (attempt {attempt + 1}): {e}")
            if resp_obj is not None and hasattr(resp_obj, 'text'): logger.error(f"UTA: Response content: {resp_obj.text}")
            if attempt >= max_retries: return None
            time.sleep(2**attempt)
        except (KeyError, IndexError, json.JSONDecodeError) as e:
            logger.error(f"UTA: Error parsing API response from {url}: {e}")
            if resp_obj is not None and hasattr(resp_obj, 'text'): logger.error(f"UTA: Raw response content that failed parsing: {resp_obj.text}")
            return None
    logger.error(f"UTA: Failed request to {url} after {max_retries + 1} attempts."); return None

def uta_get_broadcaster_id(channel_name):
    global uta_broadcaster_id_cache
    if not channel_name: logger.warning("UTA: Attempted to get broadcaster ID with no channel name specified."); return None
    if uta_broadcaster_id_cache: return uta_broadcaster_id_cache
    data = _uta_make_twitch_api_request("/users", params={"login": channel_name})
    if data and data.get("data"):
        uta_broadcaster_id_cache = data["data"][0]["id"]
        logger.info(f"UTA: Found broadcaster ID for {channel_name}: {uta_broadcaster_id_cache}"); return uta_broadcaster_id_cache
    logger.error(f"UTA: Could not find broadcaster ID for: {channel_name}"); return None

def uta_get_recent_clips(broadcaster_id, lookback_minutes):
    if not broadcaster_id: logger.error("UTA: No broadcaster ID for clips."); return []
    end_time_utc = datetime.now(timezone.utc); start_time_utc = end_time_utc - timedelta(minutes=lookback_minutes)
    formatted_start_time = start_time_utc.isoformat().replace('+00:00', 'Z')
    params = {"broadcaster_id": broadcaster_id, "started_at": formatted_start_time, "first": 20 }
    data = _uta_make_twitch_api_request("/clips", params=params)
    return data.get("data", []) if data else []

def uta_is_streamer_live(channel_name):
    if not channel_name: logger.warning("UTA: Attempted to check live status with no channel name specified."); return False, None
    params = {"user_login": channel_name}
    data = _uta_make_twitch_api_request("/streams", params=params)
    if data and data.get("data") and len(data["data"]) > 0 and data["data"][0].get("type") == "live": return True, data["data"][0]
    return False, None

async def _send_uta_notification_to_discord(message_content: str, embed: discord.Embed = None, file: discord.File = None):
    sent_to_webhook, sent_to_channel = False, False
    if UTA_STREAM_STATUS_WEBHOOK_URL and "YOUR_DISCORD_WEBHOOK_URL" not in UTA_STREAM_STATUS_WEBHOOK_URL :
        payload = {}
        if message_content: payload["content"] = message_content
        if embed: payload["embeds"] = [embed.to_dict()]

        files_for_webhook = {}
        if file:
            files_for_webhook={'file': (file.filename, file.fp, 'image/png')}
            response = await asyncio.to_thread(requests.post, UTA_STREAM_STATUS_WEBHOOK_URL, data={'payload_json': json.dumps(payload)}, files=files_for_webhook, timeout=15)
            file.fp.seek(0)
        else:
            response = await asyncio.to_thread(requests.post, UTA_STREAM_STATUS_WEBHOOK_URL, json=payload, timeout=10)

        try:
            response.raise_for_status()
            logger.info(f"UTA Stream Status: Sent webhook notification for {UTA_TWITCH_CHANNEL_NAME}")
            sent_to_webhook = True
        except Exception as e:
            logger.error(f"UTA Stream Status: Error sending webhook notification: {e}. Response: {response.text if hasattr(response, 'text') else 'N/A'}")

    if UTA_STREAM_STATUS_CHANNEL_ID and bot and not sent_to_webhook:
        try:
            channel = bot.get_channel(UTA_STREAM_STATUS_CHANNEL_ID)
            if channel:
                await channel.send(content=message_content, embed=embed, file=file)
                logger.info(f"UTA Stream Status: Sent channel message for {UTA_TWITCH_CHANNEL_NAME}")
                sent_to_channel = True
            else: logger.warning(f"UTA Stream Status: Notification channel {UTA_STREAM_STATUS_CHANNEL_ID} not found.")
        except Exception as e: logger.error(f"UTA Stream Status: Error sending channel message: {e}")

    if not sent_to_webhook and not sent_to_channel:
        logger.debug("UTA Stream Status: No webhook or channel ID configured, or both failed.")


def uta_send_discord_clip_notification(clip_url, clip_title, channel_name):
    if not UTA_DISCORD_WEBHOOK_URL_CLIPS or "YOUR_DISCORD_WEBHOOK_URL" in UTA_DISCORD_WEBHOOK_URL_CLIPS: logger.warning("UTA: Clips webhook N/A."); return
    payload = {"content": f"🎬 New clip from **{channel_name}**!\n**{clip_title}**\n{clip_url}"}
    response_obj = None
    try:
        response_obj = requests.post(UTA_DISCORD_WEBHOOK_URL_CLIPS, json=payload, timeout=10); response_obj.raise_for_status()
        logger.info(f"UTA: Sent clip to Discord: {clip_url}")
    except requests.exceptions.RequestException as e:
        logger.error(f"UTA: Error sending clip to Discord: {e}")
        if response_obj is not None and hasattr(response_obj, 'text'): logger.error(f"UTA: Response content: {response_obj.text}")

def uta_send_discord_restream_status(status_type, username, stream_data=None, stream_duration_seconds=None):
    if not UTA_DISCORD_WEBHOOK_URL_RESTREAMER or "YOUR_DISCORD_WEBHOOK_URL" in UTA_DISCORD_WEBHOOK_URL_RESTREAMER: logger.warning("UTA: Restream webhook N/A."); return
    color = 15158332 if status_type == "stop" else 3066993
    title_text = f":{'stop_button' if status_type == 'stop' else 'satellite'}: Restream {'STOPPED' if status_type == 'stop' else 'STARTED'}"
    desc = f"Restream of **{username}** to YouTube has stopped."
    if status_type == "start":
        s_title = stream_data.get("title", "N/A") if stream_data else "N/A"; game = stream_data.get("game_name", "N/A") if stream_data else "N/A"
        desc = f"Now restreaming **{username}** to YouTube.\nTwitch Title: **{s_title}**\nGame: **{game}**\n[Watch on Twitch](https://twitch.tv/{username})"
    elif status_type == "stop" and stream_duration_seconds is not None and stream_duration_seconds > 0:
        desc += f"\nStream lasted for: **{format_duration_human(int(stream_duration_seconds))}**."
    current_time_utc_iso = datetime.now(timezone.utc).isoformat()
    payload = {"content": f"{title_text} for **{username}**", "embeds": [{"title": title_text, "description": desc, "color": color, "timestamp": current_time_utc_iso, "author": {"name": username, "url": f"https://twitch.tv/{username}"}, "footer": {"text": "Twitch Monitor & Restreamer (UTA)"}}]}
    response_obj = None
    try:
        response_obj = requests.post(UTA_DISCORD_WEBHOOK_URL_RESTREAMER, json=payload, timeout=10); response_obj.raise_for_status()
        logger.info(f"UTA: Sent Discord restream {status_type} for {username}")
    except requests.exceptions.RequestException as e:
        logger.error(f"UTA: Error sending restream status to Discord: {e}")
        if response_obj is not None and hasattr(response_obj, 'text'): logger.error(f"UTA: Response content: {response_obj.text}")

def uta_terminate_process(process, name):
    if process and process.poll() is None:
        logger.info(f"UTA: Terminating {name} (PID: {process.pid})...")
        try: process.terminate(); process.wait(timeout=10); logger.info(f"UTA: {name} terminated (Code: {process.poll()}).")
        except subprocess.TimeoutExpired: logger.warning(f"UTA: {name} (PID: {process.pid}) did not terminate gracefully, killing..."); process.kill(); process.wait(); logger.info(f"UTA: {name} process killed (Code: {process.poll()}).")
        except Exception as e: logger.error(f"UTA: Error terminating {name} (PID: {process.pid}): {e}")

def uta_cleanup_restream_processes():
    global uta_streamlink_process, uta_ffmpeg_process, uta_is_restreaming_active
    logger.info("UTA: Cleaning up restream processes..."); uta_terminate_process(uta_ffmpeg_process, "FFmpeg"); uta_ffmpeg_process = None
    uta_terminate_process(uta_streamlink_process, "Streamlink"); uta_streamlink_process = None
    uta_is_restreaming_active = False; logger.info("UTA: Restream cleanup finished.")

async def _uta_check_youtube_playability(video_id: str) -> bool:
    global UTA_LAST_PLAYABILITY_CHECK_STATUS
    if not video_id or not STREAMLINK_LIB_AVAILABLE or not UTA_YOUTUBE_PLAYABILITY_CHECK_ENABLED:
        if UTA_YOUTUBE_PLAYABILITY_CHECK_ENABLED and not STREAMLINK_LIB_AVAILABLE:
            logger.warning("UTA YouTube Health Check: Streamlink library not available, skipping playability check.")
        UTA_LAST_PLAYABILITY_CHECK_STATUS = "Skipped"
        logger.info(f"UTA_GUI_LOG: PlayabilityCheckStatus={UTA_LAST_PLAYABILITY_CHECK_STATUS}")
        return True 

    youtube_watch_url = f"https://www.youtube.com/watch?v={video_id}"
    logger.info(f"UTA YouTube Health Check: Verifying playability of {youtube_watch_url}...")
    UTA_LAST_PLAYABILITY_CHECK_STATUS = f"Pending for {video_id}"
    logger.info(f"UTA_GUI_LOG: PlayabilityCheckStatus={UTA_LAST_PLAYABILITY_CHECK_STATUS}")

    for attempt in range(UTA_YOUTUBE_PLAYABILITY_CHECK_RETRIES):
        if shutdown_event.is_set():
            UTA_LAST_PLAYABILITY_CHECK_STATUS = "Cancelled (Shutdown)"
            logger.info(f"UTA_GUI_LOG: PlayabilityCheckStatus={UTA_LAST_PLAYABILITY_CHECK_STATUS}")
            return False
        try:
            streams = await asyncio.to_thread(streamlink.streams, youtube_watch_url)
            if streams and ("best" in streams or "worst" in streams or "live" in streams or "audio_only" in streams or "audio" in streams):
                logger.info(f"UTA YouTube Health Check: Stream {video_id} confirmed playable via streamlink (Attempt {attempt+1}).")
                UTA_LAST_PLAYABILITY_CHECK_STATUS = f"Passed for {video_id}"
                logger.info(f"UTA_GUI_LOG: PlayabilityCheckStatus={UTA_LAST_PLAYABILITY_CHECK_STATUS}")
                return True
            else:
                logger.warning(f"UTA YouTube Health Check: No playable streams found for {video_id} via streamlink (Attempt {attempt + 1}/{UTA_YOUTUBE_PLAYABILITY_CHECK_RETRIES}). Streams: {streams.keys() if streams else 'None'}")
        except streamlink.exceptions.NoPluginError:
            logger.error(f"UTA YouTube Health Check: Streamlink has no plugin for YouTube (unexpected). Cannot verify {video_id}.")
            UTA_LAST_PLAYABILITY_CHECK_STATUS = f"Error (NoPlugin) for {video_id}"
            logger.info(f"UTA_GUI_LOG: PlayabilityCheckStatus={UTA_LAST_PLAYABILITY_CHECK_STATUS}")
            return False 
        except streamlink.exceptions.PluginError as e: 
            logger.warning(f"UTA YouTube Health Check: Streamlink PluginError for {video_id} (Attempt {attempt + 1}/{UTA_YOUTUBE_PLAYABILITY_CHECK_RETRIES}): {e}")
        except Exception as e:
            logger.error(f"UTA YouTube Health Check: Unexpected error verifying {video_id} (Attempt {attempt + 1}/{UTA_YOUTUBE_PLAYABILITY_CHECK_RETRIES}): {e}", exc_info=True)

        if attempt < UTA_YOUTUBE_PLAYABILITY_CHECK_RETRIES - 1:
            logger.info(f"UTA YouTube Health Check: Retrying playability check in {UTA_YOUTUBE_PLAYABILITY_CHECK_DELAY_SECONDS}s...")
            if shutdown_event.wait(timeout=UTA_YOUTUBE_PLAYABILITY_CHECK_DELAY_SECONDS):
                UTA_LAST_PLAYABILITY_CHECK_STATUS = "Cancelled (Shutdown during retry)"
                logger.info(f"UTA_GUI_LOG: PlayabilityCheckStatus={UTA_LAST_PLAYABILITY_CHECK_STATUS}")
                logger.info("UTA YouTube Health Check: Shutdown during retry sleep.")
                return False
    
    logger.error(f"UTA YouTube Health Check: Failed to confirm playability of {video_id} after {UTA_YOUTUBE_PLAYABILITY_CHECK_RETRIES} attempts.")
    UTA_LAST_PLAYABILITY_CHECK_STATUS = f"Failed for {video_id}"
    logger.info(f"UTA_GUI_LOG: PlayabilityCheckStatus={UTA_LAST_PLAYABILITY_CHECK_STATUS}")
    return False

def uta_start_restream(username, youtube_rtmp_url, youtube_stream_key):
    global uta_streamlink_process, uta_ffmpeg_process, uta_is_restreaming_active, \
           UTA_FFMPEG_PID, UTA_STREAMLINK_PID, UTA_PIPE_START_TIME_UTC, UTA_LAST_PLAYABILITY_CHECK_STATUS
           
    UTA_FFMPEG_PID, UTA_STREAMLINK_PID = None, None 
    UTA_PIPE_START_TIME_UTC = datetime.now(timezone.utc)
    logger.info(f"UTA_GUI_LOG: RestreamPipeStatus=Starting")

    if not youtube_stream_key or "YOUR_YOUTUBE_STREAM_KEY" in youtube_stream_key or not youtube_rtmp_url:
        logger.error(f"UTA: YouTube RTMP URL ('{youtube_rtmp_url}') or Stream Key ('{youtube_stream_key}') N/A. No restream.")
        return False

    full_yt_url = f"{youtube_rtmp_url.rstrip('/')}/{youtube_stream_key}"
    logger.info(f"UTA: Attempt restream for {username} to {youtube_rtmp_url.rstrip('/')}/<KEY>")

    sl_cmd = [UTA_STREAMLINK_PATH, "--stdout", f"twitch.tv/{username}", "best", "--twitch-disable-hosting", "--hls-live-restart", "--retry-streams", "5", "--retry-open", "3"]
    ff_cmd = [UTA_FFMPEG_PATH, "-hide_banner", "-i", "pipe:0", "-c:v", "copy", "-c:a", "aac", "-b:a", "160k", "-map", "0:v:0?", "-map", "0:a:0?", "-f", "flv", "-bufsize", "4000k", "-flvflags", "no_duration_filesize", "-loglevel", "warning", full_yt_url]
    cur_slp, cur_ffp = None, None
    stream_pipe_ok = False 

    try:
        logger.info("UTA: Starting Streamlink...");
        startupinfo = None
        if os.name == 'nt': 
            startupinfo = subprocess.STARTUPINFO()
            startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
            startupinfo.wShowWindow = subprocess.SW_HIDE
        cur_slp = subprocess.Popen(sl_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, startupinfo=startupinfo)
        UTA_STREAMLINK_PID = cur_slp.pid
        uta_streamlink_process = cur_slp
        logger.info(f"UTA: Streamlink PID: {UTA_STREAMLINK_PID}")

        logger.info("UTA: Starting FFmpeg...");
        cur_ffp = subprocess.Popen(ff_cmd, stdin=cur_slp.stdout, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, startupinfo=startupinfo)
        UTA_FFMPEG_PID = cur_ffp.pid
        uta_ffmpeg_process = cur_ffp
        logger.info(f"UTA: FFmpeg PID: {UTA_FFMPEG_PID}")
        
        if cur_slp.stdout: cur_slp.stdout.close()
        uta_is_restreaming_active = True # Mark active as soon as processes are launched
        logger.info(f"UTA: Restreaming {username}. Monitoring FFmpeg/Streamlink processes...")
        logger.info(f"UTA_GUI_LOG: RestreamPipeStatus=Active_Connecting") # Initial active state

        if UTA_YOUTUBE_PLAYABILITY_CHECK_ENABLED and effective_youtube_api_enabled() and uta_current_youtube_video_id:
            if shutdown_event.wait(timeout=UTA_FFMPEG_STARTUP_WAIT_SECONDS): # Wait for FFmpeg to potentially connect
                logger.info("UTA: Shutdown during FFmpeg startup wait for playability check.")
                return False 
            
            playable_future = asyncio.run_coroutine_threadsafe(
                _uta_check_youtube_playability(uta_current_youtube_video_id), bot.loop
            )
            try:
                # Generous timeout for the check (includes retries and delays within the async function)
                is_playable = playable_future.result(timeout=(UTA_YOUTUBE_PLAYABILITY_CHECK_RETRIES * (UTA_YOUTUBE_PLAYABILITY_CHECK_DELAY_SECONDS + 5)) + 10) 
                if not is_playable:
                    logger.error(f"UTA: YouTube stream {uta_current_youtube_video_id} not playable after FFmpeg start. Terminating current pipe attempt.")
                    return False 
            except asyncio.TimeoutError:
                logger.error(f"UTA: YouTube playability check for {uta_current_youtube_video_id} timed out overall.")
                UTA_LAST_PLAYABILITY_CHECK_STATUS = f"Timeout for {uta_current_youtube_video_id}"
                logger.info(f"UTA_GUI_LOG: PlayabilityCheckStatus={UTA_LAST_PLAYABILITY_CHECK_STATUS}")
                return False
            except Exception as e_play:
                logger.error(f"UTA: Error during YouTube playability check orchestration: {e_play}", exc_info=True)
                UTA_LAST_PLAYABILITY_CHECK_STATUS = f"Error for {uta_current_youtube_video_id}"
                logger.info(f"UTA_GUI_LOG: PlayabilityCheckStatus={UTA_LAST_PLAYABILITY_CHECK_STATUS}")
                return False
        
        stream_pipe_ok = True # If we get here, playability check (if done) passed or was skipped.
        logger.info(f"UTA_GUI_LOG: RestreamPipeStatus=Active_Streaming") # Pipe is up
        ff_stderr_output = ""
        sl_stderr_output = b"" 

        if cur_ffp.stderr:
            for line_bytes in iter(cur_ffp.stderr.readline, b''):
                if shutdown_event.is_set(): logger.info("UTA: Shutdown signal, stopping FFmpeg log reading."); break
                decoded_line = line_bytes.decode('utf-8', errors='ignore').strip(); logger.debug(f"UTA_FFMPEG_LOG: {decoded_line}"); ff_stderr_output += decoded_line + "\n"
                if cur_slp and cur_slp.poll() is not None: 
                    logger.warning(f"UTA: Streamlink (PID: {UTA_STREAMLINK_PID}) ended (Code: {cur_slp.returncode}) during FFmpeg."); stream_pipe_ok = False; break 
                if cur_ffp.poll() is not None: break 
            if not cur_ffp.stderr.closed: cur_ffp.stderr.close()
        
        cur_ffp.wait(); ff_exit_code = cur_ffp.poll()
        logger.info(f"UTA: FFmpeg (PID: {UTA_FFMPEG_PID if UTA_FFMPEG_PID else 'N/A'}) exited with code: {ff_exit_code}")
        if ff_exit_code != 0 and ff_exit_code is not None:
            stream_pipe_ok = False
            logger.error("UTA: --- FFmpeg Error Log (Last 20 lines) ---")
            for err_line in ff_stderr_output.strip().splitlines()[-20:]:
                if err_line.strip(): logger.error(err_line)
            logger.error("UTA: --- End FFmpeg Error Log ---")

        if cur_slp:
            sl_exit_code = cur_slp.poll()
            if sl_exit_code is None: 
                logger.warning("UTA: FFmpeg exited, Streamlink running. Terminating Streamlink..."); uta_terminate_process(cur_slp, "Streamlink (post-ffmpeg cleanup)"); sl_exit_code = cur_slp.poll()
            logger.info(f"UTA: Streamlink (PID: {UTA_STREAMLINK_PID if UTA_STREAMLINK_PID else 'N/A'}) exited with code: {sl_exit_code}")
            if sl_exit_code != 0 and sl_exit_code is not None : stream_pipe_ok = False
            if cur_slp.stderr:
                try: sl_stderr_output = cur_slp.stderr.read()
                finally:
                    if not cur_slp.stderr.closed: cur_slp.stderr.close()
                if sl_stderr_output: logger.info(f"UTA: --- Streamlink Stderr Log ---\n{sl_stderr_output.decode('utf-8', errors='ignore').strip()}\n--- End Streamlink Stderr Log ---")
        
        return stream_pipe_ok

    except FileNotFoundError as e: logger.critical(f"UTA: ERROR: Command not found (Streamlink/FFmpeg): {e}."); return False
    except Exception as e: logger.error(f"UTA: Critical error during restreaming setup/monitoring: {e}", exc_info=True); return False
    finally:
        if not shutdown_event.is_set():
            temp_slp_to_clear, temp_ffp_to_clear = uta_streamlink_process, uta_ffmpeg_process
            if cur_slp == temp_slp_to_clear: uta_streamlink_process = None
            if cur_ffp == temp_ffp_to_clear: uta_ffmpeg_process = None
            uta_terminate_process(cur_ffp, "FFmpeg (start_restream finally)"); uta_terminate_process(cur_slp, "Streamlink (start_restream finally)");
        # PIDs are reset at the start of the next call or when session truly ends
        # UTA_PIPE_START_TIME_UTC is also reset at the start of the next pipe attempt.
        uta_is_restreaming_active = False
        logger.info(f"UTA_GUI_LOG: RestreamPipeStatus=Inactive_EndedAttempt")

def effective_youtube_api_enabled():
    return UTA_YOUTUBE_API_ENABLED and GOOGLE_API_AVAILABLE

# Make sure these globals are defined at the module level in UTA.py
# UTA_CURRENT_YT_VIDEO_ID_FOR_LOGGING = None # Already should be there
# twitch_session_active_global = False # Already should be there
# youtube_api_session_active_global = False # Already should be there

def uta_restreamer_monitor_loop():
    global uta_is_restreaming_active, uta_yt_service, \
           uta_current_youtube_broadcast_id, uta_current_youtube_video_id, \
           uta_current_youtube_live_stream_id, uta_current_youtube_rtmp_url, \
           uta_current_youtube_stream_key, uta_current_restream_part_number, \
           uta_youtube_next_rollover_time_utc, UTA_RESTREAM_CONSECUTIVE_FAILURES, \
           UTA_CURRENT_YT_VIDEO_ID_FOR_LOGGING, \
           twitch_session_active_global, youtube_api_session_active_global, \
           UTA_MANUAL_FFMPEG_RESTART_REQUESTED, UTA_MANUAL_NEW_PART_REQUESTED, \
           UTA_FFMPEG_PID, UTA_STREAMLINK_PID, UTA_PIPE_START_TIME_UTC, UTA_LAST_PLAYABILITY_CHECK_STATUS


    logger.info(f"UTA: Restreamer Monitor thread ({threading.current_thread().name}) started.")
    _twitch_session_active_local = False
    _youtube_api_session_active_local = False
    twitch_session_start_time_utc = None
    twitch_session_stream_data = None

    if effective_youtube_api_enabled():
        if not _uta_youtube_get_service():
             logger.warning("UTA YouTube: Failed to initialize YouTube service on thread start. Will retry.")

    while not shutdown_event.is_set():
        twitch_session_active_global = _twitch_session_active_local
        youtube_api_session_active_global = _youtube_api_session_active_local

        try:
            if not UTA_TWITCH_CHANNEL_NAME:
                logger.warning("UTA Restream: UTA_TWITCH_CHANNEL_NAME not set. Skipping cycle.")
                if shutdown_event.wait(timeout=UTA_CHECK_INTERVAL_SECONDS_RESTREAMER): break
                continue

            is_twitch_live, current_twitch_stream_data = uta_is_streamer_live(UTA_TWITCH_CHANNEL_NAME)
            now_utc = datetime.now(timezone.utc)

            manual_ffmpeg_restart_triggered = False
            if UTA_MANUAL_FFMPEG_RESTART_REQUESTED and _twitch_session_active_local and is_twitch_live:
                logger.info("UTA Restream: Manual FFmpeg/Streamlink restart triggered by command.")
                if uta_is_restreaming_active: uta_cleanup_restream_processes()
                UTA_MANUAL_FFMPEG_RESTART_REQUESTED = False; manual_ffmpeg_restart_triggered = True 
                UTA_RESTREAM_CONSECUTIVE_FAILURES = 0 # Reset failures on manual ffmpeg restart
                logger.info(f"UTA_GUI_LOG: ConsecutiveFailures={UTA_RESTREAM_CONSECUTIVE_FAILURES}")


            manual_new_part_triggered_for_loop = False
            if UTA_MANUAL_NEW_PART_REQUESTED and _twitch_session_active_local and is_twitch_live and \
               effective_youtube_api_enabled() and _youtube_api_session_active_local:
                logger.info("UTA Restream: Manual new YouTube part triggered by command.")
                UTA_MANUAL_NEW_PART_REQUESTED = False; manual_new_part_triggered_for_loop = True

            if is_twitch_live and not _twitch_session_active_local:
                logger.info(f"UTA Restream: {UTA_TWITCH_CHANNEL_NAME} is LIVE! Preparing restream...")
                _twitch_session_active_local = True; twitch_session_start_time_utc = now_utc; twitch_session_stream_data = current_twitch_stream_data
                uta_current_restream_part_number = 1; _youtube_api_session_active_local = False; UTA_RESTREAM_CONSECUTIVE_FAILURES = 0; UTA_CURRENT_YT_VIDEO_ID_FOR_LOGGING = None
                UTA_LAST_PLAYABILITY_CHECK_STATUS = "N/A"; logger.info(f"UTA_GUI_LOG: PlayabilityCheckStatus={UTA_LAST_PLAYABILITY_CHECK_STATUS}")
                UTA_FFMPEG_PID, UTA_STREAMLINK_PID, UTA_PIPE_START_TIME_UTC = None, None, None; logger.info(f"UTA_GUI_LOG: RestreamPipeStatus=Inactive")
                logger.info(f"UTA_GUI_LOG: ConsecutiveFailures=0"); logger.info(f"UTA_GUI_LOG: CooldownStatus=Inactive")

                if effective_youtube_api_enabled():
                    if not uta_yt_service: _uta_youtube_get_service()
                    if uta_yt_service:
                        # ... (Full YouTube API broadcast creation logic as in previous versions) ...
                        new_ls_id,new_rtmp_url,new_s_key = asyncio.run_coroutine_threadsafe(_uta_youtube_create_live_stream_resource(uta_yt_service, UTA_TWITCH_CHANNEL_NAME), bot.loop).result(timeout=30)
                        if new_ls_id and new_rtmp_url and new_s_key:
                            uta_current_youtube_live_stream_id, uta_current_youtube_rtmp_url, uta_current_youtube_stream_key = new_ls_id, new_rtmp_url, new_s_key
                            title = UTA_YOUTUBE_DYNAMIC_TITLE_TEMPLATE.format(twitch_username=UTA_TWITCH_CHANNEL_NAME, twitch_title=twitch_session_stream_data.get("title","N/A"), game_name=twitch_session_stream_data.get("game_name","N/A"), part_num=uta_current_restream_part_number, date=now_utc.strftime("%Y-%m-%d"), time=now_utc.strftime("%H:%M:%S"))
                            description = UTA_YOUTUBE_DYNAMIC_DESCRIPTION_TEMPLATE.format(twitch_username=UTA_TWITCH_CHANNEL_NAME, twitch_title=twitch_session_stream_data.get("title","N/A"), game_name=twitch_session_stream_data.get("game_name","N/A"))
                            new_bcast_id = asyncio.run_coroutine_threadsafe(_uta_youtube_create_broadcast(uta_yt_service, uta_current_youtube_live_stream_id, title, description, UTA_YOUTUBE_DEFAULT_PRIVACY, now_utc.isoformat()), bot.loop).result(timeout=30)
                            if new_bcast_id:
                                uta_current_youtube_broadcast_id, uta_current_youtube_video_id = new_bcast_id, new_bcast_id; UTA_CURRENT_YT_VIDEO_ID_FOR_LOGGING = uta_current_youtube_video_id; _youtube_api_session_active_local = True
                                if UTA_YOUTUBE_PLAYLIST_ID: asyncio.run_coroutine_threadsafe(_uta_youtube_add_video_to_playlist(uta_yt_service, uta_current_youtube_video_id, UTA_YOUTUBE_PLAYLIST_ID), bot.loop)
                                if UTA_YOUTUBE_SCHEDULED_ROLLOVER_HOURS > 0: uta_youtube_next_rollover_time_utc = now_utc + timedelta(hours=UTA_YOUTUBE_SCHEDULED_ROLLOVER_HOURS)
                                else: uta_youtube_next_rollover_time_utc = None
                                logger.info(f"UTA YouTube: Successfully created broadcast {uta_current_youtube_broadcast_id} (Video ID: {uta_current_youtube_video_id}) for Part {uta_current_restream_part_number}. Watch: https://www.youtube.com/watch?v={uta_current_youtube_video_id}")
                            else: logger.error("UTA YouTube: Failed to create broadcast. Legacy RTMP if configured."); _youtube_api_session_active_local = False
                        else: logger.error("UTA YouTube: Failed to create liveStream resource. Legacy RTMP if configured."); _youtube_api_session_active_local = False
                    else: logger.error("UTA YouTube: Service not available. Legacy RTMP if configured."); _youtube_api_session_active_local = False
                uta_send_discord_restream_status("start", UTA_TWITCH_CHANNEL_NAME, twitch_session_stream_data)

            if _twitch_session_active_local and is_twitch_live:
                time_to_rollover = manual_new_part_triggered_for_loop
                if not time_to_rollover and effective_youtube_api_enabled() and _youtube_api_session_active_local and \
                   uta_youtube_next_rollover_time_utc and now_utc >= uta_youtube_next_rollover_time_utc:
                    logger.info(f"UTA YouTube: Scheduled rollover time for broadcast {uta_current_youtube_broadcast_id}."); time_to_rollover = True
                
                if time_to_rollover:
                    # ... (Full rollover logic as in previous versions, including updating UTA_CURRENT_YT_VIDEO_ID_FOR_LOGGING) ...
                    logger.info(f"UTA YouTube: Initiating stream rollover from Part {uta_current_restream_part_number}.")
                    if uta_is_restreaming_active: uta_cleanup_restream_processes()
                    asyncio.run_coroutine_threadsafe(_uta_youtube_transition_broadcast(uta_yt_service, uta_current_youtube_broadcast_id, "complete"), bot.loop).result(timeout=30)
                    if UTA_YOUTUBE_MAKE_PUBLIC_AFTER_STREAM and UTA_YOUTUBE_DEFAULT_PRIVACY != "public": asyncio.run_coroutine_threadsafe(_uta_youtube_set_video_privacy(uta_yt_service, uta_current_youtube_video_id, "public"), bot.loop)
                    UTA_CURRENT_YT_VIDEO_ID_FOR_LOGGING = None 
                    uta_current_restream_part_number += 1; UTA_RESTREAM_CONSECUTIVE_FAILURES = 0; logger.info(f"UTA_GUI_LOG: ConsecutiveFailures=0")
                    logger.info(f"UTA YouTube: Preparing for Part {uta_current_restream_part_number}.")
                    new_ls_id_r,new_rtmp_url_r,new_s_key_r = asyncio.run_coroutine_threadsafe(_uta_youtube_create_live_stream_resource(uta_yt_service, UTA_TWITCH_CHANNEL_NAME), bot.loop).result(timeout=30)
                    if new_ls_id_r and new_rtmp_url_r and new_s_key_r:
                        uta_current_youtube_live_stream_id,uta_current_youtube_rtmp_url,uta_current_youtube_stream_key = new_ls_id_r,new_rtmp_url_r,new_s_key_r
                        title_r = UTA_YOUTUBE_DYNAMIC_TITLE_TEMPLATE.format(twitch_username=UTA_TWITCH_CHANNEL_NAME, twitch_title=current_twitch_stream_data.get("title","N/A"), game_name=current_twitch_stream_data.get("game_name","N/A"), part_num=uta_current_restream_part_number, date=now_utc.strftime("%Y-%m-%d"), time=now_utc.strftime("%H:%M:%S"))
                        desc_r = UTA_YOUTUBE_DYNAMIC_DESCRIPTION_TEMPLATE.format(twitch_username=UTA_TWITCH_CHANNEL_NAME, twitch_title=current_twitch_stream_data.get("title","N/A"), game_name=current_twitch_stream_data.get("game_name","N/A"))
                        new_bcast_id_r = asyncio.run_coroutine_threadsafe(_uta_youtube_create_broadcast(uta_yt_service, uta_current_youtube_live_stream_id, title_r, desc_r, UTA_YOUTUBE_DEFAULT_PRIVACY, now_utc.isoformat()), bot.loop).result(timeout=30)
                        if new_bcast_id_r:
                            uta_current_youtube_broadcast_id, uta_current_youtube_video_id = new_bcast_id_r, new_bcast_id_r
                            UTA_CURRENT_YT_VIDEO_ID_FOR_LOGGING = uta_current_youtube_video_id 
                            _youtube_api_session_active_local = True
                            if UTA_YOUTUBE_PLAYLIST_ID: asyncio.run_coroutine_threadsafe(_uta_youtube_add_video_to_playlist(uta_yt_service, uta_current_youtube_video_id, UTA_YOUTUBE_PLAYLIST_ID), bot.loop)
                            if UTA_YOUTUBE_SCHEDULED_ROLLOVER_HOURS > 0: uta_youtube_next_rollover_time_utc = now_utc + timedelta(hours=UTA_YOUTUBE_SCHEDULED_ROLLOVER_HOURS)
                            logger.info(f"UTA YouTube: Rollover successful. New broadcast {uta_current_youtube_broadcast_id} (Video ID: {uta_current_youtube_video_id}) for Part {uta_current_restream_part_number}. Watch: https://www.youtube.com/watch?v={uta_current_youtube_video_id}")
                        else: logger.error("UTA YouTube: Rollover failed to create new broadcast. Aborting."); _twitch_session_active_local=False; _youtube_api_session_active_local=False
                    else: logger.error("UTA YouTube: Rollover failed to create new liveStream. Aborting."); _twitch_session_active_local=False; _youtube_api_session_active_local=False

                if not uta_is_restreaming_active and ((effective_youtube_api_enabled() and _youtube_api_session_active_local and uta_current_youtube_rtmp_url and uta_current_youtube_stream_key) or (not effective_youtube_api_enabled() and UTA_YOUTUBE_RTMP_URL_BASE and UTA_YOUTUBE_STREAM_KEY)):
                    if not manual_ffmpeg_restart_triggered and UTA_RESTREAM_CONSECUTIVE_FAILURES >= UTA_RESTREAM_MAX_CONSECUTIVE_FAILURES :
                        logger.critical(f"UTA Restream: Max consecutive failures ({UTA_RESTREAM_CONSECUTIVE_FAILURES}). Long cooldown: {UTA_RESTREAM_LONG_COOLDOWN_SECONDS}s."); uta_send_discord_restream_status("stop",UTA_TWITCH_CHANNEL_NAME, stream_duration_seconds=0); logger.info(f"UTA_GUI_LOG: ConsecutiveFailures={UTA_RESTREAM_CONSECUTIVE_FAILURES}"); logger.info(f"UTA_GUI_LOG: CooldownStatus=LongCooldownActive_{UTA_RESTREAM_LONG_COOLDOWN_SECONDS}s")
                        if shutdown_event.wait(timeout=UTA_RESTREAM_LONG_COOLDOWN_SECONDS): break
                        logger.info(f"UTA_GUI_LOG: CooldownStatus=Inactive"); UTA_RESTREAM_CONSECUTIVE_FAILURES=0; logger.info(f"UTA_GUI_LOG: ConsecutiveFailures=0"); continue
                    
                    rtmp_to_use = uta_current_youtube_rtmp_url if effective_youtube_api_enabled() and _youtube_api_session_active_local else UTA_YOUTUBE_RTMP_URL_BASE
                    key_to_use = uta_current_youtube_stream_key if effective_youtube_api_enabled() and _youtube_api_session_active_local else UTA_YOUTUBE_STREAM_KEY
                    if not (shutil.which(UTA_STREAMLINK_PATH) and shutil.which(UTA_FFMPEG_PATH)):
                        logger.error("UTA Restream: Streamlink/FFmpeg path invalid. Aborting."); _twitch_session_active_local=False
                        if effective_youtube_api_enabled() and _youtube_api_session_active_local: asyncio.run_coroutine_threadsafe(_uta_youtube_transition_broadcast(uta_yt_service, uta_current_youtube_broadcast_id, "complete"), bot.loop); _youtube_api_session_active_local=False
                        continue
                    logger.info(f"UTA Restream: Starting FFmpeg for {UTA_TWITCH_CHANNEL_NAME} (Part {uta_current_restream_part_number if effective_youtube_api_enabled() and _youtube_api_session_active_local else 'N/A'}). Attempt {UTA_RESTREAM_CONSECUTIVE_FAILURES + 1}.")
                    restream_pipe_ok = uta_start_restream(UTA_TWITCH_CHANNEL_NAME, rtmp_to_use, key_to_use) # This logs pipe status for GUI
                    logger.info(f"UTA Restream: FFmpeg/Streamlink pipe for Part {uta_current_restream_part_number if effective_youtube_api_enabled() and _youtube_api_session_active_local else 'N/A'} ended.")
                    if restream_pipe_ok: UTA_RESTREAM_CONSECUTIVE_FAILURES = 0; logger.info(f"UTA_GUI_LOG: ConsecutiveFailures=0")
                    else: UTA_RESTREAM_CONSECUTIVE_FAILURES += 1; logger.error(f"UTA Restream: Pipe failed. Consecutive failures: {UTA_RESTREAM_CONSECUTIVE_FAILURES}."); logger.info(f"UTA_GUI_LOG: ConsecutiveFailures={UTA_RESTREAM_CONSECUTIVE_FAILURES}"); logger.info(f"UTA_GUI_LOG: RestreamPipeStatus=FailedRetry")
                    if not manual_ffmpeg_restart_triggered and UTA_RESTREAM_CONSECUTIVE_FAILURES < UTA_RESTREAM_MAX_CONSECUTIVE_FAILURES: 
                            logger.info(f"UTA_GUI_LOG: CooldownStatus=ShortRetryCooldown_{UTA_POST_RESTREAM_COOLDOWN_SECONDS}s")
                            if shutdown_event.wait(timeout=UTA_POST_RESTREAM_COOLDOWN_SECONDS): break
                            logger.info(f"UTA_GUI_LOG: CooldownStatus=Inactive")
                
                elif uta_is_restreaming_active:
                    if not manual_ffmpeg_restart_triggered:
                        logger.info(f"UTA Restream: {UTA_TWITCH_CHANNEL_NAME} live. Restream active. Check in {UTA_RESTREAM_CHECK_INTERVAL_WHEN_LIVE}s.");
                        logger.info(f"UTA_GUI_LOG: RestreamPipeStatus=Active_Monitoring")
                        if shutdown_event.wait(timeout=UTA_RESTREAM_CHECK_INTERVAL_WHEN_LIVE): break
                else:
                    logger.warning(f"UTA Restream: {UTA_TWITCH_CHANNEL_NAME} live, but not currently restreaming. Check in {UTA_CHECK_INTERVAL_SECONDS_RESTREAMER}s.");
                    logger.info(f"UTA_GUI_LOG: RestreamPipeStatus=Inactive_Waiting")
                    if shutdown_event.wait(timeout=UTA_CHECK_INTERVAL_SECONDS_RESTREAMER): break
            
            elif not is_twitch_live and _twitch_session_active_local:
                logger.info(f"UTA Restream: {UTA_TWITCH_CHANNEL_NAME} OFFLINE. Ending session.")
                if uta_is_restreaming_active: uta_cleanup_restream_processes()
                if effective_youtube_api_enabled() and _youtube_api_session_active_local:
                    logger.info(f"UTA YouTube: Finalizing YT broadcast {uta_current_youtube_broadcast_id}.")
                    asyncio.run_coroutine_threadsafe(_uta_youtube_transition_broadcast(uta_yt_service, uta_current_youtube_broadcast_id, "complete"), bot.loop).result(timeout=30)
                    if UTA_YOUTUBE_MAKE_PUBLIC_AFTER_STREAM and UTA_YOUTUBE_DEFAULT_PRIVACY!="public": asyncio.run_coroutine_threadsafe(_uta_youtube_set_video_privacy(uta_yt_service, uta_current_youtube_video_id, "public"), bot.loop)
                _youtube_api_session_active_local=False; uta_current_youtube_broadcast_id=None; uta_current_youtube_video_id=None; UTA_CURRENT_YT_VIDEO_ID_FOR_LOGGING = None
                uta_current_youtube_rtmp_url=None; uta_current_youtube_stream_key=None; uta_youtube_next_rollover_time_utc=None
                overall_duration_sec = (now_utc - twitch_session_start_time_utc).total_seconds() if twitch_session_start_time_utc else 0
                if overall_duration_sec > 15 and twitch_session_start_time_utc and bot.loop: asyncio.run_coroutine_threadsafe(log_stream_duration_binary(int(twitch_session_start_time_utc.timestamp()), int(now_utc.timestamp())), bot.loop)
                uta_send_discord_restream_status("stop", UTA_TWITCH_CHANNEL_NAME, stream_duration_seconds=overall_duration_sec)
                _twitch_session_active_local=False; twitch_session_start_time_utc=None; twitch_session_stream_data=None; UTA_RESTREAM_CONSECUTIVE_FAILURES=0
                logger.info(f"UTA_GUI_LOG: ConsecutiveFailures=0"); logger.info(f"UTA_GUI_LOG: CooldownStatus=Inactive"); logger.info(f"UTA_GUI_LOG: PlayabilityCheckStatus=N/A"); logger.info(f"UTA_GUI_LOG: RestreamPipeStatus=Inactive_SessionEnded")
                logger.info(f"UTA Restream: Cooldown for {UTA_POST_RESTREAM_COOLDOWN_SECONDS}s."); shutdown_event.wait(UTA_POST_RESTREAM_COOLDOWN_SECONDS)

            elif not is_twitch_live and not _twitch_session_active_local:
                UTA_RESTREAM_CONSECUTIVE_FAILURES=0; UTA_CURRENT_YT_VIDEO_ID_FOR_LOGGING = None; _youtube_api_session_active_local = False
                logger.info(f"UTA_GUI_LOG: ConsecutiveFailures=0"); logger.info(f"UTA_GUI_LOG: CooldownStatus=Inactive"); logger.info(f"UTA_GUI_LOG: PlayabilityCheckStatus=N/A"); logger.info(f"UTA_GUI_LOG: RestreamPipeStatus=Inactive_Offline")
                logger.info(f"UTA Restream: {UTA_TWITCH_CHANNEL_NAME} offline. Waiting {UTA_CHECK_INTERVAL_SECONDS_RESTREAMER}s..."); shutdown_event.wait(UTA_CHECK_INTERVAL_SECONDS_RESTREAMER)
            
            if shutdown_event.is_set(): break
        except Exception as e:
            logger.error(f"UTA Restreamer Monitor: Unexpected error: {e}", exc_info=True); uta_cleanup_restream_processes()
            if effective_youtube_api_enabled() and _youtube_api_session_active_local and uta_current_youtube_broadcast_id:
                logger.error(f"UTA YouTube: Finalizing YT broadcast {uta_current_youtube_broadcast_id} due to error.")
                try: asyncio.run_coroutine_threadsafe(_uta_youtube_transition_broadcast(uta_yt_service, uta_current_youtube_broadcast_id, "complete"), bot.loop).result(timeout=30)
                except Exception as yt_err: logger.error(f"UTA YouTube: Failed to finalize broadcast during error handling: {yt_err}")
            _twitch_session_active_local=False; twitch_session_start_time_utc=None; twitch_session_stream_data=None; _youtube_api_session_active_local=False; uta_current_youtube_broadcast_id=None; uta_current_youtube_video_id=None; UTA_CURRENT_YT_VIDEO_ID_FOR_LOGGING = None
            uta_current_youtube_rtmp_url=None; uta_current_youtube_stream_key=None; uta_youtube_next_rollover_time_utc=None; UTA_RESTREAM_CONSECUTIVE_FAILURES=0
            logger.info(f"UTA_GUI_LOG: ConsecutiveFailures=0"); logger.info(f"UTA_GUI_LOG: CooldownStatus=Inactive"); logger.info(f"UTA_GUI_LOG: PlayabilityCheckStatus=N/A"); logger.info(f"UTA_GUI_LOG: RestreamPipeStatus=ErrorState")
            uta_send_discord_restream_status("stop", UTA_TWITCH_CHANNEL_NAME, stream_duration_seconds=0)
            if shutdown_event.wait(timeout=60): break
            
    twitch_session_active_global = _twitch_session_active_local
    youtube_api_session_active_global = _youtube_api_session_active_local
    logger.info(f"UTA_GUI_LOG: RestreamPipeStatus=Stopped") # Final status on thread exit
    logger.info(f"UTA: Restreamer Monitor thread ({threading.current_thread().name}) finished.")

def uta_clip_monitor_loop():
    logger.info(f"UTA: Clip Monitor thread ({threading.current_thread().name}) started.")
    b_id = uta_get_broadcaster_id(UTA_TWITCH_CHANNEL_NAME)
    if not b_id and UTA_TWITCH_CHANNEL_NAME: logger.error(f"UTA Clip: No BID for {UTA_TWITCH_CHANNEL_NAME}. Will retry.")
    if b_id:
        logger.info(f"UTA Clip: Initial scan for {UTA_CLIP_LOOKBACK_MINUTES} min...")
        for c in uta_get_recent_clips(b_id, UTA_CLIP_LOOKBACK_MINUTES): uta_sent_clip_ids.add(c['id'])
        logger.info(f"UTA Clip: Primed {len(uta_sent_clip_ids)} clips.")
    while not shutdown_event.is_set():
        try:
            if not UTA_TWITCH_CHANNEL_NAME:
                logger.warning("UTA Clip: UTA_TWITCH_CHANNEL_NAME not set. Skipping clip check cycle.")
                if shutdown_event.wait(timeout=UTA_CHECK_INTERVAL_SECONDS_CLIPS): break; continue
            cur_bid = uta_broadcaster_id_cache or uta_get_broadcaster_id(UTA_TWITCH_CHANNEL_NAME)
            if not cur_bid:
                logger.warning(f"UTA Clip: No BID for {UTA_TWITCH_CHANNEL_NAME}. Skip cycle.")
                if shutdown_event.wait(timeout=UTA_CHECK_INTERVAL_SECONDS_CLIPS): break; continue
            logger.info(f"UTA Clip: Checking clips for {UTA_TWITCH_CHANNEL_NAME}...")
            clips = uta_get_recent_clips(cur_bid, UTA_CLIP_LOOKBACK_MINUTES)
            if not clips: logger.info("UTA Clip: No clips found in lookback window.")
            else:
                new_found = 0
                for clip in reversed(clips):
                    if shutdown_event.is_set(): break
                    if clip['id'] not in uta_sent_clip_ids:
                        logger.info(f"UTA Clip: New clip found: {clip['title']} - {clip['url']}")
                        uta_send_discord_clip_notification(clip['url'], clip['title'], UTA_TWITCH_CHANNEL_NAME)
                        uta_sent_clip_ids.add(clip['id']); new_found += 1
                        if shutdown_event.wait(timeout=1): break
                if new_found == 0 and clips: logger.info("UTA Clip: No *new* clips found (all fetched clips already sent).")
            if shutdown_event.is_set(): break
            logger.info(f"UTA Clip: Waiting {UTA_CHECK_INTERVAL_SECONDS_CLIPS // 60} min ({UTA_CHECK_INTERVAL_SECONDS_CLIPS}s) for next check...")
            if shutdown_event.wait(timeout=UTA_CHECK_INTERVAL_SECONDS_CLIPS): break
        except Exception as e:
            logger.error(f"UTA Clip: An unexpected error occurred in {threading.current_thread().name}: {e}", exc_info=True)
            if shutdown_event.wait(timeout=60): break
    logger.info(f"UTA: Clip Monitor thread ({threading.current_thread().name}) finished.")

def uta_restreamer_monitor_loop():
    global uta_is_restreaming_active, uta_yt_service, \
           uta_current_youtube_broadcast_id, uta_current_youtube_video_id, \
           uta_current_youtube_live_stream_id, uta_current_youtube_rtmp_url, \
           uta_current_youtube_stream_key, uta_current_restream_part_number, \
           uta_youtube_next_rollover_time_utc

    logger.info(f"UTA: Restreamer Monitor thread ({threading.current_thread().name}) started.")

    twitch_session_active = False
    twitch_session_start_time_utc = None
    twitch_session_stream_data = None

    youtube_api_session_active = False

    if UTA_YOUTUBE_API_ENABLED and GOOGLE_API_AVAILABLE:
        if not _uta_youtube_get_service():
             logger.warning("UTA YouTube: Failed to initialize YouTube service on thread start. Will retry when stream goes live.")

    while not shutdown_event.is_set():
        try:
            effective_youtube_api_enabled = UTA_YOUTUBE_API_ENABLED and GOOGLE_API_AVAILABLE

            if not UTA_TWITCH_CHANNEL_NAME:
                logger.warning("UTA Restream: UTA_TWITCH_CHANNEL_NAME not set. Skipping restream check cycle.")
                if shutdown_event.wait(timeout=UTA_CHECK_INTERVAL_SECONDS_RESTREAMER): break
                continue

            is_twitch_live, current_twitch_stream_data = uta_is_streamer_live(UTA_TWITCH_CHANNEL_NAME)
            now_utc = datetime.now(timezone.utc)

            if is_twitch_live and not twitch_session_active:
                logger.info(f"UTA Restream: {UTA_TWITCH_CHANNEL_NAME} is LIVE! Preparing to start restream...")
                twitch_session_active = True
                twitch_session_start_time_utc = now_utc
                twitch_session_stream_data = current_twitch_stream_data
                uta_current_restream_part_number = 1
                youtube_api_session_active = False

                if effective_youtube_api_enabled:
                    if not uta_yt_service: _uta_youtube_get_service()
                    if uta_yt_service:
                        new_ls_id, new_rtmp_url, new_s_key = asyncio.run_coroutine_threadsafe(
                            _uta_youtube_create_live_stream_resource(uta_yt_service, UTA_TWITCH_CHANNEL_NAME), bot.loop
                        ).result(timeout=30)

                        if new_ls_id and new_rtmp_url and new_s_key:
                            uta_current_youtube_live_stream_id = new_ls_id
                            uta_current_youtube_rtmp_url = new_rtmp_url
                            uta_current_youtube_stream_key = new_s_key

                            title = UTA_YOUTUBE_DYNAMIC_TITLE_TEMPLATE.format(
                                twitch_username=UTA_TWITCH_CHANNEL_NAME,
                                twitch_title=twitch_session_stream_data.get("title", "N/A"),
                                game_name=twitch_session_stream_data.get("game_name", "N/A"),
                                part_num=uta_current_restream_part_number,
                                date=now_utc.strftime("%Y-%m-%d"),
                                time=now_utc.strftime("%H:%M:%S")
                            )
                            description = UTA_YOUTUBE_DYNAMIC_DESCRIPTION_TEMPLATE.format(
                                twitch_username=UTA_TWITCH_CHANNEL_NAME,
                                twitch_title=twitch_session_stream_data.get("title", "N/A"),
                                game_name=twitch_session_stream_data.get("game_name", "N/A")
                            )
                            start_iso = now_utc.isoformat()

                            new_bcast_id = asyncio.run_coroutine_threadsafe(
                                _uta_youtube_create_broadcast(uta_yt_service, uta_current_youtube_live_stream_id, title, description, UTA_YOUTUBE_DEFAULT_PRIVACY, start_iso),
                                bot.loop
                            ).result(timeout=30)

                            if new_bcast_id:
                                uta_current_youtube_broadcast_id = new_bcast_id
                                uta_current_youtube_video_id = new_bcast_id
                                youtube_api_session_active = True
                                if UTA_YOUTUBE_PLAYLIST_ID:
                                    asyncio.run_coroutine_threadsafe(
                                        _uta_youtube_add_video_to_playlist(uta_yt_service, uta_current_youtube_video_id, UTA_YOUTUBE_PLAYLIST_ID),
                                        bot.loop)

                                if UTA_YOUTUBE_SCHEDULED_ROLLOVER_HOURS > 0:
                                    uta_youtube_next_rollover_time_utc = now_utc + timedelta(hours=UTA_YOUTUBE_SCHEDULED_ROLLOVER_HOURS)
                                else:
                                    uta_youtube_next_rollover_time_utc = None
                                logger.info(f"UTA YouTube: Successfully created broadcast {uta_current_youtube_broadcast_id} for Part {uta_current_restream_part_number}.")
                            else:
                                logger.error("UTA YouTube: Failed to create broadcast. Falling back to standard RTMP if configured, or aborting.")
                                effective_youtube_api_enabled = False
                        else:
                            logger.error("UTA YouTube: Failed to create liveStream resource. Falling back to standard RTMP if configured, or aborting.")
                            effective_youtube_api_enabled = False
                    else:
                        logger.error("UTA YouTube: YouTube service not available. Falling back to standard RTMP if configured, or aborting.")
                        effective_youtube_api_enabled = False

                uta_send_discord_restream_status("start", UTA_TWITCH_CHANNEL_NAME, twitch_session_stream_data)

            if twitch_session_active and is_twitch_live:
                time_to_rollover = False
                if effective_youtube_api_enabled and youtube_api_session_active and uta_youtube_next_rollover_time_utc and now_utc >= uta_youtube_next_rollover_time_utc:
                    logger.info(f"UTA YouTube: Scheduled rollover time reached for broadcast {uta_current_youtube_broadcast_id}.")
                    time_to_rollover = True

                if time_to_rollover:
                    logger.info(f"UTA YouTube: Initiating stream rollover for Part {uta_current_restream_part_number}.")
                    if uta_is_restreaming_active:
                         uta_cleanup_restream_processes()

                    asyncio.run_coroutine_threadsafe(
                        _uta_youtube_transition_broadcast(uta_yt_service, uta_current_youtube_broadcast_id, "complete"), bot.loop
                    ).result(timeout=30)
                    if UTA_YOUTUBE_MAKE_PUBLIC_AFTER_STREAM and UTA_YOUTUBE_DEFAULT_PRIVACY != "public":
                         asyncio.run_coroutine_threadsafe(
                             _uta_youtube_set_video_privacy(uta_yt_service, uta_current_youtube_video_id, "public"), bot.loop)

                    uta_current_restream_part_number += 1
                    logger.info(f"UTA YouTube: Preparing for Part {uta_current_restream_part_number}.")

                    new_ls_id_roll, new_rtmp_url_roll, new_s_key_roll = asyncio.run_coroutine_threadsafe(
                        _uta_youtube_create_live_stream_resource(uta_yt_service, UTA_TWITCH_CHANNEL_NAME), bot.loop
                    ).result(timeout=30)

                    if new_ls_id_roll and new_rtmp_url_roll and new_s_key_roll:
                        uta_current_youtube_live_stream_id = new_ls_id_roll
                        uta_current_youtube_rtmp_url = new_rtmp_url_roll
                        uta_current_youtube_stream_key = new_s_key_roll

                        title = UTA_YOUTUBE_DYNAMIC_TITLE_TEMPLATE.format(
                            twitch_username=UTA_TWITCH_CHANNEL_NAME,
                            twitch_title=current_twitch_stream_data.get("title", "N/A"),
                            game_name=current_twitch_stream_data.get("game_name", "N/A"),
                            part_num=uta_current_restream_part_number,
                            date=now_utc.strftime("%Y-%m-%d"),
                            time=now_utc.strftime("%H:%M:%S")
                        )
                        description = UTA_YOUTUBE_DYNAMIC_DESCRIPTION_TEMPLATE.format(
                            twitch_username=UTA_TWITCH_CHANNEL_NAME,
                            twitch_title=current_twitch_stream_data.get("title", "N/A"),
                            game_name=current_twitch_stream_data.get("game_name", "N/A")
                        )
                        start_iso = now_utc.isoformat()

                        new_bcast_id_roll = asyncio.run_coroutine_threadsafe(
                            _uta_youtube_create_broadcast(uta_yt_service, uta_current_youtube_live_stream_id, title, description, UTA_YOUTUBE_DEFAULT_PRIVACY, start_iso),
                            bot.loop
                        ).result(timeout=30)

                        if new_bcast_id_roll:
                            uta_current_youtube_broadcast_id = new_bcast_id_roll
                            uta_current_youtube_video_id = new_bcast_id_roll
                            if UTA_YOUTUBE_PLAYLIST_ID:
                                asyncio.run_coroutine_threadsafe(
                                    _uta_youtube_add_video_to_playlist(uta_yt_service, uta_current_youtube_video_id, UTA_YOUTUBE_PLAYLIST_ID), bot.loop)

                            if UTA_YOUTUBE_SCHEDULED_ROLLOVER_HOURS > 0:
                                uta_youtube_next_rollover_time_utc = now_utc + timedelta(hours=UTA_YOUTUBE_SCHEDULED_ROLLOVER_HOURS)
                            logger.info(f"UTA YouTube: Rollover successful. New broadcast {uta_current_youtube_broadcast_id} for Part {uta_current_restream_part_number}.")
                        else:
                            logger.error("UTA YouTube: Rollover failed to create new broadcast. Aborting further restream for this Twitch session.")
                            twitch_session_active = False
                            youtube_api_session_active = False
                    else:
                        logger.error("UTA YouTube: Rollover failed to create new liveStream resource. Aborting further restream for this Twitch session.")
                        twitch_session_active = False
                        youtube_api_session_active = False

                if not uta_is_restreaming_active and \
                   ((effective_youtube_api_enabled and youtube_api_session_active and uta_current_youtube_rtmp_url and uta_current_youtube_stream_key) or \
                    (not effective_youtube_api_enabled and UTA_YOUTUBE_RTMP_URL_BASE and UTA_YOUTUBE_STREAM_KEY)):

                    rtmp_to_use = uta_current_youtube_rtmp_url if effective_youtube_api_enabled else UTA_YOUTUBE_RTMP_URL_BASE
                    key_to_use = uta_current_youtube_stream_key if effective_youtube_api_enabled else UTA_YOUTUBE_STREAM_KEY

                    if not (shutil.which(UTA_STREAMLINK_PATH) and shutil.which(UTA_FFMPEG_PATH)):
                        logger.error("UTA Restream: Streamlink or FFmpeg path invalid. Aborting restream.")
                        twitch_session_active = False
                        if youtube_api_session_active:
                             asyncio.run_coroutine_threadsafe(
                                _uta_youtube_transition_broadcast(uta_yt_service, uta_current_youtube_broadcast_id, "complete"), bot.loop)
                             youtube_api_session_active = False
                        continue

                    logger.info(f"UTA Restream: Starting FFmpeg for {UTA_TWITCH_CHANNEL_NAME} (Part {uta_current_restream_part_number if effective_youtube_api_enabled else 'N/A'}).")
                    restream_success = uta_start_restream(UTA_TWITCH_CHANNEL_NAME, rtmp_to_use, key_to_use)

                    logger.info(f"UTA Restream: FFmpeg process for {UTA_TWITCH_CHANNEL_NAME} (Part {uta_current_restream_part_number if effective_youtube_api_enabled else 'N/A'}) ended.")

                    if not restream_success:
                        logger.error(f"UTA Restream: uta_start_restream failed to initialize for Part {uta_current_restream_part_number if effective_youtube_api_enabled else 'N/A'}. Check logs.")

                elif uta_is_restreaming_active:
                    logger.info(f"UTA Restream: {UTA_TWITCH_CHANNEL_NAME} is still live. Current restream (Part {uta_current_restream_part_number if effective_youtube_api_enabled else 'N/A'}) active. Check in {UTA_RESTREAM_CHECK_INTERVAL_WHEN_LIVE}s.")
                    if shutdown_event.wait(timeout=UTA_RESTREAM_CHECK_INTERVAL_WHEN_LIVE): break

                else:
                    logger.warning(f"UTA Restream: {UTA_TWITCH_CHANNEL_NAME} is live, but not restreaming (possibly due to YouTube API setup issues or missing RTMP config). Check in {UTA_CHECK_INTERVAL_SECONDS_RESTREAMER}s.")
                    if shutdown_event.wait(timeout=UTA_CHECK_INTERVAL_SECONDS_RESTREAMER): break

            elif not is_twitch_live and twitch_session_active:
                logger.info(f"UTA Restream: {UTA_TWITCH_CHANNEL_NAME} appears OFFLINE. Ending restream session.")
                if uta_is_restreaming_active:
                    uta_cleanup_restream_processes()

                if effective_youtube_api_enabled and youtube_api_session_active:
                    logger.info(f"UTA YouTube: Finalizing YouTube broadcast {uta_current_youtube_broadcast_id}.")
                    asyncio.run_coroutine_threadsafe(
                        _uta_youtube_transition_broadcast(uta_yt_service, uta_current_youtube_broadcast_id, "complete"), bot.loop
                    ).result(timeout=30)
                    if UTA_YOUTUBE_MAKE_PUBLIC_AFTER_STREAM and UTA_YOUTUBE_DEFAULT_PRIVACY != "public":
                         asyncio.run_coroutine_threadsafe(
                             _uta_youtube_set_video_privacy(uta_yt_service, uta_current_youtube_video_id, "public"), bot.loop)
                    youtube_api_session_active = False
                    uta_current_youtube_broadcast_id = None
                    uta_current_youtube_video_id = None
                    uta_current_youtube_rtmp_url = None
                    uta_current_youtube_stream_key = None
                    uta_youtube_next_rollover_time_utc = None

                overall_duration_seconds = 0
                if twitch_session_start_time_utc:
                    overall_duration_seconds = (now_utc - twitch_session_start_time_utc).total_seconds()

                if overall_duration_seconds > 15 and twitch_session_start_time_utc and bot.loop:
                    asyncio.run_coroutine_threadsafe(log_stream_duration_binary(int(twitch_session_start_time_utc.timestamp()), int(now_utc.timestamp())), bot.loop)

                uta_send_discord_restream_status("stop", UTA_TWITCH_CHANNEL_NAME, stream_duration_seconds=overall_duration_seconds)

                twitch_session_active = False
                twitch_session_start_time_utc = None
                twitch_session_stream_data = None

                logger.info(f"UTA Restream: Cooldown for {UTA_POST_RESTREAM_COOLDOWN_SECONDS}s after stream offline.")
                if shutdown_event.wait(timeout=UTA_POST_RESTREAM_COOLDOWN_SECONDS): break

            elif not is_twitch_live and not twitch_session_active:
                logger.info(f"UTA Restream: {UTA_TWITCH_CHANNEL_NAME} is offline. Waiting {UTA_CHECK_INTERVAL_SECONDS_RESTREAMER}s...")
                if shutdown_event.wait(timeout=UTA_CHECK_INTERVAL_SECONDS_RESTREAMER): break

        except Exception as e:
            logger.error(f"UTA Restreamer Monitor: An unexpected error in {threading.current_thread().name}: {e}", exc_info=True)
            uta_cleanup_restream_processes()

            if effective_youtube_api_enabled and youtube_api_session_active and uta_current_youtube_broadcast_id:
                logger.error(f"UTA YouTube: Attempting to finalize YouTube broadcast {uta_current_youtube_broadcast_id} due to error.")
                try:
                    asyncio.run_coroutine_threadsafe(
                        _uta_youtube_transition_broadcast(uta_yt_service, uta_current_youtube_broadcast_id, "complete"), bot.loop
                    ).result(timeout=30)
                except Exception as yt_err:
                    logger.error(f"UTA YouTube: Failed to finalize broadcast during error handling: {yt_err}")

            twitch_session_active = False
            twitch_session_start_time_utc = None
            twitch_session_stream_data = None
            youtube_api_session_active = False
            uta_current_youtube_broadcast_id = None
            uta_current_youtube_video_id = None
            uta_current_youtube_rtmp_url = None
            uta_current_youtube_stream_key = None
            uta_youtube_next_rollover_time_utc = None

            uta_send_discord_restream_status("stop", UTA_TWITCH_CHANNEL_NAME, stream_duration_seconds=0)

            if shutdown_event.wait(timeout=60): break

    logger.info(f"UTA: Restreamer Monitor thread ({threading.current_thread().name}) finished.")


def uta_stream_status_monitor_loop():
    logger.info(f"UTA: Stream Status Monitor thread ({threading.current_thread().name}) started.")
    is_live_status = False # Tracks if the bot currently considers the stream live
    last_game_name = None
    last_title = None
    last_tags = None # Stored as a list
    current_session_start_time = None # UTC datetime when the current live session (from bot's perspective) started
    current_session_peak_viewers = 0
    last_viewer_log_time = 0 # Unix timestamp of last viewer log

    while not shutdown_event.is_set():
        try:
            if not UTA_TWITCH_CHANNEL_NAME:
                logger.debug("UTA Status: No Twitch channel configured. Skipping status check.")
                if shutdown_event.wait(timeout=UTA_STREAM_STATUS_POLL_INTERVAL_SECONDS): break
                continue

            logger.debug(f"UTA Status: Checking stream status for {UTA_TWITCH_CHANNEL_NAME}...")
            live, stream_data = uta_is_streamer_live(UTA_TWITCH_CHANNEL_NAME) # This hits Twitch API
            current_utc_time = datetime.now(timezone.utc)

            if live: # Twitch stream is currently live
                current_viewers = stream_data.get("viewer_count", 0)
                current_game_name = stream_data.get("game_name", "N/A")
                current_title = stream_data.get("title", "N/A")
                current_tags_from_api = stream_data.get("tags", []) # This is often None or empty from /streams, better from /channels
                stream_started_at_str = stream_data.get("started_at") # Twitch's reported start time

                if not is_live_status: # Stream just went live (or bot just started and found it live)
                    is_live_status = True
                    # Use Twitch's reported start time if available, otherwise use current time
                    current_session_start_time = datetime.fromisoformat(stream_started_at_str.replace('Z', '+00:00')) if stream_started_at_str else current_utc_time
                    
                    last_game_name = current_game_name
                    last_title = current_title
                    last_tags = list(current_tags_from_api or []) # Ensure it's a list

                    current_session_peak_viewers = current_viewers
                    logger.info(f"UTA Status: {UTA_TWITCH_CHANNEL_NAME} is LIVE. Game: {current_game_name}, Title: {current_title}, Tags: {last_tags}")

                    if bot.loop and bot.is_ready(): # Ensure bot is ready before trying to send/log
                        youtube_vid_id_for_log = UTA_CURRENT_YT_VIDEO_ID_FOR_LOGGING if effective_youtube_api_enabled() and youtube_api_session_active_global else None
                        asyncio.run_coroutine_threadsafe(log_stream_activity_binary(
                            EVENT_TYPE_STREAM_START, current_utc_time, 
                            title=current_title, game=current_game_name, tags=last_tags,
                            youtube_video_id=youtube_vid_id_for_log
                        ), bot.loop)
                        
                        embed = discord.Embed(title=f"🔴 {UTA_TWITCH_CHANNEL_NAME} is LIVE!",
                                              description=f"**{current_title}**\nPlaying: **{current_game_name}**\n[Watch Stream](https://twitch.tv/{UTA_TWITCH_CHANNEL_NAME})",
                                              color=discord.Color.red(),
                                              timestamp=current_session_start_time) # Timestamp with stream's actual start
                        if last_tags: embed.add_field(name="Tags", value=", ".join(last_tags[:8]) + ("..." if len(last_tags) > 8 else ""), inline=False)
                        thumbnail_url = stream_data.get("thumbnail_url", "").replace("{width}", "1280").replace("{height}", "720")
                        if thumbnail_url: embed.set_image(url=thumbnail_url + f"?t={int(time.time())}") # Add timestamp to try and avoid caching

                        asyncio.run_coroutine_threadsafe(_send_uta_notification_to_discord(None, embed=embed), bot.loop)
                    last_viewer_log_time = 0 # Reset for new session
                
                else: # Stream was already known to be live, check for changes
                    should_update_yt_metadata = False
                    new_yt_title_for_update = None

                    if current_game_name != last_game_name:
                        logger.info(f"UTA Status: {UTA_TWITCH_CHANNEL_NAME} game changed from '{last_game_name}' to '{current_game_name}'.")
                        if bot.loop and bot.is_ready():
                            asyncio.run_coroutine_threadsafe(log_stream_activity_binary(EVENT_TYPE_GAME_CHANGE, current_utc_time, old_game=last_game_name, new_game=current_game_name), bot.loop)
                            embed = discord.Embed(title=f"🔄 Game Change for {UTA_TWITCH_CHANNEL_NAME}", description=f"Now playing: **{current_game_name}**\nWas: {last_game_name}\n[Watch Stream](https://twitch.tv/{UTA_TWITCH_CHANNEL_NAME})", color=discord.Color.blue(), timestamp=current_utc_time)
                            asyncio.run_coroutine_threadsafe(_send_uta_notification_to_discord(None, embed=embed), bot.loop)
                        last_game_name = current_game_name
                        should_update_yt_metadata = True

                    if current_title != last_title:
                        logger.info(f"UTA Status: {UTA_TWITCH_CHANNEL_NAME} title changed from '{last_title}' to '{current_title}'.")
                        if bot.loop and bot.is_ready():
                            asyncio.run_coroutine_threadsafe(log_stream_activity_binary(EVENT_TYPE_TITLE_CHANGE, current_utc_time, old_title=last_title, new_title=current_title), bot.loop)
                            embed = discord.Embed(title=f"✍️ Title Change for {UTA_TWITCH_CHANNEL_NAME}", description=f"New title: **{current_title}**\n[Watch Stream](https://twitch.tv/{UTA_TWITCH_CHANNEL_NAME})", color=discord.Color.green(), timestamp=current_utc_time)
                            asyncio.run_coroutine_threadsafe(_send_uta_notification_to_discord(None, embed=embed), bot.loop)
                        last_title = current_title
                        should_update_yt_metadata = True
                    
                    # Using current_tags_from_api which might be empty from /streams.
                    # A more robust tag comparison might need to fetch from /channels endpoint if tags are critical.
                    if set(current_tags_from_api or []) != set(last_tags or []): 
                        logger.info(f"UTA Status: {UTA_TWITCH_CHANNEL_NAME} tags changed from '{last_tags}' to '{current_tags_from_api}'.")
                        if bot.loop and bot.is_ready():
                            asyncio.run_coroutine_threadsafe(log_stream_activity_binary(EVENT_TYPE_TAGS_CHANGE, current_utc_time, old_tags=last_tags, new_tags=(current_tags_from_api or [])), bot.loop)
                            embed = discord.Embed(title=f"🏷️ Tags Change for {UTA_TWITCH_CHANNEL_NAME}", color=discord.Color.orange(), timestamp=current_utc_time)
                            embed.add_field(name="Old Tags", value=", ".join(last_tags[:8]) + ("..." if len(last_tags) > 8 else "") or "None", inline=False)
                            embed.add_field(name="New Tags", value=", ".join((current_tags_from_api or [])[:8]) + ("..." if len(current_tags_from_api or []) > 8 else "") or "None", inline=False)
                            embed.add_field(name="Link", value=f"[Watch Stream](https://twitch.tv/{UTA_TWITCH_CHANNEL_NAME})", inline=False)
                            asyncio.run_coroutine_threadsafe(_send_uta_notification_to_discord(None, embed=embed), bot.loop)
                        last_tags = list(current_tags_from_api or [])
                        # Tag changes usually don't trigger YouTube metadata updates unless they are part of title/desc templates.

                    if should_update_yt_metadata and \
                       effective_youtube_api_enabled() and \
                       youtube_api_session_active_global and \
                       uta_current_youtube_broadcast_id and \
                       uta_yt_service:
                        
                        new_yt_title_for_update = UTA_YOUTUBE_DYNAMIC_TITLE_TEMPLATE.format(
                            twitch_username=UTA_TWITCH_CHANNEL_NAME,
                            twitch_title=current_title, 
                            game_name=current_game_name, 
                            part_num=uta_current_restream_part_number, 
                            date=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                            time=datetime.now(timezone.utc).strftime("%H:%M:%S")
                        )
                        # Potentially also update description if it includes game/title
                        new_yt_description_for_update = UTA_YOUTUBE_DYNAMIC_DESCRIPTION_TEMPLATE.format(
                            twitch_username=UTA_TWITCH_CHANNEL_NAME,
                            twitch_title=current_title,
                            game_name=current_game_name
                        )

                        logger.info(f"UTA YouTube: Attempting to update metadata for broadcast {uta_current_youtube_broadcast_id} due to Twitch title/game change.")
                        logger.info(f"UTA YouTube: New proposed title: {new_yt_title_for_update}")
                        
                        update_success_future = asyncio.run_coroutine_threadsafe(
                            _uta_youtube_update_broadcast_metadata(uta_yt_service, uta_current_youtube_broadcast_id, 
                                                                   new_title=new_yt_title_for_update, 
                                                                   new_description=new_yt_description_for_update),
                            bot.loop
                        )
                        try:
                            if update_success_future.result(timeout=20): # Increased timeout for API call
                                logger.info(f"UTA YouTube: Successfully updated metadata for broadcast {uta_current_youtube_broadcast_id}.")
                            else:
                                logger.error(f"UTA YouTube: Failed to update metadata for broadcast {uta_current_youtube_broadcast_id}.")
                        except Exception as e_yt_update:
                            logger.error(f"UTA YouTube: Exception updating metadata for {uta_current_youtube_broadcast_id}: {e_yt_update}", exc_info=True)

                # Viewer count logging
                current_session_peak_viewers = max(current_session_peak_viewers, current_viewers)
                if UTA_VIEWER_COUNT_LOGGING_ENABLED and bot.loop and bot.is_ready() and \
                   (time.time() - last_viewer_log_time >= UTA_VIEWER_COUNT_LOG_INTERVAL_SECONDS):
                    asyncio.run_coroutine_threadsafe(log_viewer_data_binary(current_utc_time, current_viewers), bot.loop)
                    last_viewer_log_time = time.time()

            else: # Twitch stream is currently offline
                if is_live_status: # Stream just went offline
                    is_live_status = False # Update our state
                    duration_seconds = 0
                    session_start_unix, session_end_unix = 0, 0
                    if current_session_start_time: 
                        duration_seconds = (current_utc_time - current_session_start_time).total_seconds()
                        session_start_unix = int(current_session_start_time.timestamp())
                        session_end_unix = int(current_utc_time.timestamp())

                    logger.info(f"UTA Status: {UTA_TWITCH_CHANNEL_NAME} is OFFLINE. Lasted: {format_duration_human(int(duration_seconds))}. Peak Viewers: {current_session_peak_viewers}")
                    
                    avg_viewers, _, num_viewer_datapoints = (None, 0, 0)
                    if UTA_VIEWER_COUNT_LOGGING_ENABLED and UTA_VIEWER_COUNT_LOG_FILE and session_start_unix and session_end_unix :
                        avg_viewers, _, num_viewer_datapoints = _get_viewer_stats_for_period_sync(
                            UTA_VIEWER_COUNT_LOG_FILE, session_start_unix, session_end_unix
                        )

                    games_played_summary_str = "N/A"
                    if UTA_STREAM_ACTIVITY_LOG_FILE and session_start_unix and session_end_unix:
                        game_segments = _parse_stream_activity_for_game_segments_sync(
                            UTA_STREAM_ACTIVITY_LOG_FILE, session_start_unix, session_end_unix
                        )
                        if game_segments:
                            games_summary = {}
                            for seg in game_segments:
                                games_summary[seg['game']] = games_summary.get(seg['game'], 0) + (seg['end_ts'] - seg['start_ts'])
                            sorted_games = sorted(games_summary.items(), key=lambda item: item[1], reverse=True)
                            games_played_parts = [f"{game} ({format_duration_human(int(dur))})" for game, dur in sorted_games]
                            if games_played_parts: games_played_summary_str = ", ".join(games_played_parts)
                            if len(games_played_summary_str) > 1000: games_played_summary_str = games_played_summary_str[:997] + "..."

                    follower_gain_str = "N/A (Follower log N/A)"
                    if FCTD_FOLLOWER_DATA_FILE and FCTD_TWITCH_USERNAME and \
                       FCTD_TWITCH_USERNAME.lower() == (UTA_TWITCH_CHANNEL_NAME or "").lower() and \
                       session_start_unix and session_end_unix: # Only if it's the same channel
                        s_foll, e_foll, _, _, _ = _read_and_find_records_sync(
                            FCTD_FOLLOWER_DATA_FILE, session_start_unix, session_end_unix
                        )
                        if s_foll is not None and e_foll is not None:
                            gain = e_foll - s_foll
                            follower_gain_str = f"{gain:+,} followers"
                        else:
                            follower_gain_str = "No follower data for session"

                    if bot.loop and bot.is_ready():
                        asyncio.run_coroutine_threadsafe(log_stream_activity_binary(EVENT_TYPE_STREAM_END, current_utc_time, duration_seconds=int(duration_seconds), peak_viewers=current_session_peak_viewers), bot.loop)
                        
                        embed = discord.Embed(title=f"📊 Stream Session Summary for {UTA_TWITCH_CHANNEL_NAME}", color=discord.Color.dark_grey(), timestamp=current_utc_time)
                        embed.set_author(name=UTA_TWITCH_CHANNEL_NAME, url=f"https://twitch.tv/{UTA_TWITCH_CHANNEL_NAME}")
                        embed.add_field(name="Status", value="⚫ OFFLINE", inline=False)
                        embed.add_field(name="Duration", value=format_duration_human(int(duration_seconds)), inline=True)
                        embed.add_field(name="Peak Viewers", value=f"{current_session_peak_viewers:,}", inline=True)
                        if avg_viewers is not None:
                            embed.add_field(name="Avg. Viewers", value=f"{avg_viewers:,.1f} (from {num_viewer_datapoints} points)", inline=True)
                        else:
                            embed.add_field(name="Avg. Viewers", value="N/A", inline=True)
                        
                        embed.add_field(name="Games Played", value=games_played_summary_str, inline=False)
                        if FCTD_TWITCH_USERNAME == UTA_TWITCH_CHANNEL_NAME: # Only show if FCTD is tracking the same user
                             embed.add_field(name="Follower Change (Session)", value=follower_gain_str, inline=False)
                        
                        asyncio.run_coroutine_threadsafe(_send_uta_notification_to_discord(None, embed=embed), bot.loop)

                    # Reset session variables
                    current_session_start_time = None; current_session_peak_viewers = 0; 
                    last_game_name = None; last_title = None; last_tags = None
            
            if shutdown_event.wait(timeout=UTA_STREAM_STATUS_POLL_INTERVAL_SECONDS): break
        except Exception as e:
            logger.error(f"UTA Stream Status Monitor: An unexpected error in {threading.current_thread().name}: {e}", exc_info=True)
            # Reset state on major error to avoid inconsistent notifications if it recovers
            is_live_status = False; current_session_start_time = None; current_session_peak_viewers = 0;
            last_game_name = None; last_title = None; last_tags = None;
            if shutdown_event.wait(timeout=60): break # Wait a bit longer after an error
            
    logger.info(f"UTA: Stream Status Monitor thread ({threading.current_thread().name}) finished.")

async def _stop_uta_threads():
    global uta_clip_thread, uta_restreamer_thread, uta_stream_status_thread
    logger.info("Reload/Shutdown: Stopping UTA threads...")
    shutdown_event.set(); threads_to_join = []
    if uta_clip_thread and uta_clip_thread.is_alive(): threads_to_join.append(uta_clip_thread)
    if uta_restreamer_thread and uta_restreamer_thread.is_alive(): threads_to_join.append(uta_restreamer_thread)
    if uta_stream_status_thread and uta_stream_status_thread.is_alive(): threads_to_join.append(uta_stream_status_thread)
    for t in threads_to_join:
        logger.info(f"Reload/Shutdown: Attempting to join {t.name}...")
        await asyncio.to_thread(t.join, timeout=10)
        if t.is_alive(): logger.warning(f"Reload/Shutdown: Thread {t.name} did not join cleanly after 10s.")
        else: logger.info(f"Reload/Shutdown: Thread {t.name} joined.")
    if UTA_RESTREAMER_ENABLED: logger.info("Reload/Shutdown: Performing final UTA process cleanup."); uta_cleanup_restream_processes()
    uta_clip_thread = None; uta_restreamer_thread = None; uta_stream_status_thread = None
    logger.info("Reload/Shutdown: UTA threads processed for stopping.")

def _start_uta_threads(reason="Starting"):
    global uta_clip_thread, uta_restreamer_thread, uta_stream_status_thread, \
           uta_broadcaster_id_cache, uta_sent_clip_ids, uta_yt_service

    shutdown_event.clear(); logger.info(f"UTA: {reason} UTA threads. Shutdown event cleared.")
    uta_broadcaster_id_cache = None; uta_sent_clip_ids.clear();
    logger.info(f"UTA: {reason} - Cleared broadcaster ID cache and sent clip IDs.")

    if not uta_get_twitch_access_token():
        logger.critical(f"UTA: {reason} - Failed to get/refresh Twitch token for UTA. Functionality will be impaired.")

    if UTA_YOUTUBE_API_ENABLED and GOOGLE_API_AVAILABLE:
        logger.info(f"UTA: {reason} - Attempting to initialize YouTube API service...")
        if not _uta_youtube_get_service():
             logger.warning(f"UTA YouTube: Failed to initialize YouTube service during thread start ({reason}). Restreamer will attempt again if stream goes live, or fall back.")
    elif UTA_YOUTUBE_API_ENABLED and not GOOGLE_API_AVAILABLE:
        logger.error(f"UTA: {reason} - YouTube API is enabled in config, but Google libraries are not installed. YouTube API features for restreamer will be disabled.")


    if UTA_CLIP_MONITOR_ENABLED and UTA_TWITCH_CHANNEL_NAME:
        logger.info(f"UTA: {reason} Clip Monitor thread...")
        uta_clip_thread = threading.Thread(target=uta_clip_monitor_loop, name=f"UTAClipMon-{reason[:4]}", daemon=True); uta_clip_thread.start()
    else: logger.info(f"UTA: {reason} - Clip Monitor disabled or UTA_TWITCH_CHANNEL_NAME not set.")

    restreamer_prereqs_met = False
    if UTA_RESTREAMER_ENABLED and UTA_TWITCH_CHANNEL_NAME and \
       shutil.which(UTA_STREAMLINK_PATH) and shutil.which(UTA_FFMPEG_PATH):
        if UTA_YOUTUBE_API_ENABLED and GOOGLE_API_AVAILABLE:
            restreamer_prereqs_met = True
            logger.info(f"UTA: {reason} - Restreamer (YouTube API Mode) prerequisites met (paths ok, API enabled).")
        elif not UTA_YOUTUBE_API_ENABLED and UTA_YOUTUBE_RTMP_URL_BASE and UTA_YOUTUBE_STREAM_KEY and \
             "YOUR_YOUTUBE_STREAM_KEY" not in UTA_YOUTUBE_STREAM_KEY:
            restreamer_prereqs_met = True
            logger.info(f"UTA: {reason} - Restreamer (Legacy RTMP Mode) prerequisites met (paths, URL, Key ok).")
        else:
            logger.warning(f"UTA: {reason} - Restreamer enabled, but YouTube configuration (API or Legacy RTMP) is incomplete or API libs missing.")


    if restreamer_prereqs_met:
        logger.info(f"UTA: {reason} Restreamer Monitor thread...")
        uta_restreamer_thread = threading.Thread(target=uta_restreamer_monitor_loop, name=f"UTARestream-{reason[:4]}", daemon=True); uta_restreamer_thread.start()
    else:
        logger.info(f"UTA: {reason} - Restreamer disabled or prerequisites not met.")

    if UTA_STREAM_STATUS_NOTIFICATIONS_ENABLED and UTA_TWITCH_CHANNEL_NAME:
        logger.info(f"UTA: {reason} Stream Status Monitor thread...")
        uta_stream_status_thread = threading.Thread(target=uta_stream_status_monitor_loop, name=f"UTAStatusMon-{reason[:4]}", daemon=True); uta_stream_status_thread.start()
    else: logger.info(f"UTA: {reason} - Stream Status Monitor disabled or UTA_TWITCH_CHANNEL_NAME not set.")

# =====================================================================================
# --- Bot Setup, Events, Tasks, Commands ---
# =====================================================================================
intents = discord.Intents.default(); intents.message_content = True
bot = commands.Bot(command_prefix=FCTD_COMMAND_PREFIX, intents=intents, help_command=None, owner_id=int(owner_id_from_config) if owner_id_from_config else None)

@bot.event
async def on_ready():
    logger.info(f'{bot.user.name} (ID: {bot.user.id}) connected to Discord!')
    global bot_start_time; bot_start_time = datetime.now(timezone.utc)
    await log_bot_session_event(BOT_EVENT_START, bot_start_time)

    logger.info(f'Bot started at: {bot_start_time.isoformat()}'); logger.info(f'Cmd Prefix: {FCTD_COMMAND_PREFIX}')
    fctd_cmd_ch_msg = f'Listening for fctd cmds in ch ID: {FCTD_COMMAND_CHANNEL_ID}' if FCTD_COMMAND_CHANNEL_ID else 'Listening for fctd cmds in ALL channels.'
    logger.info(fctd_cmd_ch_msg); logger.info(f'Connected to {len(bot.guilds)} guilds.')

    if FCTD_TWITCH_USERNAME:
        logger.info(f'fctd: Targeting Twitch User for followers: {FCTD_TWITCH_USERNAME}')
        global fctd_current_twitch_user_id
        fctd_current_twitch_user_id = await fctd_twitch_api.get_user_id(FCTD_TWITCH_USERNAME)
        if not fctd_current_twitch_user_id:
            logger.error(f"fctd: CRITICAL: No Twitch User ID for {FCTD_TWITCH_USERNAME}. Follower features FAIL.")
        else:
            logger.info(f"fctd: Twitch User ID for {FCTD_TWITCH_USERNAME}: {fctd_current_twitch_user_id}")
            if FCTD_TARGET_CHANNEL_ID or FCTD_FOLLOWER_DATA_FILE:
                if not update_channel_name_and_log_followers.is_running():
                    update_channel_name_and_log_followers.start()
    else:
        logger.warning("fctd: FCTD_TWITCH_USERNAME not set. Follower tracking disabled.")

    if UTA_ENABLED:
        logger.info("--- UTA Module Enabled ---")
        if not UTA_TWITCH_CHANNEL_NAME or "target_twitch_username_for_uta" in UTA_TWITCH_CHANNEL_NAME:
            logger.error("UTA: UTA_TWITCH_CHANNEL_NAME not configured. UTA features limited/disabled.")
        else:
            logger.info(f"UTA: Targeting Twitch Channel: {UTA_TWITCH_CHANNEL_NAME}")
            if UTA_RESTREAMER_ENABLED:
                if not shutil.which(UTA_STREAMLINK_PATH):
                    logger.critical(f"UTA CRITICAL: Streamlink '{UTA_STREAMLINK_PATH}' not found. Restreamer will be impaired.")
                if not shutil.which(UTA_FFMPEG_PATH):
                    logger.critical(f"UTA CRITICAL: FFmpeg '{UTA_FFMPEG_PATH}' not found. Restreamer will be impaired.")

                if UTA_YOUTUBE_API_ENABLED:
                    if not GOOGLE_API_AVAILABLE:
                        logger.critical("UTA CRITICAL: YouTube API is enabled for restreamer, but Google API client libraries are not installed. Install 'google-api-python-client google-auth-httplib2 google-auth-oauthlib'. YouTube API features will be disabled.")
                    elif not os.path.exists(UTA_YOUTUBE_CLIENT_SECRET_FILE):
                         logger.critical(f"UTA CRITICAL: YouTube API client secret file ('{UTA_YOUTUBE_CLIENT_SECRET_FILE}') not found. YouTube API features for restreamer will be disabled.")

        _start_uta_threads(reason="Initial Startup")
    else:
        logger.info("--- UTA Module Disabled ---")

    if not MATPLOTLIB_AVAILABLE:
        logger.warning("Matplotlib library not found. Plotting commands will be disabled. Install with 'pip install matplotlib'.")


@tasks.loop(minutes=FCTD_UPDATE_INTERVAL_MINUTES)
async def update_channel_name_and_log_followers():
    if not fctd_current_twitch_user_id or not FCTD_TWITCH_USERNAME: return
    count = await fctd_twitch_api.get_follower_count(fctd_current_twitch_user_id)
    time_utc = datetime.now(timezone.utc)
    if count is not None:
        if FCTD_FOLLOWER_DATA_FILE: await log_follower_data_binary(time_utc, count)
        if FCTD_TARGET_CHANNEL_ID:
            ch = bot.get_channel(FCTD_TARGET_CHANNEL_ID)
            if ch:
                new_name = f"{FCTD_CHANNEL_NAME_PREFIX}{count:,}{FCTD_CHANNEL_NAME_SUFFIX}"
                if ch.name != new_name:
                    try: await ch.edit(name=new_name); logger.info(f"fctd: Ch name for {FCTD_TWITCH_USERNAME} to: {new_name}")
                    except discord.Forbidden: logger.error(f"fctd: No 'Manage Channels' for ch {FCTD_TARGET_CHANNEL_ID}.")
                    except discord.HTTPException as e: logger.error(f"fctd: Failed ch name edit (HTTP): {e}")
                    except Exception as e: logger.error(f"fctd: Unexpected ch name edit error: {e}")
            elif not ch: logger.warning(f"fctd: Target ch {FCTD_TARGET_CHANNEL_ID} not found.")
    else: logger.warning(f"fctd: No follower count for {FCTD_TWITCH_USERNAME}. Skip update/log.")

@update_channel_name_and_log_followers.before_loop
async def before_update_task():
    await bot.wait_until_ready()
    global FCTD_UPDATE_INTERVAL_MINUTES
    current_interval_minutes = update_channel_name_and_log_followers.minutes
    if current_interval_minutes != FCTD_UPDATE_INTERVAL_MINUTES :
        logger.info(f"fctd: Follower task interval mismatch (current: {current_interval_minutes}, config: {FCTD_UPDATE_INTERVAL_MINUTES}). Updating loop interval.")
        try:
            update_channel_name_and_log_followers.change_interval(minutes=FCTD_UPDATE_INTERVAL_MINUTES)
            logger.info(f"fctd: Follower task interval changed to {FCTD_UPDATE_INTERVAL_MINUTES} minutes.")
        except Exception as e:
             logger.error(f"fctd: Error changing follower task interval dynamically: {e}")

    if FCTD_TWITCH_USERNAME and (FCTD_TARGET_CHANNEL_ID or FCTD_FOLLOWER_DATA_FILE):
        logger.info(f"fctd: Follower task for {FCTD_TWITCH_USERNAME} (interval: {FCTD_UPDATE_INTERVAL_MINUTES} min) will start/continue if not already running.")
    else:
        logger.info("fctd: Follower task prerequisites not met. It will not run.")
        if update_channel_name_and_log_followers.is_running(): update_channel_name_and_log_followers.cancel()

# --- Bot Commands ---
@bot.command(name="uptime", help="Shows how long the bot has been running in the current session.")
async def uptime_command(ctx: commands.Context):
    if FCTD_COMMAND_CHANNEL_ID is not None and ctx.channel.id != FCTD_COMMAND_CHANNEL_ID: return
    global bot_start_time
    if bot_start_time is None: await ctx.send("Bot start time not recorded yet."); return
    uptime_delta = datetime.now(timezone.utc) - bot_start_time
    human_uptime = format_duration_human(int(uptime_delta.total_seconds()))
    embed = discord.Embed(title="Bot Uptime (Current Session)", description=f"I have been running for **{human_uptime}** in this session.", color=discord.Color.green())
    embed.add_field(name="Current Session Started At", value=discord.utils.format_dt(bot_start_time, 'F'), inline=False)
    await ctx.send(embed=embed)

@bot.command(name="runtime", help="Shows bot's total runtime in a past period. Usage: !runtime <period> (e.g., 24h, 7d)")
async def runtime_command(ctx: commands.Context, *, duration_input: str = None):
    if FCTD_COMMAND_CHANNEL_ID is not None and ctx.channel.id != FCTD_COMMAND_CHANNEL_ID: return
    if not BOT_SESSION_LOG_FILE_PATH:
        await ctx.send("Bot session logging is not configured."); return
    if not os.path.exists(BOT_SESSION_LOG_FILE_PATH):
        await ctx.send(f"Bot session log file (`{os.path.basename(BOT_SESSION_LOG_FILE_PATH)}`) not found."); return
    if duration_input is None:
        await ctx.send(f"Please specify a period. Usage: `{FCTD_COMMAND_PREFIX}runtime <duration>` (e.g., `24h`, `7d`, `1mo`)."); return
    time_delta, period_name_display = parse_duration_to_timedelta(duration_input)
    if not time_delta: await ctx.send(period_name_display); return
    now_utc = datetime.now(timezone.utc)
    query_end_unix = int(now_utc.timestamp())
    query_start_unix = int((now_utc - time_delta).timestamp())
    async with ctx.typing():
        total_uptime_sec, num_sessions = await asyncio.to_thread(
            _calculate_runtime_in_period_sync, BOT_SESSION_LOG_FILE_PATH, query_start_unix, query_end_unix)
    human_uptime_in_period = format_duration_human(total_uptime_sec)
    embed = discord.Embed(
        title=f"Bot Runtime History ({period_name_display})",
        description=f"The bot was active for a total of **{human_uptime_in_period}** during the {period_name_display}.",
        color=discord.Color.blue())
    embed.add_field(name="Query Period Start", value=discord.utils.format_dt(datetime.fromtimestamp(query_start_unix, tz=timezone.utc), 'F'), inline=True)
    embed.add_field(name="Query Period End", value=discord.utils.format_dt(datetime.fromtimestamp(query_end_unix, tz=timezone.utc), 'F'), inline=True)
    embed.add_field(name="Contributing Sessions", value=str(num_sessions), inline=True)
    period_duration_seconds = query_end_unix - query_start_unix
    if period_duration_seconds > 0 :
        percentage_uptime = (total_uptime_sec / period_duration_seconds) * 100
        embed.add_field(name="Uptime Percentage", value=f"{percentage_uptime:.2f}% of the period", inline=False)
    else: embed.add_field(name="Uptime Percentage", value="N/A (invalid period)", inline=False)
    await ctx.send(embed=embed)

def get_config_diff(old_conf, new_conf):
    diff = {}; all_keys = set(old_conf.keys()) | set(new_conf.keys())
    for key in all_keys:
        old_val = old_conf.get(key); new_val = new_conf.get(key)
        if old_val != new_val:
            old_display = str(old_val)[:30] + "..." if isinstance(old_val, str) and len(old_val) > 30 else old_val
            new_display = str(new_val)[:30] + "..." if isinstance(new_val, str) and len(new_val) > 30 else new_val
            if "TOKEN" in key.upper() or "SECRET" in key.upper() or "KEY" in key.upper(): old_display = "**** (hidden)" if old_val else "Not set"; new_display = "**** (hidden)" if new_val else "Not set"
            diff[key] = {"old": old_display, "new": new_display}
    return diff

@bot.command(name="reloadconfig", aliases=['reload'], help="Reloads config.json. (Bot owner only)")
@commands.is_owner()
async def reload_config_command(ctx: commands.Context):
    global config_data, fctd_twitch_api, fctd_current_twitch_user_id, \
           uta_shared_access_token, uta_token_expiry_time, uta_broadcaster_id_cache, \
           FCTD_UPDATE_INTERVAL_MINUTES, uta_yt_service
    await ctx.send("Attempting to reload configuration...");
    logger.info(f"Configuration reload initiated by {ctx.author} (ID: {ctx.author.id}).")
    old_config_data_copy = config_data.copy()
    old_uta_youtube_api_enabled = old_config_data_copy.get('UTA_YOUTUBE_API_ENABLED', False)
    old_uta_youtube_client_secret_file = old_config_data_copy.get('UTA_YOUTUBE_CLIENT_SECRET_FILE')
    old_uta_youtube_token_file = old_config_data_copy.get('UTA_YOUTUBE_TOKEN_FILE')
    success, new_loaded_data = load_config(initial_load=False)
    if not success: await ctx.send(f"Configuration reload failed: {new_loaded_data}"); logger.error(f"Configuration reload failed: {new_loaded_data}"); return
    was_uta_enabled_overall = old_config_data_copy.get('UTA_ENABLED', False)
    new_uta_enabled_overall = new_loaded_data.get('UTA_ENABLED', False)
    uta_config_changed_structurally = False
    if new_uta_enabled_overall:
        uta_key_params = [ # Ensure this list is comprehensive
            "UTA_TWITCH_CHANNEL_NAME", "UTA_STREAMLINK_PATH", "UTA_FFMPEG_PATH",
            "UTA_YOUTUBE_API_ENABLED", "UTA_YOUTUBE_CLIENT_SECRET_FILE", "UTA_YOUTUBE_TOKEN_FILE",
            "UTA_YOUTUBE_RTMP_URL_BASE", "UTA_YOUTUBE_STREAM_KEY",
            "UTA_STREAM_STATUS_POLL_INTERVAL_SECONDS",
            "UTA_CHECK_INTERVAL_SECONDS_CLIPS", "UTA_CHECK_INTERVAL_SECONDS_RESTREAMER",
            "UTA_CLIP_LOOKBACK_MINUTES", "UTA_RESTREAM_CHECK_INTERVAL_WHEN_LIVE",
            "UTA_POST_RESTREAM_COOLDOWN_SECONDS", "UTA_VIEWER_COUNT_LOG_INTERVAL_SECONDS",
            "UTA_CLIP_MONITOR_ENABLED", "UTA_RESTREAMER_ENABLED", "UTA_STREAM_STATUS_NOTIFICATIONS_ENABLED",
            "UTA_YOUTUBE_SCHEDULED_ROLLOVER_HOURS",
            "UTA_RESTREAM_MAX_CONSECUTIVE_FAILURES", "UTA_RESTREAM_LONG_COOLDOWN_SECONDS",
            "UTA_YOUTUBE_PLAYABILITY_CHECK_ENABLED", "UTA_YOUTUBE_PLAYABILITY_CHECK_RETRIES",
            "UTA_YOUTUBE_PLAYABILITY_CHECK_DELAY_SECONDS", "UTA_FFMPEG_STARTUP_WAIT_SECONDS"
        ]
        for key in uta_key_params:
            if old_config_data_copy.get(key) != new_loaded_data.get(key):
                uta_config_changed_structurally = True; break
    if (was_uta_enabled_overall != new_uta_enabled_overall) or \
       (new_uta_enabled_overall and uta_config_changed_structurally):
        logger.info("Reload: UTA related configuration or master enable status changed. Managing UTA threads.")
        await _stop_uta_threads()
    config_data = new_loaded_data
    apply_config_globally(config_data)
    diff = get_config_diff(old_config_data_copy, config_data)
    diff_summary = "\n".join([f"**'{k}'**: `{v['old']}` -> `{v['new']}`" for k,v in diff.items()]) if diff else "No changes detected."
    logger.info(f"Configuration reload diff:\n{diff_summary}")
    if 'DISCORD_TOKEN' in diff: logger.warning("DISCORD_TOKEN changed. Full bot restart required."); await ctx.send("⚠️ **DISCORD_TOKEN changed!** Full bot restart required.")
    if 'DISCORD_BOT_OWNER_ID' in diff and bot:
        new_owner_id_str = config_data.get('DISCORD_BOT_OWNER_ID')
        if new_owner_id_str:
            try: bot.owner_id = int(new_owner_id_str); logger.info(f"Bot owner ID updated to: {bot.owner_id}")
            except ValueError: logger.error(f"Invalid DISCORD_BOT_OWNER_ID in new config: {new_owner_id_str}")
        else: bot.owner_id = None; logger.info("Bot owner ID removed from config.")
    if 'FCTD_COMMAND_PREFIX' in diff and bot: bot.command_prefix = FCTD_COMMAND_PREFIX; logger.info(f"Bot command prefix updated to: {FCTD_COMMAND_PREFIX}")
    if 'TWITCH_CLIENT_ID' in diff or 'TWITCH_CLIENT_SECRET' in diff:
        logger.info("Twitch client ID/secret changed. Re-init fctd.TwitchAPI, clear UTA Twitch token.")
        with uta_token_refresh_lock: uta_shared_access_token = None; uta_token_expiry_time = 0
        if FCTD_TWITCH_USERNAME:
            fctd_current_twitch_user_id = await fctd_twitch_api.get_user_id(FCTD_TWITCH_USERNAME);
            logger.info(f"Reload: Re-fetched fctd_current_twitch_user_id: {fctd_current_twitch_user_id}")
    if 'FCTD_TWITCH_USERNAME' in diff:
        logger.info(f"FCTD_TWITCH_USERNAME changed. Updating fctd_current_twitch_user_id.")
        if FCTD_TWITCH_USERNAME:
            fctd_current_twitch_user_id = await fctd_twitch_api.get_user_id(FCTD_TWITCH_USERNAME);
            logger.info(f"New fctd_current_twitch_user_id: {fctd_current_twitch_user_id}")
        else: fctd_current_twitch_user_id = None; logger.info("FCTD_TWITCH_USERNAME removed. User ID set to None.")
    if update_channel_name_and_log_followers.is_running() and \
       update_channel_name_and_log_followers.minutes != FCTD_UPDATE_INTERVAL_MINUTES:
        try:
            update_channel_name_and_log_followers.change_interval(minutes=FCTD_UPDATE_INTERVAL_MINUTES)
            logger.info(f"Follower update interval changed to {FCTD_UPDATE_INTERVAL_MINUTES} minutes.")
        except Exception as e: logger.error(f"Error changing follower task interval post-reload: {e}")
    should_run_fctd_task = bool(FCTD_TWITCH_USERNAME and fctd_current_twitch_user_id and (FCTD_TARGET_CHANNEL_ID or FCTD_FOLLOWER_DATA_FILE))
    if should_run_fctd_task and not update_channel_name_and_log_followers.is_running():
        logger.info("Reload: Starting follower task due to config changes."); update_channel_name_and_log_followers.start()
    elif not should_run_fctd_task and update_channel_name_and_log_followers.is_running():
        logger.info("Reload: Stopping follower task due to config changes."); update_channel_name_and_log_followers.cancel()
    new_uta_youtube_api_enabled = config_data.get('UTA_YOUTUBE_API_ENABLED', False)
    new_uta_youtube_client_secret_file = config_data.get('UTA_YOUTUBE_CLIENT_SECRET_FILE')
    new_uta_youtube_token_file = config_data.get('UTA_YOUTUBE_TOKEN_FILE')
    if GOOGLE_API_AVAILABLE and new_uta_youtube_api_enabled and \
       (old_uta_youtube_api_enabled != new_uta_youtube_api_enabled or \
        old_uta_youtube_client_secret_file != new_uta_youtube_client_secret_file or \
        old_uta_youtube_token_file != new_uta_youtube_token_file):
        logger.info("Reload: YouTube API configuration changed. Re-initializing YouTube service.")
        uta_yt_service = None
    if (was_uta_enabled_overall != new_uta_enabled_overall) or \
       (new_uta_enabled_overall and uta_config_changed_structurally):
        logger.info("Reload: UTA is active and its config/status necessitates a thread (re)start.")
        _start_uta_threads(reason="Reload")
    elif not new_uta_enabled_overall and was_uta_enabled_overall:
        logger.info("Reload: UTA is now disabled. Threads were already stopped (or _stop_uta_threads handled it).")
    else: logger.info("Reload: UTA status/config did not necessitate a full thread group restart beyond what was already handled.")
    final_message = f"Configuration reloaded successfully.\n**Changes:**\n{diff_summary if len(diff_summary) < 1800 else 'Too many changes to display, see logs.'}"
    await ctx.send(final_message)

@bot.command(name="followers", aliases=['foll', 'followerstats'])
async def followers_command(ctx: commands.Context, *, duration_input: str = None):
    if FCTD_COMMAND_CHANNEL_ID is not None and ctx.channel.id != FCTD_COMMAND_CHANNEL_ID: return
    if not FCTD_TWITCH_USERNAME: await ctx.send("fctd: Twitch user not configured."); return
    if duration_input is None:
        embed = discord.Embed(title=f"{FCTD_TWITCH_USERNAME} Follower Stats", description=f"Use `{FCTD_COMMAND_PREFIX}followers <duration>`.", color=discord.Color.purple())
        embed.add_field(name="Format", value="`10m`, `2h`, `3d`, `1w`, `1mo`, `1y`", inline=False); await ctx.send(embed=embed); return
    if not FCTD_FOLLOWER_DATA_FILE or not os.path.exists(FCTD_FOLLOWER_DATA_FILE) or os.path.getsize(FCTD_FOLLOWER_DATA_FILE) < BINARY_RECORD_SIZE:
        await ctx.send(f"fctd: Not enough follower data for {FCTD_TWITCH_USERNAME}."); return
    delta, period = parse_duration_to_timedelta(duration_input)
    if not delta: await ctx.send(period); return
    async with ctx.typing(): msg = await get_follower_gain_for_period(delta, period)
    await ctx.send(msg or "fctd: Error fetching follower data.")

@bot.command(name="follrate", aliases=['growthrate'], help="Shows follower growth rate & graph. Usage: !follrate <period e.g., 7d, 1mo>")
async def follower_rate_command(ctx: commands.Context, *, duration_input: str = None):
    if FCTD_COMMAND_CHANNEL_ID is not None and ctx.channel.id != FCTD_COMMAND_CHANNEL_ID: return
    if not FCTD_TWITCH_USERNAME:
        await ctx.send("fctd: Twitch user not configured for follower tracking."); return
    if not FCTD_FOLLOWER_DATA_FILE or not os.path.exists(FCTD_FOLLOWER_DATA_FILE) or os.path.getsize(FCTD_FOLLOWER_DATA_FILE) < BINARY_RECORD_SIZE * 2:
        await ctx.send(f"fctd: Not enough follower data for {FCTD_TWITCH_USERNAME} to calculate rates (need at least 2 records)."); return
    if duration_input is None:
        await ctx.send(f"Please specify a period. Usage: `{FCTD_COMMAND_PREFIX}follrate <duration>` (e.g., `7d`, `30d`, `3mo`)."); return
    time_delta, period_name = parse_duration_to_timedelta(duration_input)
    if not time_delta: await ctx.send(period_name); return
    now_utc = datetime.now(timezone.utc); cutoff_datetime_utc = now_utc - time_delta
    cutoff_timestamp_unix = int(cutoff_datetime_utc.timestamp())
    discord_file_to_send = None
    async with ctx.typing():
        start_c, end_c, first_ts_unix, last_ts_unix, _ = await asyncio.to_thread(
            _read_and_find_records_sync, FCTD_FOLLOWER_DATA_FILE, cutoff_timestamp_unix, None)
        if start_c is None or end_c is None or first_ts_unix is None or last_ts_unix is None or last_ts_unix <= first_ts_unix :
            await ctx.send(f"Could not retrieve sufficient distinct data points for `{period_name}` to calculate rates."); return
        gain = end_c - start_c; actual_duration_seconds = last_ts_unix - first_ts_unix
        actual_duration_days = actual_duration_seconds / 86400.0
        if actual_duration_days < 1/24/4: # approx 15 mins
            await ctx.send(f"Data range too short ({format_duration_human(actual_duration_seconds)}) to calculate meaningful rates for {period_name}."); return
        avg_per_day = gain / actual_duration_days if actual_duration_days > 0 else 0
        avg_per_week = avg_per_day * 7; avg_per_month = avg_per_day * 30.4375 # Avg month days
        actual_start_dt = datetime.fromtimestamp(first_ts_unix, timezone.utc)
        actual_end_dt = datetime.fromtimestamp(last_ts_unix, timezone.utc)
        embed = discord.Embed(
            title=f"Follower Growth Rate for {FCTD_TWITCH_USERNAME}",
            description=f"Analysis period: **{period_name}**\nEffective data from {discord.utils.format_dt(actual_start_dt, 'R')} to {discord.utils.format_dt(actual_end_dt, 'R')}",
            color=discord.Color.green() if gain >=0 else discord.Color.red())
        embed.add_field(name="Total Change", value=f"{gain:+,} followers", inline=False)
        embed.add_field(name="Effective Data Duration", value=format_duration_human(int(actual_duration_seconds)), inline=False)
        embed.add_field(name="Avg. per Day", value=f"{avg_per_day:+.2f}", inline=True)
        embed.add_field(name="Avg. per Week", value=f"{avg_per_week:+.2f}", inline=True)
        embed.add_field(name="Avg. per Month", value=f"{avg_per_month:+.2f}", inline=True)
        embed.set_footer(text=f"Initial: {start_c:,} | Final: {end_c:,}")
        if MATPLOTLIB_AVAILABLE:
            plot_timestamps, plot_counts = [], []
            try:
                with open(FCTD_FOLLOWER_DATA_FILE, 'rb') as f_plot:
                    while True:
                        chunk = f_plot.read(BINARY_RECORD_SIZE)
                        if not chunk: break
                        if len(chunk) < BINARY_RECORD_SIZE: break
                        ts_plot, count_plot = struct.unpack(BINARY_RECORD_FORMAT, chunk)
                        # Extend plot range slightly before cutoff to show trend leading into period
                        if ts_plot >= (cutoff_timestamp_unix - time_delta.total_seconds()*0.1) and ts_plot <= int(now_utc.timestamp()):
                            plot_timestamps.append(datetime.fromtimestamp(ts_plot, tz=timezone.utc)); plot_counts.append(count_plot)
                if len(plot_timestamps) > 1:
                    fig, ax = plt.subplots(figsize=(10, 5))
                    ax.plot(plot_timestamps, plot_counts, marker='.', linestyle='-', markersize=3, color='cyan')
                    ax.set_title(f"Follower Trend ({period_name})", fontsize=12)
                    ax.set_xlabel("Date/Time (UTC)", fontsize=10); ax.set_ylabel("Follower Count", fontsize=10)
                    ax.grid(True, linestyle=':', alpha=0.7); ax.tick_params(axis='x', labelrotation=30, labelsize=8)
                    ax.tick_params(axis='y', labelsize=8); ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
                    fig.patch.set_alpha(0); ax.set_facecolor('#2C2F33') # Dark background for axes
                    ax.spines['top'].set_visible(False); ax.spines['right'].set_visible(False)
                    ax.spines['bottom'].set_color('grey'); ax.spines['left'].set_color('grey')
                    ax.tick_params(colors='lightgrey'); ax.yaxis.label.set_color('lightgrey'); ax.xaxis.label.set_color('lightgrey')
                    ax.title.set_color('white')
                    img_bytes = io.BytesIO(); fig.savefig(img_bytes, format='png', bbox_inches='tight', facecolor=fig.get_facecolor())
                    img_bytes.seek(0); plt.close(fig)
                    plot_filename = f"follrate_{FCTD_TWITCH_USERNAME}_{datetime.now().strftime('%Y%m%d%H%M')}.png"
                    discord_file_to_send = discord.File(fp=img_bytes, filename=plot_filename)
                    embed.set_image(url=f"attachment://{plot_filename}")
                else: logger.info(f"Not enough data points ({len(plot_timestamps)}) for follrate plot.")
            except FileNotFoundError: logger.warning(f"follrate plot: {FCTD_FOLLOWER_DATA_FILE} not found.")
            except Exception as e_plot: logger.error(f"Error generating follrate plot: {e_plot}", exc_info=True)
    await ctx.send(embed=embed, file=discord_file_to_send if discord_file_to_send else None)

@bot.command(name="readdata", help="Dumps raw data. Keys: followers, viewers, durations, activity, sessions. Owner only.")
@commands.is_owner()
async def read_data_command(ctx: commands.Context, filename_key: str = "followers", max_records_str: str = "50"):
    if FCTD_COMMAND_CHANNEL_ID is not None and ctx.channel.id != FCTD_COMMAND_CHANNEL_ID: return
    filepath_to_read = None; record_format_expected = BINARY_RECORD_FORMAT; record_size_expected = BINARY_RECORD_SIZE
    is_duration_file, is_activity_file, is_bot_session_file = False, False, False; data_type_name = "Unknown"
    filename_key_lower = filename_key.lower()
    if filename_key_lower in ["followers", "foll"]: filepath_to_read = FCTD_FOLLOWER_DATA_FILE; data_type_name = "Follower"
    elif filename_key_lower in ["viewers", "views"]: filepath_to_read = UTA_VIEWER_COUNT_LOG_FILE; data_type_name = "Viewer Count"
    elif filename_key_lower in ["durations", "streamdurations"]:
        filepath_to_read = UTA_STREAM_DURATION_LOG_FILE; record_format_expected, record_size_expected = STREAM_DURATION_RECORD_FORMAT, STREAM_DURATION_RECORD_SIZE
        is_duration_file = True; data_type_name = "Stream Duration"
    elif filename_key_lower in ["activity", "streamactivity"]:
        filepath_to_read = UTA_STREAM_ACTIVITY_LOG_FILE; is_activity_file = True; data_type_name = "Stream Activity"
        record_size_expected = SA_BASE_HEADER_SIZE # Header only, body is dynamic
    elif filename_key_lower in ["sessions", "botsessions"]:
        filepath_to_read = BOT_SESSION_LOG_FILE_PATH; record_format_expected, record_size_expected = BOT_SESSION_RECORD_FORMAT, BOT_SESSION_RECORD_SIZE
        is_bot_session_file = True; data_type_name = "Bot Session"
    else: await ctx.send(f"Unknown data file key '{filename_key}'. Use 'followers', 'viewers', 'durations', 'activity', or 'sessions'."); return
    if not filepath_to_read: await ctx.send(f"{data_type_name} data file not configured."); return
    try: max_r = min(max(1, int(max_records_str)), 200)
    except ValueError: max_r = 50; await ctx.send("Invalid num for max_records, using 50.")
    async with ctx.typing():
        lines = []; basename = os.path.basename(filepath_to_read); chunks = []
        if not os.path.exists(filepath_to_read): chunks = [f"```Error: File '{filepath_to_read}' not found.```"]
        elif os.path.getsize(filepath_to_read) == 0: chunks = [f"```File '{filepath_to_read}' is empty.```"]
        elif os.path.getsize(filepath_to_read) < record_size_expected :
            chunks = [f"```File '{basename}' too small ({os.path.getsize(filepath_to_read)}B < {record_size_expected}B for initial record/header).```"]
        else:
            lines.append(f"Reading: {basename}")
            if is_activity_file: lines.append(f"Format: EventType(B), Timestamp(I), Title(H+str), Game(H+str), Tags(H+list[H+str]), [Opt: YouTubeVideoID(H+str)]\n... and other event types.\n")
            elif is_bot_session_file: lines.append(f"Record size: {record_size_expected}B\nFormat: EventType (B), Timestamp (I)\n")
            elif is_duration_file: lines.append(f"Record size: {record_size_expected}B\nFormat: Start_TS (I), End_TS (I)\n")
            else: lines.append(f"Record size: {record_size_expected}B\nFormat: Timestamp (I), {data_type_name} Count (I)\n")
            read_c, disp_c = 0, 0
            try:
                with open(filepath_to_read, 'rb') as f:
                    file_total_size = os.fstat(f.fileno()).st_size
                    while True:
                        if disp_c >= max_r: break
                        current_event_start_offset = f.tell()
                        if is_activity_file:
                            if current_event_start_offset + SA_BASE_HEADER_SIZE > file_total_size: break
                            header_chunk = f.read(SA_BASE_HEADER_SIZE);
                            if not header_chunk: break; read_c += 1
                            event_type, unix_ts = struct.unpack(SA_BASE_HEADER_FORMAT, header_chunk)
                            dt_obj = datetime.fromtimestamp(unix_ts, tz=timezone.utc); line_prefix = f"{dt_obj.isoformat()} ({unix_ts}) | Evt: {event_type} "
                            event_desc = ""; incomplete_body = False
                            try:
                                if event_type == EVENT_TYPE_STREAM_START:
                                    title, i1 = _read_string_from_file_handle_sync(f); game, i2 = _read_string_from_file_handle_sync(f); tags, i3 = _read_tag_list_from_file_handle_sync(f)
                                    yt_id_str = ""; inc4_yt_field_present_and_incomplete = False
                                    pos_before_yt = f.tell()
                                    if file_total_size - pos_before_yt >= SA_STRING_LEN_SIZE:
                                        peek_len_bytes = f.read(SA_STRING_LEN_SIZE)
                                        peek_s_len = struct.unpack(SA_STRING_LEN_FORMAT, peek_len_bytes)[0]
                                        f.seek(pos_before_yt)
                                        if file_total_size - pos_before_yt >= SA_STRING_LEN_SIZE + peek_s_len:
                                            temp_yt_id, inc4_yt_attempt = _read_string_from_file_handle_sync(f)
                                            if not inc4_yt_attempt: yt_id_str = f" | YT_ID: '{temp_yt_id}'" if temp_yt_id else " | YT_ID: (empty)"
                                            else: inc4_yt_field_present_and_incomplete = True; f.seek(pos_before_yt) 
                                    if i1 or i2 or i3 or inc4_yt_field_present_and_incomplete : incomplete_body=True; event_desc = "INCOMPLETE START"
                                    else: event_desc = f"(START) | T: '{title}' | G: '{game}' | Tags: {tags if tags else '[]'}{yt_id_str}"
                                elif event_type == EVENT_TYPE_STREAM_END:
                                    d_bytes = f.read(SA_INT_SIZE*2)
                                    if len(d_bytes) < SA_INT_SIZE*2: incomplete_body=True; event_desc = "INCOMPLETE END"
                                    else: dur, peak = struct.unpack(f'>{SA_INT_FORMAT[1:]}{SA_INT_FORMAT[1:]}', d_bytes); event_desc = f"(END) | Dur: {format_duration_human(dur)} | PeakV: {peak}"
                                elif event_type == EVENT_TYPE_GAME_CHANGE: old_g,i1=_read_string_from_file_handle_sync(f);new_g,i2=_read_string_from_file_handle_sync(f); event_desc = f"(GAME_CHG) | From: '{old_g}' | To: '{new_g}'" if not (i1 or i2) else "INCOMPLETE GAME_CHG"; incomplete_body = i1 or i2
                                elif event_type == EVENT_TYPE_TITLE_CHANGE: old_t,i1=_read_string_from_file_handle_sync(f);new_t,i2=_read_string_from_file_handle_sync(f); event_desc = f"(TITLE_CHG) | From: '{old_t}' | To: '{new_t}'" if not (i1 or i2) else "INCOMPLETE TITLE_CHG"; incomplete_body = i1 or i2
                                elif event_type == EVENT_TYPE_TAGS_CHANGE: old_tags,i1 = _read_tag_list_from_file_handle_sync(f); new_tags,i2 = _read_tag_list_from_file_handle_sync(f); event_desc = f"(TAGS_CHG) | Old: {old_tags if old_tags else '[]'} | New: {new_tags if new_tags else '[]'}" if not (i1 or i2) else "INCOMPLETE TAGS_CHG"; incomplete_body = i1 or i2
                                else: event_desc = f"Unknown Evt ({event_type})."; incomplete_body = _consume_activity_event_body_sync(f, event_type)
                                lines.append(f"{line_prefix}{event_desc}");
                                if incomplete_body : f.seek(current_event_start_offset); logger.warning(f"Incomplete activity event {event_type} at offset {current_event_start_offset}, stopping read."); break
                            except struct.error as se_inner: lines.append(f"{line_prefix}Struct error body: {se_inner}"); f.seek(current_event_start_offset); break
                            except Exception as e_inner: lines.append(f"{line_prefix}Error body: {e_inner}"); f.seek(current_event_start_offset); break
                        elif is_bot_session_file:
                            if current_event_start_offset + BOT_SESSION_RECORD_SIZE > file_total_size: break
                            chunk = f.read(BOT_SESSION_RECORD_SIZE);
                            if not chunk: break; read_c += 1
                            event_type, unix_ts = struct.unpack(BOT_SESSION_RECORD_FORMAT, chunk)
                            dt_obj = datetime.fromtimestamp(unix_ts, tz=timezone.utc)
                            event_name_str = "START" if event_type == BOT_EVENT_START else "STOP" if event_type == BOT_EVENT_STOP else f"Unknown ({event_type})"
                            lines.append(f"{dt_obj.isoformat()} ({unix_ts}) | Bot Event: {event_name_str}")
                        else: # Followers, Viewers, Durations
                            if current_event_start_offset + record_size_expected > file_total_size: break
                            chunk = f.read(record_size_expected);
                            if not chunk: break; read_c += 1
                            if is_duration_file:
                                start_ts, end_ts = struct.unpack(record_format_expected, chunk)
                                s_dt, e_dt = datetime.fromtimestamp(start_ts, tz=timezone.utc), datetime.fromtimestamp(end_ts, tz=timezone.utc)
                                lines.append(f"Start: {s_dt.isoformat()} ({start_ts}) | End: {e_dt.isoformat()} ({end_ts}) | Dur: {format_duration_human(end_ts - start_ts)}")
                            else:
                                unix_ts, count = struct.unpack(record_format_expected, chunk)
                                dt_obj = datetime.fromtimestamp(unix_ts, tz=timezone.utc)
                                lines.append(f"{dt_obj.isoformat()} ({unix_ts}) | {data_type_name}s: {count}")
                        disp_c += 1
                if not is_activity_file and not is_bot_session_file:
                    try:
                        total_recs = file_total_size // record_size_expected if record_size_expected > 0 else 0
                        if disp_c < read_c or (total_recs > 0 and read_c < total_recs):
                            lines.append(f"\nDisplaying {disp_c} of {read_c} records read.")
                            if total_recs > 0 and read_c < total_recs: lines.append(f"(File has ~{total_recs} full records)")
                    except ZeroDivisionError: lines.append(f"\nDisplaying {disp_c} of {read_c} records read.")
                else: lines.append(f"\nDisplayed {disp_c} of {read_c} records read from {data_type_name.lower()} log.")
                lines.append(f"\nTotal records displayed: {disp_c}.")
            except FileNotFoundError: chunks = [f"```Error: File '{filepath_to_read}' not found during read.```"]
            except struct.error as se: logger.error(f"Struct unpacking error: {se}", exc_info=True); lines.append(f"\nError: Struct unpack failed. ({se})")
            except Exception as e: logger.error(f"Error processing: {e}", exc_info=True); lines.append(f"\nError: {str(e)}")
            if not chunks :
                cur_chunks, cur_chunk_str = [], ""
                for line_item in lines:
                    if not cur_chunk_str: cur_chunk_str = line_item + "\n"; continue
                    if len(cur_chunk_str) + len(line_item) + 1 > (1990-8): # Discord message limit
                        if cur_chunk_str.strip(): cur_chunks.append(f"```\n{cur_chunk_str.strip()}\n```")
                        cur_chunk_str = line_item + "\n"
                    else: cur_chunk_str += line_item + "\n"
                if cur_chunk_str.strip(): cur_chunks.append(f"```\n{cur_chunk_str.strip()}\n```")
                chunks = cur_chunks if cur_chunks else ["```No data or unexpected empty.```"]
    if not chunks: await ctx.send(f"No {data_type_name.lower()} data or error. Check logs."); return
    for i, chunk_msg in enumerate(chunks):
        if i > 0: await asyncio.sleep(0.5)
        await ctx.send(chunk_msg)

@bot.command(name="daystats", help="Follower & stream stats on a date (YYYY-MM-DD) with optional graph.")
async def day_stats_command(ctx: commands.Context, date_str: str = None):
    if FCTD_COMMAND_CHANNEL_ID is not None and ctx.channel.id != FCTD_COMMAND_CHANNEL_ID: return
    target_twitch_user = FCTD_TWITCH_USERNAME; uta_target_user = UTA_TWITCH_CHANNEL_NAME if UTA_ENABLED else None
    if not target_twitch_user and not (uta_target_user and (UTA_RESTREAMER_ENABLED or UTA_STREAM_STATUS_NOTIFICATIONS_ENABLED) ): await ctx.send("No Twitch user configured for follower or UTA stream stats."); return
    if date_str is None: await ctx.send(f"Provide date as YYYY-MM-DD. Ex: `{FCTD_COMMAND_PREFIX}daystats 2023-10-26`"); return
    try: target_date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError: await ctx.send("Invalid date format. Use YYYY-MM-DD."); return
    if target_date_obj > (datetime.now(timezone.utc).date() + timedelta(days=1)): await ctx.send("Cannot query stats for dates far in future."); return
    embed = discord.Embed(title=f"Twitch Stats for {target_date_obj.isoformat()}", color=discord.Color.blue())
    discord_file_to_send = None
    day_start_unix = int(datetime.combine(target_date_obj, datetime.min.time(), tzinfo=timezone.utc).timestamp())
    day_end_unix = int(datetime.combine(target_date_obj, datetime.max.time(), tzinfo=timezone.utc).timestamp())
    async with ctx.typing():
        if target_twitch_user and FCTD_FOLLOWER_DATA_FILE:
            res_foll = await asyncio.to_thread(_get_counts_for_day_boundaries_sync, FCTD_FOLLOWER_DATA_FILE, target_date_obj)
            if isinstance(res_foll, str): embed.add_field(name=f"Followers ({target_twitch_user})", value=res_foll, inline=False)
            else:
                sc, ec, sts, ets, nrd_f = res_foll['start_count'], res_foll['end_count'], res_foll['start_ts'], res_foll['end_ts'], res_foll['num_records_on_day']
                records_for_plot = res_foll.get('records_for_plot', [])
                gain = ec - sc; sdt, edt = datetime.fromtimestamp(sts, tz=timezone.utc), datetime.fromtimestamp(ets, tz=timezone.utc)
                act = f"gained {gain:,}" if gain>0 else f"lost {abs(gain):,}" if gain<0 else "no net change in"
                foll_desc = f"{target_twitch_user} {act} followers.\nInitial: {sc:,} ({discord.utils.format_dt(sdt,'R')})\nFinal: {ec:,} ({discord.utils.format_dt(edt,'R')})"
                f_notes = []
                if sdt.date() < target_date_obj: f_notes.append("Initial from prior day.")
                if ets == sts and nrd_f == 0 : f_notes.append("Count stable (no new data on day).")
                elif nrd_f == 0 and edt.date() < target_date_obj: f_notes.append("Final from prior day.")
                elif nrd_f > 0: f_notes.append(f"{nrd_f} data point(s) on day.")
                if f_notes: foll_desc += f"\n*({' | '.join(f_notes)})*"
                embed.add_field(name=f"Followers ({target_twitch_user})", value=foll_desc, inline=False)
                if MATPLOTLIB_AVAILABLE and records_for_plot and len(records_for_plot) > 1:
                    try:
                        plot_times = [datetime.fromtimestamp(r['ts'], tz=timezone.utc) for r in records_for_plot]; plot_counts = [r['count'] for r in records_for_plot]
                        fig, ax = plt.subplots(figsize=(8, 3)); ax.plot(plot_times, plot_counts, marker='.', linestyle='-', markersize=5, color='lightgreen')
                        ax.set_title(f"Follower Trend on {target_date_obj.isoformat()}", fontsize=10)
                        ax.set_xlabel("Time (UTC)", fontsize=8); ax.set_ylabel("Followers", fontsize=8)
                        ax.grid(True, linestyle=':', alpha=0.5); ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
                        ax.tick_params(axis='x', rotation=20, labelsize=7); ax.tick_params(axis='y', labelsize=7)
                        fig.patch.set_alpha(0); ax.set_facecolor('#2C2F33')
                        ax.spines['top'].set_visible(False); ax.spines['right'].set_visible(False); ax.spines['bottom'].set_color('grey'); ax.spines['left'].set_color('grey')
                        ax.tick_params(colors='lightgrey'); ax.yaxis.label.set_color('lightgrey'); ax.xaxis.label.set_color('lightgrey'); ax.title.set_color('white')
                        plt.tight_layout(); img_bytes = io.BytesIO()
                        fig.savefig(img_bytes, format='png', bbox_inches='tight', facecolor=fig.get_facecolor())
                        img_bytes.seek(0); plt.close(fig)
                        plot_filename = f"daystats_foll_{target_twitch_user}_{target_date_obj.isoformat()}.png"
                        discord_file_to_send = discord.File(fp=img_bytes, filename=plot_filename)
                        embed.set_image(url=f"attachment://{plot_filename}")
                    except Exception as e_plot: logger.error(f"Error generating daystats follower plot: {e_plot}", exc_info=True)
        elif target_twitch_user: embed.add_field(name=f"Followers ({target_twitch_user})", value="Follower data file not configured.", inline=False)
        if uta_target_user and UTA_STREAM_ACTIVITY_LOG_FILE and UTA_STREAM_STATUS_NOTIFICATIONS_ENABLED:
            game_segments = await asyncio.to_thread(_parse_stream_activity_for_game_segments_sync, UTA_STREAM_ACTIVITY_LOG_FILE, day_start_unix, day_end_unix)
            total_stream_time_on_day = sum(seg['end_ts'] - seg['start_ts'] for seg in game_segments); num_distinct_streams = 0
            if game_segments:
                game_segments.sort(key=lambda s: s['start_ts']); num_distinct_streams = 1
                for i in range(1, len(game_segments)):
                    if game_segments[i]['start_ts'] - game_segments[i-1]['end_ts'] > 300: num_distinct_streams +=1
            if total_stream_time_on_day > 0: embed.add_field(name=f"Total Stream Time ({uta_target_user})", value=f"Streamed for **{format_duration_human(total_stream_time_on_day)}** across {num_distinct_streams} session(s).", inline=False)
            else: embed.add_field(name=f"Total Stream Time ({uta_target_user})", value="No streams logged via UTA activity log on this day.", inline=False)
        elif uta_target_user: embed.add_field(name=f"Total Stream Time ({uta_target_user})", value="UTA Stream Activity log not configured/enabled.", inline=False)
        if uta_target_user and UTA_VIEWER_COUNT_LOGGING_ENABLED and UTA_VIEWER_COUNT_LOG_FILE:
            avg_v, peak_v_day, num_dp = await asyncio.to_thread(
                 _get_viewer_stats_for_period_sync, UTA_VIEWER_COUNT_LOG_FILE, day_start_unix, day_end_unix)
            if avg_v is not None: embed.add_field(name=f"Avg Viewers ({uta_target_user})", value=f"{avg_v:,.1f} (from {num_dp} data points)\nPeak on day: {peak_v_day:,}", inline=False)
            else: embed.add_field(name=f"Avg Viewers ({uta_target_user})", value="No viewer data for this day.", inline=False)
        elif uta_target_user : embed.add_field(name=f"Avg Viewers ({uta_target_user})", value="Viewer logging disabled or file not found.", inline=False)
    if not embed.fields and not discord_file_to_send: embed.description = "No data to display or plot. Check configuration and data files."
    await ctx.send(embed=embed, file=discord_file_to_send if discord_file_to_send else None)

@bot.command(name="streamtime", help="Total stream time for UTA_TWITCH_CHANNEL_NAME over a period (from restream logs). Usage: !streamtime <period>")
async def stream_time_command(ctx: commands.Context, *, duration_input: str = None):
    if FCTD_COMMAND_CHANNEL_ID is not None and ctx.channel.id != FCTD_COMMAND_CHANNEL_ID: return
    if not UTA_ENABLED or not UTA_RESTREAMER_ENABLED or not UTA_TWITCH_CHANNEL_NAME or not UTA_STREAM_DURATION_LOG_FILE:
        await ctx.send("UTA Restreamer, target channel, or stream duration log not configured/enabled for this command (uses restream log)."); return
    if duration_input is None: await ctx.send(f"Provide duration (e.g., `7d`, `1mo`). Ex: `{FCTD_COMMAND_PREFIX}streamtime 7d`"); return
    time_delta, period_name = parse_duration_to_timedelta(duration_input)
    if not time_delta: await ctx.send(period_name); return
    now_utc = datetime.now(timezone.utc); period_end_utc, period_start_utc = now_utc, now_utc - time_delta
    period_start_unix, period_end_unix = int(period_start_utc.timestamp()), int(period_end_utc.timestamp())
    async with ctx.typing(): total_duration_seconds, num_streams = await asyncio.to_thread(_read_stream_durations_for_period_sync, UTA_STREAM_DURATION_LOG_FILE, period_start_unix, period_end_unix)
    human_readable_duration = format_duration_human(total_duration_seconds)
    embed = discord.Embed(title=f"Restream Time for {UTA_TWITCH_CHANNEL_NAME} ({period_name})", description=f"{UTA_TWITCH_CHANNEL_NAME} restreamed for **{human_readable_duration}** across {num_streams} session(s) in the {period_name}.", color=discord.Color.purple() if num_streams > 0 else discord.Color.light_grey())
    embed.set_footer(text=f"Query period: {discord.utils.format_dt(period_start_utc)} to {discord.utils.format_dt(period_end_utc)}")
    await ctx.send(embed=embed)

@bot.command(name="twitchinfo", aliases=['tinfo'], help="Shows public info for a Twitch channel. Usage: !twitchinfo [username]")
async def twitch_info_command(ctx: commands.Context, twitch_username_to_check: str = None):
    if FCTD_COMMAND_CHANNEL_ID is not None and ctx.channel.id != FCTD_COMMAND_CHANNEL_ID: return
    if not twitch_username_to_check:
        if UTA_ENABLED and UTA_TWITCH_CHANNEL_NAME: twitch_username_to_check = UTA_TWITCH_CHANNEL_NAME
        else: await ctx.send(f"Please specify a Twitch username or configure `UTA_TWITCH_CHANNEL_NAME`. Usage: `{FCTD_COMMAND_PREFIX}twitchinfo <username>`"); return
    async with ctx.typing():
        user_data_response = await asyncio.to_thread(_uta_make_twitch_api_request, "/users", params={"login": twitch_username_to_check})
        if not user_data_response or not user_data_response.get("data"): await ctx.send(f"Could not find Twitch user: `{twitch_username_to_check}`."); return
        user_info = user_data_response["data"][0]; broadcaster_id = user_info["id"]
        channel_task = asyncio.to_thread(_uta_make_twitch_api_request, "/channels", params={"broadcaster_id": broadcaster_id})
        stream_task = asyncio.to_thread(_uta_make_twitch_api_request, "/streams", params={"user_id": broadcaster_id})
        followers_task = asyncio.to_thread(_uta_make_twitch_api_request, "/channels/followers", params={"broadcaster_id": broadcaster_id})
        channel_data_response, stream_data_response, followers_data_response = await asyncio.gather(channel_task, stream_task, followers_task)
    channel_info = channel_data_response.get("data", [{}])[0] if channel_data_response and channel_data_response.get("data") else {}
    stream_info = stream_data_response.get("data", [{}])[0] if stream_data_response and stream_data_response.get("data") else {}
    follower_count = followers_data_response.get("total", 0) if followers_data_response else 0
    embed = discord.Embed(title=f"Twitch Info: {user_info.get('display_name', twitch_username_to_check)}", color=discord.Color.purple())
    if user_info.get("profile_image_url"): embed.set_thumbnail(url=user_info.get("profile_image_url"))
    description = user_info.get("description")
    if description: embed.description = description[:250] + "..." if len(description) > 250 else description
    embed.add_field(name="Followers", value=f"{follower_count:,}", inline=True)
    embed.add_field(name="Total Views", value=f"{user_info.get('view_count', 0):,}", inline=True)
    if user_info.get("created_at"):
        created_dt = datetime.fromisoformat(user_info.get("created_at").replace('Z', '+00:00'))
        embed.add_field(name="Created At", value=discord.utils.format_dt(created_dt, 'D'), inline=True)
    is_live = stream_info and stream_info.get("type") == "live"
    if is_live:
        live_title = stream_info.get("title", "N/A"); live_game = stream_info.get("game_name", "N/A"); viewers = stream_info.get("viewer_count", 0)
        started_at_str = stream_info.get("started_at"); uptime_str = "N/A"
        if started_at_str:
            started_dt = datetime.fromisoformat(started_at_str.replace('Z', '+00:00'))
            uptime_delta = datetime.now(timezone.utc) - started_dt; uptime_str = format_duration_human(int(uptime_delta.total_seconds()))
        live_details = f"**Title:** {live_title}\n**Game:** {live_game}\n**Viewers:** {viewers:,}\n**Uptime:** {uptime_str}"
        embed.add_field(name="🔴 LIVE NOW", value=live_details, inline=False)
    else:
        embed.add_field(name="Status", value="Offline", inline=False)
        if channel_info.get("title"): embed.add_field(name="Last Title", value=channel_info.get("title"), inline=True)
        if channel_info.get("game_name"): embed.add_field(name="Last Game", value=channel_info.get("game_name"), inline=True)
    if channel_info.get("broadcaster_language"): embed.add_field(name="Language", value=channel_info.get("broadcaster_language"), inline=True)
    current_tags_to_display = stream_info.get("tags", []) if is_live else (channel_info.get("tags", []) if channel_info else [])
    if current_tags_to_display:
        tags_str = ", ".join(current_tags_to_display[:8]) + ("..." if len(current_tags_to_display) > 8 else "")
        embed.add_field(name="Tags", value=tags_str if tags_str else "None", inline=False)
    embed.url = f"https://twitch.tv/{twitch_username_to_check}"; embed.set_footer(text=f"ID: {broadcaster_id} | Data fetched at"); embed.timestamp = datetime.now(timezone.utc)
    await ctx.send(embed=embed)

@bot.command(name="gamestats", help="Game stats with optional viewer histogram. Usage: !gamestats \"<Game Name>\" [period]")
async def game_stats_command(ctx: commands.Context, game_name_input: str, *, duration_input: str = None):
    if FCTD_COMMAND_CHANNEL_ID is not None and ctx.channel.id != FCTD_COMMAND_CHANNEL_ID: return
    if not UTA_ENABLED or not UTA_TWITCH_CHANNEL_NAME or not UTA_STREAM_ACTIVITY_LOG_FILE:
        await ctx.send("UTA, target Twitch channel, or stream activity log not configured/enabled for game stats."); return
    target_game_name = game_name_input.strip()
    if not target_game_name: await ctx.send("Please provide a game name. Usage: `!gamestats \"Exact Game Name\" [period]`"); return
    query_start_unix, query_end_unix, period_name_display = None, None, "all time"
    if duration_input:
        time_delta, period_name = parse_duration_to_timedelta(duration_input)
        if not time_delta: await ctx.send(period_name); return
        now_utc = datetime.now(timezone.utc); query_end_unix = int(now_utc.timestamp()); query_start_unix = int((now_utc - time_delta).timestamp())
        period_name_display = period_name
    discord_file_to_send = None
    async with ctx.typing():
        game_segments = await asyncio.to_thread(_parse_stream_activity_for_game_segments_sync, UTA_STREAM_ACTIVITY_LOG_FILE, query_start_unix, query_end_unix)
        target_game_segments = [seg for seg in game_segments if seg['game'].lower() == target_game_name.lower()]
        if not target_game_segments: await ctx.send(f"No streaming data found for game '{target_game_name}' in {period_name_display}. Ensure exact game name from Twitch."); return
        total_time_streamed_sec = sum(seg['end_ts'] - seg['start_ts'] for seg in target_game_segments)
        avg_viewers_for_game, total_follower_gain_for_game = None, None; sessions_with_follower_data = 0
        viewer_counts_for_plot = []; total_viewer_datapoints_for_game = 0
        if UTA_VIEWER_COUNT_LOGGING_ENABLED and UTA_VIEWER_COUNT_LOG_FILE and os.path.exists(UTA_VIEWER_COUNT_LOG_FILE):
            all_viewer_records_in_period = []
            min_seg_start = min(s['start_ts'] for s in target_game_segments) if target_game_segments else 0
            max_seg_end = max(s['end_ts'] for s in target_game_segments) if target_game_segments else 0
            if min_seg_start < max_seg_end :
                try:
                    with open(UTA_VIEWER_COUNT_LOG_FILE, 'rb') as vf:
                        while True:
                            chunk = vf.read(BINARY_RECORD_SIZE)
                            if not chunk: break
                            if len(chunk) < BINARY_RECORD_SIZE: break
                            ts, count = struct.unpack(BINARY_RECORD_FORMAT, chunk)
                            if min_seg_start <= ts < max_seg_end: all_viewer_records_in_period.append({'ts': ts, 'count': count})
                except Exception as e: logger.error(f"Error reading viewer log for gamestats: {e}")
            if all_viewer_records_in_period:
                for seg in target_game_segments:
                    for vr in all_viewer_records_in_period:
                        if seg['start_ts'] <= vr['ts'] < seg['end_ts']: viewer_counts_for_plot.append(vr['count'])
                if viewer_counts_for_plot:
                    avg_viewers_for_game = sum(viewer_counts_for_plot) / len(viewer_counts_for_plot)
                    total_viewer_datapoints_for_game = len(viewer_counts_for_plot)
        if FCTD_FOLLOWER_DATA_FILE and os.path.exists(FCTD_FOLLOWER_DATA_FILE) and FCTD_TWITCH_USERNAME == UTA_TWITCH_CHANNEL_NAME:
            current_total_gain = 0
            for seg in target_game_segments:
                s_foll, e_foll, _, _, _ = await asyncio.to_thread(_read_and_find_records_sync, FCTD_FOLLOWER_DATA_FILE, seg['start_ts'], seg['end_ts'])
                if s_foll is not None and e_foll is not None: current_total_gain += (e_foll - s_foll); sessions_with_follower_data +=1
            total_follower_gain_for_game = current_total_gain
        embed = discord.Embed(title=f"Game Stats for: {target_game_name}", description=f"Channel: {UTA_TWITCH_CHANNEL_NAME}\nPeriod: {period_name_display}", color=discord.Color.blue())
        embed.add_field(name="Total Time Streamed", value=format_duration_human(total_time_streamed_sec), inline=False)
        if avg_viewers_for_game is not None: embed.add_field(name="Average Viewers", value=f"{avg_viewers_for_game:,.0f} (from {total_viewer_datapoints_for_game} data points)" if total_viewer_datapoints_for_game > 0 else "No viewer data during these game sessions.", inline=True)
        else: embed.add_field(name="Average Viewers", value="Viewer count logging not enabled or no data.", inline=True)
        if total_follower_gain_for_game is not None:
            gain_str = f"{total_follower_gain_for_game:+,}" if total_follower_gain_for_game != 0 else "0"
            embed.add_field(name="Follower Gain During Game", value=f"{gain_str} followers ({sessions_with_follower_data} sessions w/ data)", inline=True)
        elif FCTD_TWITCH_USERNAME == UTA_TWITCH_CHANNEL_NAME : embed.add_field(name="Follower Gain During Game", value="Follower count logging not enabled or no data.", inline=True)
        embed.set_footer(text=f"{len(target_game_segments)} play session(s) found for '{target_game_name}'.")
        if MATPLOTLIB_AVAILABLE and viewer_counts_for_plot and len(viewer_counts_for_plot) > 1:
            try:
                fig, ax = plt.subplots(figsize=(8, 4)); ax.hist(viewer_counts_for_plot, bins=15, edgecolor='black', color='skyblue')
                ax.set_title(f"Viewer Distribution for '{target_game_name}'", fontsize=10)
                ax.set_xlabel("Viewer Count", fontsize=9); ax.set_ylabel("Frequency (Data Points)", fontsize=9)
                ax.grid(True, linestyle=':', alpha=0.5, axis='y'); ax.tick_params(labelsize=8)
                fig.patch.set_alpha(0); ax.set_facecolor('#2C2F33')
                ax.spines['top'].set_visible(False); ax.spines['right'].set_visible(False); ax.spines['bottom'].set_color('grey'); ax.spines['left'].set_color('grey')
                ax.tick_params(colors='lightgrey'); ax.yaxis.label.set_color('lightgrey'); ax.xaxis.label.set_color('lightgrey'); ax.title.set_color('white')
                plt.tight_layout(); img_bytes = io.BytesIO(); fig.savefig(img_bytes, format='png', bbox_inches='tight', facecolor=fig.get_facecolor())
                img_bytes.seek(0); plt.close(fig)
                plot_filename = f"gamestats_viewers_{target_game_name.replace(' ','_')}_{datetime.now().strftime('%Y%m%d%H%M')}.png"
                discord_file_to_send = discord.File(fp=img_bytes, filename=plot_filename)
                embed.set_image(url=f"attachment://{plot_filename}")
            except Exception as e_plot: logger.error(f"Error generating gamestats viewer plot: {e_plot}", exc_info=True)
    await ctx.send(embed=embed, file=discord_file_to_send if discord_file_to_send else None)

@bot.command(name="exportdata", help="Exports data to CSV. Usage: !exportdata <type> [period|all]. Owner only.")
@commands.is_owner()
async def export_data_command(ctx: commands.Context, data_type: str, period_input: str = "all"):
    if FCTD_COMMAND_CHANNEL_ID is not None and ctx.channel.id != FCTD_COMMAND_CHANNEL_ID: return
    data_type_lower = data_type.lower(); filepath_to_export, record_format, record_size = None, None, None
    is_activity, is_bot_session = False, False; headers, data_name_for_file = [], "export"
    if data_type_lower in ["followers", "foll"]: filepath_to_export = FCTD_FOLLOWER_DATA_FILE; record_format, record_size = BINARY_RECORD_FORMAT, BINARY_RECORD_SIZE; headers = ["Timestamp", "DateTimeUTC", "FollowerCount"]; data_name_for_file = "followers"
    elif data_type_lower in ["viewers", "views"]: filepath_to_export = UTA_VIEWER_COUNT_LOG_FILE; record_format, record_size = BINARY_RECORD_FORMAT, BINARY_RECORD_SIZE; headers = ["Timestamp", "DateTimeUTC", "ViewerCount"]; data_name_for_file = "viewers"
    elif data_type_lower in ["durations", "streamdurations"]: filepath_to_export = UTA_STREAM_DURATION_LOG_FILE; record_format, record_size = STREAM_DURATION_RECORD_FORMAT, STREAM_DURATION_RECORD_SIZE; headers = ["StartTimestamp", "StartDateTimeUTC", "EndTimestamp", "EndDateTimeUTC", "DurationSeconds"]; data_name_for_file = "stream_durations"
    elif data_type_lower in ["activity", "streamactivity"]: filepath_to_export = UTA_STREAM_ACTIVITY_LOG_FILE; is_activity = True; record_size = SA_BASE_HEADER_SIZE; headers = ["Timestamp", "DateTimeUTC", "EventType", "Arg1_Title_OldGame_OldTitle_OldTags", "Arg2_Game_NewGame_NewTitle_NewTags", "Arg3_Tags_PeakViewers_YTVideoID", "Arg4_YouTubeVideoID_If_StartEvent"]; data_name_for_file = "stream_activity" # Adjusted headers
    elif data_type_lower in ["sessions", "botsessions"]: filepath_to_export = BOT_SESSION_LOG_FILE_PATH; is_bot_session = True; record_format, record_size = BOT_SESSION_RECORD_FORMAT, BOT_SESSION_RECORD_SIZE; headers = ["Timestamp", "DateTimeUTC", "EventType", "EventName"]; data_name_for_file = "bot_sessions"
    else: await ctx.send("Invalid data type. Choose: `followers`, `viewers`, `durations`, `activity`, `sessions`."); return
    if not filepath_to_export: await ctx.send(f"File path for '{data_type_lower}' not configured."); return
    if not os.path.exists(filepath_to_export) or os.path.getsize(filepath_to_export) == 0: await ctx.send(f"Data file '{os.path.basename(filepath_to_export)}' not found or is empty."); return
    query_start_unix, query_end_unix = None, None
    if period_input.lower() != "all":
        delta, period_name_p = parse_duration_to_timedelta(period_input)
        if not delta: await ctx.send(f"Invalid period format: '{period_input}'. Error: {period_name_p}"); return
        now_utc = datetime.now(timezone.utc); query_start_unix = int((now_utc - delta).timestamp()); query_end_unix = int(now_utc.timestamp())
    await ctx.send(f"Processing `{data_name_for_file}` data for export... This might take a moment.")
    async with ctx.typing():
        csv_rows = [headers]
        try:
            with open(filepath_to_export, 'rb') as f:
                file_total_size = os.fstat(f.fileno()).st_size
                while True:
                    row_data_values = []
                    current_event_start_offset = f.tell()
                    if is_activity:
                        if current_event_start_offset + SA_BASE_HEADER_SIZE > file_total_size: break
                        header_chunk = f.read(SA_BASE_HEADER_SIZE)
                        if not header_chunk: break
                        event_type, ts = struct.unpack(SA_BASE_HEADER_FORMAT, header_chunk)
                        dt_obj_iso = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
                        base_event_data = [ts, dt_obj_iso, event_type]; specific_event_data_list = ["", "", "", ""]; incomplete_event = False # Default 4 args
                        try:
                            if event_type == EVENT_TYPE_STREAM_START:
                                title,i1=_read_string_from_file_handle_sync(f); game,i2=_read_string_from_file_handle_sync(f); tags,i3=_read_tag_list_from_file_handle_sync(f)
                                yt_id = None; inc4_yt = False
                                pos_b_yt = f.tell()
                                if file_total_size - pos_b_yt >= SA_STRING_LEN_SIZE:
                                    peek_len_b = f.read(SA_STRING_LEN_SIZE); peek_s_l = struct.unpack(SA_STRING_LEN_FORMAT, peek_len_b)[0]; f.seek(pos_b_yt)
                                    if file_total_size - pos_b_yt >= SA_STRING_LEN_SIZE + peek_s_l: temp_yt, inc4_yt_att = _read_string_from_file_handle_sync(f); yt_id = temp_yt if not inc4_yt_att else None; inc4_yt=inc4_yt_att
                                    else: f.seek(pos_b_yt) # Not enough for string
                                else: f.seek(pos_b_yt) # Not enough for len
                                if i1 or i2 or i3 or inc4_yt : incomplete_event=True
                                else: specific_event_data_list = [title, game, ",".join(tags), yt_id or ""]
                            elif event_type == EVENT_TYPE_STREAM_END: d_bytes = f.read(SA_INT_SIZE*2); dur, peak = struct.unpack(f'>{SA_INT_FORMAT[1:]}{SA_INT_FORMAT[1:]}', d_bytes) if len(d_bytes)==SA_INT_SIZE*2 else (0,0); specific_event_data_list = [dur, peak, "", ""]; incomplete_event = len(d_bytes) < SA_INT_SIZE*2
                            elif event_type == EVENT_TYPE_GAME_CHANGE: old_g,i1=_read_string_from_file_handle_sync(f);new_g,i2=_read_string_from_file_handle_sync(f); specific_event_data_list = [old_g, new_g, "", ""] if not (i1 or i2) else ["","","",""]; incomplete_event = i1 or i2
                            elif event_type == EVENT_TYPE_TITLE_CHANGE: old_t,i1=_read_string_from_file_handle_sync(f);new_t,i2=_read_string_from_file_handle_sync(f); specific_event_data_list = [old_t, new_t, "", ""] if not (i1 or i2) else ["","","",""]; incomplete_event = i1 or i2
                            elif event_type == EVENT_TYPE_TAGS_CHANGE: old_tags,i1 = _read_tag_list_from_file_handle_sync(f); new_tags,i2 = _read_tag_list_from_file_handle_sync(f); specific_event_data_list = [",".join(old_tags), ",".join(new_tags), "", ""] if not (i1 or i2) else ["","","",""]; incomplete_event = i1 or i2
                            else: incomplete_event = _consume_activity_event_body_sync(f, event_type); specific_event_data_list = [f"Unknown Event Type {event_type}", "", "", ""]
                        except Exception as e_parse_body: logger.error(f"Export: Error parsing activity body type {event_type} at {ts}: {e_parse_body}"); incomplete_event = True; specific_event_data_list=["PARSE_ERROR_BODY","","",""]
                        if incomplete_event: f.seek(current_event_start_offset); logger.warning(f"Export: Incomplete activity event {event_type} at offset {current_event_start_offset}, stopping."); break
                        if (query_start_unix and ts < query_start_unix) or (query_end_unix and ts > query_end_unix): continue
                        row_data_values = base_event_data + specific_event_data_list
                    # ... (rest of the elif/else for other data types - unchanged) ...
            # ... (rest of the export logic - unchanged) ...
        except FileNotFoundError: await ctx.send(f"Error: File '{filepath_to_export}' not found during export.")
        except struct.error as e: await ctx.send(f"Error processing binary data: {e}. File might be corrupt or have unexpected format.")
        except Exception as e: logger.error(f"Error during data export for {data_type_lower}: {e}", exc_info=True); await ctx.send(f"An unexpected error occurred: {e}")


@bot.command(name="plotfollowers", help="Plots follower count over time. Usage: !plotfollowers <period>. Owner only.")
@commands.is_owner()
async def plot_followers_command(ctx: commands.Context, *, duration_input: str = None):
    if FCTD_COMMAND_CHANNEL_ID is not None and ctx.channel.id != FCTD_COMMAND_CHANNEL_ID: return
    if not MATPLOTLIB_AVAILABLE: await ctx.send("Plotting library (matplotlib) not installed."); return
    if not FCTD_FOLLOWER_DATA_FILE or not os.path.exists(FCTD_FOLLOWER_DATA_FILE): await ctx.send("Follower data file not found or not configured."); return
    period_name = "all time"; query_start_unix = None; now_utc_unix = int(datetime.now(timezone.utc).timestamp())
    if duration_input:
        delta, period_name_parsed = parse_duration_to_timedelta(duration_input)
        if not delta: await ctx.send(period_name_parsed); return
        query_start_unix = int((datetime.now(timezone.utc) - delta).timestamp()); period_name = period_name_parsed
    await ctx.send(f"Generating follower plot for {FCTD_TWITCH_USERNAME or 'configured user'} ({period_name})...")
    async with ctx.typing():
        timestamps, counts = [], []
        try:
            with open(FCTD_FOLLOWER_DATA_FILE, 'rb') as f:
                while True:
                    chunk = f.read(BINARY_RECORD_SIZE)
                    if not chunk: break
                    if len(chunk) < BINARY_RECORD_SIZE: break
                    ts, count = struct.unpack(BINARY_RECORD_FORMAT, chunk)
                    if query_start_unix and ts < query_start_unix: continue
                    if ts > now_utc_unix + 3600 : continue # Ignore data far in future
                    timestamps.append(datetime.fromtimestamp(ts, tz=timezone.utc)); counts.append(count)
        except FileNotFoundError: await ctx.send(f"File {FCTD_FOLLOWER_DATA_FILE} not found."); return
        except Exception as e_read: await ctx.send(f"Error reading follower data: {e_read}"); return
        if not timestamps or len(timestamps) < 2: await ctx.send("Not enough follower data found for the specified period to plot."); return
        fig, ax = plt.subplots(figsize=(12, 6)); ax.plot(timestamps, counts, marker='.', linestyle='-', markersize=4, color='cyan')
        ax.set_title(f"Follower Count for {FCTD_TWITCH_USERNAME or 'User'} ({period_name})")
        ax.set_xlabel("Date/Time (UTC)"); ax.set_ylabel("Follower Count")
        ax.grid(True, linestyle=':', alpha=0.7); ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M'))
        plt.xticks(rotation=45, ha="right"); plt.tight_layout()
        await _send_plot_if_available(ctx, fig, f"followers_{FCTD_TWITCH_USERNAME or 'user'}")

@bot.command(name="plotstreamdurations", help="Plots histogram of stream durations. Usage: !plotstreamdurations <period>. Owner only.")
@commands.is_owner()
async def plot_stream_durations_command(ctx: commands.Context, *, duration_input: str = None):
    if FCTD_COMMAND_CHANNEL_ID is not None and ctx.channel.id != FCTD_COMMAND_CHANNEL_ID: return
    if not MATPLOTLIB_AVAILABLE: await ctx.send("Plotting library (matplotlib) not installed."); return
    target_file, data_source_name, is_activity_log = None, "", False
    if UTA_STREAM_ACTIVITY_LOG_FILE and os.path.exists(UTA_STREAM_ACTIVITY_LOG_FILE) and UTA_STREAM_STATUS_NOTIFICATIONS_ENABLED:
        target_file = UTA_STREAM_ACTIVITY_LOG_FILE; data_source_name = "Stream Activity Durations"; is_activity_log = True
    elif UTA_STREAM_DURATION_LOG_FILE and os.path.exists(UTA_STREAM_DURATION_LOG_FILE):
        target_file = UTA_STREAM_DURATION_LOG_FILE; data_source_name = "Restream Durations"
    else: await ctx.send(f"No suitable stream duration data file found (checked UTA Activity Log and Restream Log)."); return
    period_name = "all time"; query_start_unix, query_end_unix = None, None; now_utc = datetime.now(timezone.utc)
    if duration_input:
        delta, period_name_parsed = parse_duration_to_timedelta(duration_input)
        if not delta: await ctx.send(period_name_parsed); return
        query_start_unix = int((now_utc - delta).timestamp()); query_end_unix = int(now_utc.timestamp()); period_name = period_name_parsed
    await ctx.send(f"Generating {data_source_name} plot for {UTA_TWITCH_CHANNEL_NAME or 'configured channel'} ({period_name})...")
    async with ctx.typing():
        durations_hours = []
        try:
            if is_activity_log:
                active_stream_start_ts = None
                with open(target_file, 'rb') as f:
                    file_total_size = os.fstat(f.fileno()).st_size
                    while True:
                        current_event_start_offset = f.tell()
                        if current_event_start_offset + SA_BASE_HEADER_SIZE > file_total_size: break
                        header_chunk = f.read(SA_BASE_HEADER_SIZE);
                        if not header_chunk: break
                        event_type, ts = struct.unpack(SA_BASE_HEADER_FORMAT, header_chunk); incomplete_body = False
                        if event_type != EVENT_TYPE_STREAM_START and event_type != EVENT_TYPE_STREAM_END:
                            if (query_start_unix and ts < query_start_unix - (86400*14)) or \
                               (query_end_unix and ts > query_end_unix + (86400*1)):
                                incomplete_body = _consume_activity_event_body_sync(f, event_type)
                                if incomplete_body: break; continue
                        if event_type == EVENT_TYPE_STREAM_START:
                            if (query_end_unix and ts < query_end_unix + 86400) or not query_end_unix: active_stream_start_ts = ts
                            incomplete_body = _consume_activity_event_body_sync(f, event_type)
                        elif event_type == EVENT_TYPE_STREAM_END:
                            if active_stream_start_ts is not None:
                                stream_s = active_stream_start_ts; stream_e = ts; is_relevant = True
                                if query_start_unix and stream_e < query_start_unix : is_relevant = False
                                if query_end_unix and stream_s > query_end_unix : is_relevant = False
                                if is_relevant:
                                    eff_s = max(stream_s, query_start_unix) if query_start_unix else stream_s
                                    eff_e = min(stream_e, query_end_unix) if query_end_unix else stream_e
                                    if eff_e > eff_s: durations_hours.append((eff_e - eff_s) / 3600.0)
                            active_stream_start_ts = None; incomplete_body = _consume_activity_event_body_sync(f, event_type)
                        else: incomplete_body = _consume_activity_event_body_sync(f, event_type)
                        if incomplete_body: logger.warning(f"PlotStreamDurations: Incomplete body for event {event_type} at {ts}. Stopping read."); f.seek(current_event_start_offset); break
                if active_stream_start_ts and ((query_end_unix and active_stream_start_ts < query_end_unix) or not query_end_unix):
                    eff_s = max(active_stream_start_ts, query_start_unix) if query_start_unix else active_stream_start_ts
                    eff_e = query_end_unix if query_end_unix else int(now_utc.timestamp())
                    if eff_e > eff_s: durations_hours.append((eff_e - eff_s) / 3600.0)
            else:
                with open(target_file, 'rb') as f:
                    while True:
                        chunk = f.read(STREAM_DURATION_RECORD_SIZE);
                        if not chunk: break
                        if len(chunk) < STREAM_DURATION_RECORD_SIZE: break
                        s_ts, e_ts = struct.unpack(STREAM_DURATION_RECORD_FORMAT, chunk); is_relevant = True
                        if query_start_unix and e_ts < query_start_unix : is_relevant = False
                        if query_end_unix and s_ts > query_end_unix : is_relevant = False
                        if is_relevant:
                            eff_s_ts = max(s_ts, query_start_unix) if query_start_unix else s_ts
                            eff_e_ts = min(e_ts, query_end_unix) if query_end_unix else e_ts
                            if eff_e_ts > eff_s_ts: durations_hours.append((eff_e_ts - eff_s_ts) / 3600.0)
        except FileNotFoundError: await ctx.send(f"File {target_file} not found."); return
        except Exception as e_read: await ctx.send(f"Error reading duration data: {e_read}"); logger.error(f"Error in plotstreamdurations read: {e_read}", exc_info=True); return
        if not durations_hours or len(durations_hours) == 0: await ctx.send(f"No {data_source_name.lower()} data found for the specified period to plot."); return
        fig, ax = plt.subplots(figsize=(10, 6))
        num_bins = max(1, min(20, len(set(durations_hours)) // 2 if len(set(durations_hours)) > 4 else 5))
        if len(durations_hours) <=5 : num_bins = len(durations_hours)
        ax.hist(durations_hours, bins=num_bins, edgecolor='black', color='skyblue')
        ax.set_title(f"Histogram of {data_source_name} for {UTA_TWITCH_CHANNEL_NAME or 'Channel'} ({period_name})")
        ax.set_xlabel("Duration (Hours)"); ax.set_ylabel("Number of Streams")
        ax.grid(axis='y', alpha=0.75, linestyle=':'); plt.tight_layout()
        await _send_plot_if_available(ctx, fig, f"stream_durations_hist_{UTA_TWITCH_CHANNEL_NAME or 'channel'}")

@bot.command(name="commands", aliases=['help'], help="Lists all available commands.")
async def list_commands_command(ctx: commands.Context):
    if FCTD_COMMAND_CHANNEL_ID is not None and ctx.channel.id != FCTD_COMMAND_CHANNEL_ID: return
    embed = discord.Embed(title="Bot Commands", description=f"Prefix: `{FCTD_COMMAND_PREFIX}`", color=discord.Color.blue())
    sorted_commands = sorted(bot.commands, key=lambda c: c.name)
    for cmd in sorted_commands:
        if cmd.hidden: continue
        if cmd.name in ["plotfollowers", "plotstreamdurations"] and not MATPLOTLIB_AVAILABLE: continue
        name_aliases = f"`{FCTD_COMMAND_PREFIX}{cmd.name}`"
        if cmd.aliases: name_aliases += f" (or {', '.join([f'`{FCTD_COMMAND_PREFIX}{a}`' for a in cmd.aliases])})"
        desc = cmd.help or "No description."; embed.add_field(name=name_aliases, value=desc, inline=False)
    if not embed.fields: embed.description = "No commands available."
    if not MATPLOTLIB_AVAILABLE: embed.set_footer(text="Plotting commands are hidden as Matplotlib is not installed.")
    await ctx.send(embed=embed)

@bot.command(name="utastatus", help="Shows status of UTA modules. (Bot owner only)")
@commands.is_owner()
async def uta_status_command(ctx: commands.Context):
    if FCTD_COMMAND_CHANNEL_ID is not None and ctx.channel.id != FCTD_COMMAND_CHANNEL_ID: return
    embed = discord.Embed(title="Bot & UTA Module Status", color=discord.Color.orange())
    uptime_delta = datetime.now(timezone.utc) - bot_start_time
    human_uptime = format_duration_human(int(uptime_delta.total_seconds()))
    embed.add_field(name="Bot Uptime (Current Session)", value=f"{human_uptime} (Since: {discord.utils.format_dt(bot_start_time, 'F')})", inline=False)
    if not UTA_ENABLED: embed.add_field(name="UTA Status", value="UTA module disabled in config.", inline=False); await ctx.send(embed=embed); return
    embed.add_field(name="UTA Enabled", value=str(UTA_ENABLED), inline=False)
    embed.add_field(name="Target Twitch Channel", value=UTA_TWITCH_CHANNEL_NAME or "Not Set", inline=False)
    clip_status = "Disabled in Config"
    if UTA_CLIP_MONITOR_ENABLED:
        clip_thread_status = "Not Active";
        if uta_clip_thread and uta_clip_thread.is_alive(): clip_thread_status = f"Active ({uta_clip_thread.name})"
        clip_status = f"Enabled. Thread: {clip_thread_status}. Sent Clips Cache: {len(uta_sent_clip_ids)}"
    embed.add_field(name="Clip Monitor", value=clip_status, inline=False)
    restream_status = "Disabled in Config"
    if UTA_RESTREAMER_ENABLED:
        restream_thread_status = "Not Active"
        if uta_restreamer_thread and uta_restreamer_thread.is_alive(): restream_thread_status = f"Active ({uta_restreamer_thread.name})"
        restream_status = f"Enabled. Thread: {restream_thread_status}. Currently Restreaming: {uta_is_restreaming_active}"
        if uta_is_restreaming_active:
            if UTA_FFMPEG_PID: restream_status += f"\n  FFmpeg PID: `{UTA_FFMPEG_PID}`"
            if UTA_STREAMLINK_PID: restream_status += f"\n  Streamlink PID: `{UTA_STREAMLINK_PID}`"
            if UTA_PIPE_START_TIME_UTC:
                pipe_uptime_delta = datetime.now(timezone.utc) - UTA_PIPE_START_TIME_UTC
                pipe_uptime_str = format_duration_human(int(pipe_uptime_delta.total_seconds()))
                restream_status += f"\n  Current Pipe Uptime: {pipe_uptime_str}"
        
        if effective_youtube_api_enabled():
            restream_status += "\n  Mode: YouTube API"
            if uta_yt_service:
                restream_status += f" (Service Initialized)"
                if uta_current_youtube_broadcast_id: restream_status += f"\n    Current YT Broadcast ID: `{uta_current_youtube_broadcast_id}` (Part {uta_current_restream_part_number})"
                if uta_youtube_next_rollover_time_utc: restream_status += f"\n    Next Rollover: {discord.utils.format_dt(uta_youtube_next_rollover_time_utc, 'R')}"
            else: restream_status += " (Service NOT Initialized)"
        elif UTA_YOUTUBE_API_ENABLED and not GOOGLE_API_AVAILABLE: restream_status += "\n  Mode: YouTube API (Google Libs Missing!)"
        else: restream_status += "\n  Mode: Legacy RTMP"
        
        restream_status += f"\n  Consecutive Pipe Failures: {UTA_RESTREAM_CONSECUTIVE_FAILURES}/{UTA_RESTREAM_MAX_CONSECUTIVE_FAILURES}"
        if UTA_YOUTUBE_PLAYABILITY_CHECK_ENABLED:
            restream_status += f"\n  Last YT Playability Check: {UTA_LAST_PLAYABILITY_CHECK_STATUS}"
    embed.add_field(name="Restreamer", value=restream_status, inline=False)
    stream_status_mon_text = "Disabled in Config"
    if UTA_STREAM_STATUS_NOTIFICATIONS_ENABLED:
        status_thread_active = "Not Active"
        if uta_stream_status_thread and uta_stream_status_thread.is_alive(): status_thread_active = f"Active ({uta_stream_status_thread.name})"
        stream_status_mon_text = f"Enabled. Thread: {status_thread_active}."
        if UTA_VIEWER_COUNT_LOGGING_ENABLED: stream_status_mon_text += f"\n  Viewer Logging: Enabled (Interval: {UTA_VIEWER_COUNT_LOG_INTERVAL_SECONDS}s)"
        else: stream_status_mon_text += "\n  Viewer Logging: Disabled"
        stream_status_mon_text += f"\n  Activity Log File: `{UTA_STREAM_ACTIVITY_LOG_FILE}`"
    embed.add_field(name="Stream Status Monitor & Activity Logger", value=stream_status_mon_text, inline=False)
    token_status = "No Token or Error"
    if uta_shared_access_token and uta_token_expiry_time > 0:
        expiry_dt = datetime.fromtimestamp(uta_token_expiry_time)
        token_status = f"Token Acquired. Expires: {discord.utils.format_dt(expiry_dt, 'R')} ({discord.utils.format_dt(expiry_dt, 'f')})"
    elif uta_token_expiry_time == 0 and not uta_shared_access_token: token_status = "Failed to acquire token or token expired and failed refresh."
    embed.add_field(name="UTA Twitch API Token", value=token_status, inline=False)
    if BOT_SESSION_LOG_FILE_PATH: embed.add_field(name="Bot Session Log", value=f"Enabled (`{os.path.basename(BOT_SESSION_LOG_FILE_PATH)}`)", inline=False)
    else: embed.add_field(name="Bot Session Log", value="Disabled or not configured", inline=False)
    await ctx.send(embed=embed)

@bot.command(name="utarestartffmpeg", help="Requests UTA to restart the current FFmpeg/Streamlink pipe. Owner only.")
@commands.is_owner()
async def uta_restart_ffmpeg_command(ctx: commands.Context):
    global UTA_MANUAL_FFMPEG_RESTART_REQUESTED
    if not (UTA_ENABLED and UTA_RESTREAMER_ENABLED):
        await ctx.send("UTA Restreamer is not enabled."); return
    if not twitch_session_active_global:
        await ctx.send("Cannot restart FFmpeg/Streamlink: No active Twitch session being restreamed."); return
    # No need to check uta_is_restreaming_active here, the loop will handle it if it's already stopped/restarting
    UTA_MANUAL_FFMPEG_RESTART_REQUESTED = True
    logger.info(f"Discord command: Manual FFmpeg/Streamlink restart requested by {ctx.author}.")
    await ctx.send("Request to restart FFmpeg/Streamlink pipe has been sent. It will be processed by the restreamer loop shortly.")

@bot.command(name="utastartnewpart", help="Requests UTA to start a new YouTube broadcast part (API mode only). Owner only.")
@commands.is_owner()
async def uta_start_new_part_command(ctx: commands.Context):
    global UTA_MANUAL_NEW_PART_REQUESTED
    if not (UTA_ENABLED and UTA_RESTREAMER_ENABLED and effective_youtube_api_enabled()):
        await ctx.send("UTA Restreamer with YouTube API mode is not active or not configured."); return
    if not twitch_session_active_global:
        await ctx.send("Cannot start a new part: No active Twitch session being restreamed."); return
    if not youtube_api_session_active_global:
         await ctx.send("Cannot start a new part: No active YouTube API broadcast part. This command is for API mode streams."); return
    UTA_MANUAL_NEW_PART_REQUESTED = True
    logger.info(f"Discord command: Manual new YouTube part requested by {ctx.author}.")
    await ctx.send("Request to start a new YouTube broadcast part has been sent. It will be processed by the restreamer loop shortly.")

@bot.command(name="utaytstatus", help="Shows current YouTube restream status (API mode). Owner only.")
@commands.is_owner()
async def uta_yt_status_command(ctx: commands.Context):
    if not (UTA_ENABLED and UTA_RESTREAMER_ENABLED and effective_youtube_api_enabled()):
        await ctx.send("YouTube API restreaming is not active or not configured."); return
    if not twitch_session_active_global:
        await ctx.send("Not currently in an active Twitch restream session."); return
    if not youtube_api_session_active_global:
        await ctx.send("Currently in a Twitch session, but no active YouTube API broadcast part (possibly using legacy RTMP or API error)."); return
    if uta_current_youtube_broadcast_id:
        embed = discord.Embed(title="UTA YouTube Restream Status", color=discord.Color.blue())
        embed.add_field(name="Target Twitch Channel", value=UTA_TWITCH_CHANNEL_NAME or "N/A", inline=False)
        embed.add_field(name="Current YouTube Video ID", value=f"`{uta_current_youtube_video_id}`\n[Watch Link](https://www.youtube.com/watch?v={uta_current_youtube_video_id})", inline=False)
        embed.add_field(name="Current Part Number", value=str(uta_current_restream_part_number), inline=True)
        embed.add_field(name="Bound LiveStream ID", value=f"`{uta_current_youtube_live_stream_id or 'N/A'}`", inline=True)
        if uta_youtube_next_rollover_time_utc:
            embed.add_field(name="Next Scheduled Rollover", value=discord.utils.format_dt(uta_youtube_next_rollover_time_utc, 'F') + f" ({discord.utils.format_dt(uta_youtube_next_rollover_time_utc, 'R')})", inline=False)
        else: embed.add_field(name="Scheduled Rollover", value="Disabled or not applicable", inline=False)
        embed.set_footer(text="This status reflects the current YouTube 'part' of the ongoing Twitch stream.")
        await ctx.send(embed=embed)
    else: await ctx.send("No active YouTube broadcast ID found for the current session, but Twitch session is active.")

@bot.command(name="utahealth", help="Shows an extended health check diagnostic for the bot. Owner only.")
@commands.is_owner()
async def uta_health_command(ctx: commands.Context):
    if FCTD_COMMAND_CHANNEL_ID is not None and ctx.channel.id != FCTD_COMMAND_CHANNEL_ID: return

    await ctx.send("Performing bot health check...")
    async with ctx.typing():
        embed = discord.Embed(title="UTA Bot Health Check", color=discord.Color.dark_teal(), timestamp=datetime.now(timezone.utc))

        # --- Section 1: General Bot & System Info ---
        embed.add_field(name="🕒 Bot Current UTC", value=f"`{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S %Z')}`", inline=False)
        embed.add_field(name="⏱️ Bot Session Uptime", value=f"`{format_duration_human(int((datetime.now(timezone.utc) - bot_start_time).total_seconds()))}` (Since: {discord.utils.format_dt(bot_start_time, 'F')})", inline=False)
        embed.add_field(name="🐍 Python Version", value=f"`{sys.version.split(' ')[0]}`", inline=True)
        embed.add_field(name="🤖 Discord.py Version", value=f"`{discord.__version__}`", inline=True)
        embed.add_field(name="✨ Bot Ready State", value=f"{'✅ Ready' if bot.is_ready() else '❌ Not Ready'}", inline=True)
        embed.add_field(name="📶 Bot Latency", value=f"`{bot.latency*1000:.2f} ms`", inline=True)
        embed.add_field(name="🏰 Guilds Connected", value=f"`{len(bot.guilds)}`", inline=True)
        embed.add_field(name="---", value="---", inline=True) # Spacer

        # --- Section 2: Libraries & Config File ---
        libs_status = [
            f"🎨 Matplotlib: {'✅ Available' if MATPLOTLIB_AVAILABLE else '❌ Not Available'}",
            f"🔗 Streamlink Lib: {'✅ Available' if STREAMLINK_LIB_AVAILABLE else '❌ Not Available'}",
            f"▶️ Google API Libs: {'✅ Available' if GOOGLE_API_AVAILABLE else '❌ Not Available'}"
        ]
        embed.add_field(name="📚 Core Libraries", value="\n".join(libs_status), inline=False)

        config_status_parts = []
        if os.path.exists(CONFIG_FILE):
            config_stat = os.stat(CONFIG_FILE)
            config_status_parts.append(f"✅ Found (`{CONFIG_FILE}`)")
            config_status_parts.append(f"Size: {config_stat.st_size}B")
            config_status_parts.append(f"Modified: {datetime.fromtimestamp(config_stat.st_mtime, timezone.utc).strftime('%Y-%m-%d %H:%M')}")
        else:
            config_status_parts.append(f"❌ Not Found (`{CONFIG_FILE}`)")
        embed.add_field(name="🔧 Configuration File", value="\n".join(config_status_parts), inline=False)

        # --- Section 3: API Tokens & Services ---
        tw_token_status = "❌ Not Acquired/Error"
        if uta_shared_access_token and uta_token_expiry_time > 0:
            expires_in_seconds = uta_token_expiry_time - time.time()
            if expires_in_seconds > 0:
                tw_token_status = f"✅ Acquired (Expires: {discord.utils.format_dt(datetime.fromtimestamp(uta_token_expiry_time), 'R')})"
            else:
                tw_token_status = f"⚠️ Expired (Refresh Pending, Was: {discord.utils.format_dt(datetime.fromtimestamp(uta_token_expiry_time), 'F')})"
        embed.add_field(name="🔑 Twitch App Token", value=tw_token_status, inline=False)

        yt_api_status = "ℹ️ Disabled in Config"
        if UTA_YOUTUBE_API_ENABLED:
            if not GOOGLE_API_AVAILABLE: yt_api_status = "❌ Enabled but Google Libs Missing"
            elif uta_yt_service is not None: yt_api_status = "✅ Initialized & Service Built"
            else:
                if os.path.exists(UTA_YOUTUBE_TOKEN_FILE): yt_api_status = "⚠️ Token File Exists, Service Not Built (Error?)"
                else: yt_api_status = "⚠️ Not Initialized (No Token File, Needs Auth)"
        embed.add_field(name="🎬 YouTube API Service", value=yt_api_status, inline=False)

        # --- Section 4: Executables & Paths ---
        executables_status = []
        sl_path_str = UTA_STREAMLINK_PATH or "N/A (Not Configured)"
        sl_found = shutil.which(sl_path_str) if sl_path_str != "N/A (Not Configured)" else None
        executables_status.append(f"🔗 Streamlink Exe: `{sl_path_str}` {'✅ Found' if sl_found else '❌ Not Found'}")
        
        ff_path_str = UTA_FFMPEG_PATH or "N/A (Not Configured)"
        ff_found = shutil.which(ff_path_str) if ff_path_str != "N/A (Not Configured)" else None
        executables_status.append(f"🎞️ FFmpeg Exe: `{ff_path_str}` {'✅ Found' if ff_found else '❌ Not Found'}")
        embed.add_field(name="🛠️ External Executables", value="\n".join(executables_status), inline=False)

        # --- Section 5: Log Files (Existence & Size) ---
        log_file_details = []
        log_file_paths = {
            "Followers": FCTD_FOLLOWER_DATA_FILE, "Activity": UTA_STREAM_ACTIVITY_LOG_FILE,
            "Viewers": UTA_VIEWER_COUNT_LOG_FILE, "Durations": UTA_STREAM_DURATION_LOG_FILE,
            "Bot Sessions": BOT_SESSION_LOG_FILE_PATH
        }
        for name, path in log_file_paths.items():
            if path: # Only check if path is configured
                if os.path.exists(path):
                    size_kb = os.path.getsize(path) / 1024
                    log_file_details.append(f"{name}: ✅ ({size_kb:.1f} KB)")
                else:
                    log_file_details.append(f"{name}: ❌ (Path: `{path}`)")
            else:
                log_file_details.append(f"{name}: ℹ️ Not Configured")

        embed.add_field(name="📦 Data Log Files", value="\n".join(log_file_details) if log_file_details else "N/A", inline=False)
        
        # --- Section 6: Disk Space ---
        try:
            # Check disk space of the directory where the script is running, good proxy for log/data storage
            script_dir = os.path.dirname(os.path.abspath(__file__)) 
            total, used, free = shutil.disk_usage(script_dir)
            free_gb = free / (1024**3); total_gb = total / (1024**3); percent_free = (free / total) * 100
            embed.add_field(name="💾 Disk Space (Bot Dir)", value=f"{free_gb:.1f}GB Free / {total_gb:.1f}GB Total ({percent_free:.1f}%)", inline=False)
        except Exception as e:
            logger.warning(f"Could not get disk space: {e}")
            embed.add_field(name="💾 Disk Space (Bot Dir)", value="⚠️ Error retrieving", inline=False)

        # --- Section 7: UTA Module & Thread Status ---
        embed.add_field(name="💠 UTA Overall Status", value=f"{'✅ UTA Enabled' if UTA_ENABLED else '❌ UTA Disabled'} (Target: `{UTA_TWITCH_CHANNEL_NAME or 'Not Set'}`)", inline=False)

        if UTA_ENABLED:
            threads_info = []
            clip_s = "ℹ️ Disabled in Config"
            if UTA_CLIP_MONITOR_ENABLED: clip_s = f"{'✅ Active' if uta_clip_thread and uta_clip_thread.is_alive() else '❌ Inactive/Error'} (ID: {uta_clip_thread.ident if uta_clip_thread else 'N/A'})"
            threads_info.append(f"🎬 Clip Monitor: {clip_s}")

            restream_s = "ℹ️ Disabled in Config"
            if UTA_RESTREAMER_ENABLED: restream_s = f"{'✅ Active' if uta_restreamer_thread and uta_restreamer_thread.is_alive() else '❌ Inactive/Error'} (ID: {uta_restreamer_thread.ident if uta_restreamer_thread else 'N/A'})"
            threads_info.append(f"📡 Restreamer Thread: {restream_s}")
            
            status_mon_s = "ℹ️ Disabled in Config"
            if UTA_STREAM_STATUS_NOTIFICATIONS_ENABLED: status_mon_s = f"{'✅ Active' if uta_stream_status_thread and uta_stream_status_thread.is_alive() else '❌ Inactive/Error'} (ID: {uta_stream_status_thread.ident if uta_stream_status_thread else 'N/A'})"
            threads_info.append(f"📊 Status Monitor Thread: {status_mon_s}")
            embed.add_field(name="🧵 UTA Component Threads", value="\n".join(threads_info), inline=False)

            if UTA_RESTREAMER_ENABLED:
                restream_pipe_status_val = "Inactive"
                if uta_is_restreaming_active: restream_pipe_status_val = "✅ Streaming"
                elif twitch_session_active_global : restream_pipe_status_val = "⚠️ Twitch Live, Pipe Inactive/Retrying"
                
                restream_details = [f"Pipe Status: {restream_pipe_status_val}"]
                if uta_is_restreaming_active: # Only show PIDs/uptime if pipe is thought to be active
                    if UTA_FFMPEG_PID: restream_details.append(f"FFmpeg PID: `{UTA_FFMPEG_PID}`")
                    if UTA_STREAMLINK_PID: restream_details.append(f"Streamlink PID: `{UTA_STREAMLINK_PID}`")
                    if UTA_PIPE_START_TIME_UTC:
                        pipe_uptime_delta = datetime.now(timezone.utc) - UTA_PIPE_START_TIME_UTC
                        restream_details.append(f"Current Pipe Uptime: {format_duration_human(int(pipe_uptime_delta.total_seconds()))}")
                
                restream_details.append(f"Consecutive Fails: {UTA_RESTREAM_CONSECUTIVE_FAILURES}/{UTA_RESTREAM_MAX_CONSECUTIVE_FAILURES}")

                if effective_youtube_api_enabled():
                    restream_details.append(f"YT API Mode: {'Active' if youtube_api_session_active_global else 'Inactive/Error'}")
                    if youtube_api_session_active_global and uta_current_youtube_broadcast_id:
                         restream_details.append(f"YT Broadcast: `{uta_current_youtube_video_id}` (Part {uta_current_restream_part_number})")
                         if uta_youtube_next_rollover_time_utc:
                             restream_details.append(f"Next Rollover: {discord.utils.format_dt(uta_youtube_next_rollover_time_utc, 'R')}")
                elif UTA_YOUTUBE_API_ENABLED: # API enabled but not effective (e.g. libs missing)
                    restream_details.append("YT API Mode: Configured but Inoperable")
                else:
                    restream_details.append("YT API Mode: Disabled (Legacy RTMP)")

                if UTA_YOUTUBE_PLAYABILITY_CHECK_ENABLED:
                    restream_details.append(f"Last YT Playability: {UTA_LAST_PLAYABILITY_CHECK_STATUS}")
                
                embed.add_field(name="🔧 Restreamer Details", value="\n".join(restream_details), inline=False)

        embed.set_footer(text="Health check complete.")
    await ctx.send(embed=embed)

@bot.command(name="utaping", help="Checks bot's responsiveness and Discord gateway latency.")
async def uta_ping_command(ctx: commands.Context):
    if FCTD_COMMAND_CHANNEL_ID is not None and ctx.channel.id != FCTD_COMMAND_CHANNEL_ID: return
    
    start_time = time.monotonic()
    message = await ctx.send("Pinging...")
    end_time = time.monotonic()
    
    latency_gateway = bot.latency * 1000  # Gateway latency in ms
    latency_roundtrip = (end_time - start_time) * 1000 # Command processing and message roundtrip
    
    embed = discord.Embed(title="🏓 Pong!", color=discord.Color.green())
    embed.add_field(name="Gateway Latency", value=f"{latency_gateway:.2f} ms", inline=False)
    embed.add_field(name="Command Roundtrip", value=f"{latency_roundtrip:.2f} ms", inline=False)
    await message.edit(content=None, embed=embed)


@bot.command(name="utarestreamerstate", help="Shows focused status of the UTA restreamer module. Owner only.")
@commands.is_owner()
async def uta_restreamer_state_command(ctx: commands.Context):
    if FCTD_COMMAND_CHANNEL_ID is not None and ctx.channel.id != FCTD_COMMAND_CHANNEL_ID: return

    if not UTA_ENABLED:
        await ctx.send("UTA module is disabled in config."); return
    if not UTA_RESTREAMER_ENABLED:
        await ctx.send("UTA Restreamer module is disabled in config."); return

    embed = discord.Embed(title="📡 UTA Restreamer Status", color=discord.Color.purple(), timestamp=datetime.now(timezone.utc))
    embed.add_field(name="Target Twitch Channel", value=f"`{UTA_TWITCH_CHANNEL_NAME or 'Not Set'}`", inline=False)

    # Twitch Live Status (based on what the restreamer loop thinks)
    embed.add_field(name="Twitch Live Session", value=f"{'✅ Active' if twitch_session_active_global else '❌ Inactive'}", inline=True)

    # YouTube Mode
    yt_mode = "Legacy RTMP"
    if effective_youtube_api_enabled():
        yt_mode = f"YouTube API ({'✅ Initialized' if uta_yt_service else '⚠️ Not Initialized'})"
    elif UTA_YOUTUBE_API_ENABLED: # API enabled in config, but libs missing
        yt_mode = "YouTube API (❌ Libs Missing!)"
    embed.add_field(name="YouTube Mode", value=yt_mode, inline=True)
    embed.add_field(name="YT API Part Active", value=f"{'✅ Yes' if youtube_api_session_active_global else '❌ No'}", inline=True)


    # Pipe Status
    pipe_status_str = "Inactive"
    if uta_is_restreaming_active: pipe_status_str = "✅ Streaming"
    elif twitch_session_active_global: pipe_status_str = "⚠️ Twitch Live, Pipe Inactive/Retrying" # If Twitch is live but pipe isn't
    embed.add_field(name="Stream Pipe Status", value=pipe_status_str, inline=False)

    if uta_is_restreaming_active or twitch_session_active_global : # Show these details if a session is active or pipe is trying
        if UTA_FFMPEG_PID: embed.add_field(name="FFmpeg PID", value=f"`{UTA_FFMPEG_PID}`", inline=True)
        if UTA_STREAMLINK_PID: embed.add_field(name="Streamlink PID", value=f"`{UTA_STREAMLINK_PID}`", inline=True)
        if UTA_PIPE_START_TIME_UTC:
            uptime_delta = datetime.now(timezone.utc) - UTA_PIPE_START_TIME_UTC
            pipe_uptime = format_duration_human(int(uptime_delta.total_seconds()))
            embed.add_field(name="Current Pipe Uptime", value=pipe_uptime, inline=True)
    
    embed.add_field(name="Consecutive Pipe Fails", value=f"{UTA_RESTREAM_CONSECUTIVE_FAILURES}/{UTA_RESTREAM_MAX_CONSECUTIVE_FAILURES}", inline=True)
    if UTA_YOUTUBE_PLAYABILITY_CHECK_ENABLED:
        embed.add_field(name="Last YT Playability", value=UTA_LAST_PLAYABILITY_CHECK_STATUS, inline=True)
    
    if youtube_api_session_active_global and uta_current_youtube_video_id:
        embed.add_field(name="Current YT Video ID", value=f"[{uta_current_youtube_video_id}](https://www.youtube.com/watch?v={uta_current_youtube_video_id}) (Part {uta_current_restream_part_number})", inline=False)
        if uta_youtube_next_rollover_time_utc:
            embed.add_field(name="Next YT Rollover", value=f"{discord.utils.format_dt(uta_youtube_next_rollover_time_utc, 'R')}", inline=False)
    
    await ctx.send(embed=embed)


@bot.command(name="utasummary", help="Generates a summary report for a period. Usage: !utasummary <period|today|yesterday|all>")
@commands.is_owner()
async def uta_summary_command(ctx: commands.Context, period_input: str = "7d"):
    if FCTD_COMMAND_CHANNEL_ID is not None and ctx.channel.id != FCTD_COMMAND_CHANNEL_ID: return
    if not UTA_ENABLED:
        await ctx.send("UTA module is disabled. No summary available."); return

    now_utc = datetime.now(timezone.utc)
    query_start_utc, query_end_utc = None, now_utc # Default end is now
    period_name_display = ""

    if period_input.lower() == "today":
        query_start_utc = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
        period_name_display = "Today"
    elif period_input.lower() == "yesterday":
        yesterday = now_utc - timedelta(days=1)
        query_start_utc = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
        query_end_utc = yesterday.replace(hour=23, minute=59, second=59, microsecond=999999)
        period_name_display = "Yesterday"
    elif period_input.lower() == "all":
        query_start_unix = 0 # Effectively from the beginning of time for log files
        query_end_unix = int(now_utc.timestamp())
        period_name_display = "All Time"
    else:
        time_delta, period_name_parsed = parse_duration_to_timedelta(period_input)
        if not time_delta:
            await ctx.send(f"Invalid period: '{period_input}'. Use a duration (e.g., 7d, 24h), 'today', 'yesterday', or 'all'."); return
        query_start_utc = now_utc - time_delta
        period_name_display = period_name_parsed # "last X days/hours"

    if query_start_utc: query_start_unix = int(query_start_utc.timestamp())
    if query_end_utc: query_end_unix = int(query_end_utc.timestamp())
    
    # --- If period_name_display is not set by specific keywords, create one from dates ---
    if not period_name_display and query_start_utc and query_end_utc:
        period_name_display = f"From {query_start_utc.strftime('%Y-%m-%d %H:%M')} to {query_end_utc.strftime('%Y-%m-%d %H:%M')} UTC"


    await ctx.send(f"Generating summary for {UTA_TWITCH_CHANNEL_NAME or 'configured channel'} for period: {period_name_display}...")
    async with ctx.typing():
        embed = discord.Embed(title=f"UTA Summary: {UTA_TWITCH_CHANNEL_NAME or 'Channel'}",
                              description=f"Period: **{period_name_display}**",
                              color=discord.Color.gold(),
                              timestamp=datetime.now(timezone.utc))

        # 1. Total time Twitch stream was live (from stream_activity.bin)
        total_twitch_live_seconds = 0
        num_twitch_sessions = 0
        if UTA_STREAM_ACTIVITY_LOG_FILE and os.path.exists(UTA_STREAM_ACTIVITY_LOG_FILE):
            # This requires parsing start/end events from stream_activity.bin for the period
            # For simplicity, we'll approximate or use a simpler metric for now.
            # A full parse of stream_activity.bin to sum up live durations is complex.
            # Let's use the game segments parser for an approximation of "on-air" time for games
            game_segments = await asyncio.to_thread(
                _parse_stream_activity_for_game_segments_sync,
                UTA_STREAM_ACTIVITY_LOG_FILE,
                query_start_unix,
                query_end_unix
            )
            total_twitch_live_seconds = sum(seg['end_ts'] - seg['start_ts'] for seg in game_segments)
            # Approximating sessions based on gaps in game segments (very rough)
            if game_segments:
                game_segments.sort(key=lambda s: s['start_ts'])
                num_twitch_sessions = 1
                for i in range(1, len(game_segments)):
                    if game_segments[i]['start_ts'] - game_segments[i-1]['end_ts'] > 600: # 10 min gap
                        num_twitch_sessions +=1
            embed.add_field(name="Approx. Twitch Live Time", value=f"{format_duration_human(total_twitch_live_seconds)} (across ~{num_twitch_sessions} sessions)", inline=False)
        else:
            embed.add_field(name="Twitch Live Time", value="Stream activity log not found/configured.", inline=False)

        # 2. Total time restreamed to YouTube (from stream_durations.bin)
        if UTA_RESTREAMER_ENABLED and UTA_STREAM_DURATION_LOG_FILE and os.path.exists(UTA_STREAM_DURATION_LOG_FILE):
            total_restream_seconds, num_yt_vods = await asyncio.to_thread(
                _read_stream_durations_for_period_sync,
                UTA_STREAM_DURATION_LOG_FILE,
                query_start_unix,
                query_end_unix
            )
            embed.add_field(name="Total YouTube Restream Time", value=f"{format_duration_human(total_restream_seconds)} (across {num_yt_vods} VODs/parts)", inline=False)
        elif UTA_RESTREAMER_ENABLED:
            embed.add_field(name="Total YouTube Restream Time", value="Stream duration log not found/configured.", inline=False)

        # 3. Total follower gain/loss (if FCTD_TWITCH_USERNAME matches UTA_TWITCH_CHANNEL_NAME)
        if FCTD_FOLLOWER_DATA_FILE and os.path.exists(FCTD_FOLLOWER_DATA_FILE) and \
           FCTD_TWITCH_USERNAME and FCTD_TWITCH_USERNAME.lower() == (UTA_TWITCH_CHANNEL_NAME or "").lower():
            start_foll, end_foll, first_foll_ts, last_foll_ts, _ = await asyncio.to_thread(
                _read_and_find_records_sync,
                FCTD_FOLLOWER_DATA_FILE,
                query_start_unix, # Cutoff for start value
                query_end_unix    # Inclusive end for end value
            )
            if start_foll is not None and end_foll is not None:
                foll_gain = end_foll - start_foll
                foll_start_dt = datetime.fromtimestamp(first_foll_ts, timezone.utc) if first_foll_ts else "N/A"
                foll_end_dt = datetime.fromtimestamp(last_foll_ts, timezone.utc) if last_foll_ts else "N/A"
                embed.add_field(name="Follower Change", value=f"{foll_gain:+,} (from {start_foll:,} to {end_foll:,})", inline=True)
            else:
                embed.add_field(name="Follower Change", value="Not enough data for period.", inline=True)
        
        # 4. Peak viewer count during the period
        if UTA_VIEWER_COUNT_LOGGING_ENABLED and UTA_VIEWER_COUNT_LOG_FILE and os.path.exists(UTA_VIEWER_COUNT_LOG_FILE):
            avg_v, peak_v, num_dp = await asyncio.to_thread(
                 _get_viewer_stats_for_period_sync, UTA_VIEWER_COUNT_LOG_FILE, query_start_unix, query_end_unix
            )
            if num_dp > 0:
                embed.add_field(name="Peak Viewers", value=f"{peak_v:,} (Avg: {avg_v:,.1f} from {num_dp} points)", inline=True)
            else:
                embed.add_field(name="Peak Viewers", value="No viewer data for period.", inline=True)

        # 5. Top 3 games played by duration
        if UTA_STREAM_ACTIVITY_LOG_FILE and os.path.exists(UTA_STREAM_ACTIVITY_LOG_FILE) and total_twitch_live_seconds > 0:
            game_durations = {}
            for seg in game_segments: # game_segments already filtered by period
                game_durations[seg['game']] = game_durations.get(seg['game'], 0) + (seg['end_ts'] - seg['start_ts'])
            
            sorted_games = sorted(game_durations.items(), key=lambda item: item[1], reverse=True)
            if sorted_games:
                top_games_str = []
                for i, (game, duration_sec) in enumerate(sorted_games[:3]):
                    top_games_str.append(f"{i+1}. {game} ({format_duration_human(duration_sec)})")
                embed.add_field(name="🎲 Top Games Played", value="\n".join(top_games_str) if top_games_str else "No game data.", inline=False)
            else:
                embed.add_field(name="🎲 Top Games Played", value="No game data found for period.", inline=False)

    await ctx.send(embed=embed)

@bot.command(name="utapeakviewers", help="Shows peak viewer count for a period. Usage: !utapeakviewers <period|today|yesterday|all>")
@commands.is_owner()
async def uta_peak_viewers_command(ctx: commands.Context, period_input: str = "all"):
    if FCTD_COMMAND_CHANNEL_ID is not None and ctx.channel.id != FCTD_COMMAND_CHANNEL_ID: return

    if not (UTA_ENABLED and UTA_VIEWER_COUNT_LOGGING_ENABLED and UTA_VIEWER_COUNT_LOG_FILE and os.path.exists(UTA_VIEWER_COUNT_LOG_FILE)):
        await ctx.send("UTA Viewer count logging is not enabled or the log file is not found/configured.")
        return

    now_utc = datetime.now(timezone.utc)
    query_start_utc, query_end_utc = None, now_utc
    period_name_display = ""

    if period_input.lower() == "today":
        query_start_utc = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
        period_name_display = "Today"
    elif period_input.lower() == "yesterday":
        yesterday = now_utc - timedelta(days=1)
        query_start_utc = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
        query_end_utc = yesterday.replace(hour=23, minute=59, second=59, microsecond=999999)
        period_name_display = "Yesterday"
    elif period_input.lower() == "all":
        query_start_unix = 0 # Effectively from the beginning of time for log files
        query_end_unix = int(now_utc.timestamp())
        period_name_display = "All Time"
    else:
        time_delta, period_name_parsed = parse_duration_to_timedelta(period_input)
        if not time_delta:
            await ctx.send(f"Invalid period: '{period_input}'. Use a duration (e.g., 7d, 24h), 'today', 'yesterday', or 'all'."); return
        query_start_utc = now_utc - time_delta
        period_name_display = period_name_parsed

    if query_start_utc: query_start_unix = int(query_start_utc.timestamp())
    # query_end_unix is already set for "all" or will be from query_end_utc
    if query_end_utc and period_input.lower() != "all": query_end_unix = int(query_end_utc.timestamp())


    async with ctx.typing():
        peak_viewer_count = 0
        peak_viewer_timestamp_unix = None
        records_in_period = 0

        try:
            with open(UTA_VIEWER_COUNT_LOG_FILE, 'rb') as f:
                while True:
                    chunk = f.read(BINARY_RECORD_SIZE)
                    if not chunk: break
                    if len(chunk) < BINARY_RECORD_SIZE: break # Corrupt record
                    
                    ts, count = struct.unpack(BINARY_RECORD_FORMAT, chunk)
                    
                    if query_start_unix <= ts <= query_end_unix:
                        records_in_period += 1
                        if count > peak_viewer_count:
                            peak_viewer_count = count
                            peak_viewer_timestamp_unix = ts
        except FileNotFoundError:
            await ctx.send(f"Viewer count log file `{UTA_VIEWER_COUNT_LOG_FILE}` not found."); return
        except Exception as e:
            logger.error(f"Error reading viewer count log for peak viewers: {e}", exc_info=True)
            await ctx.send(f"An error occurred while reading viewer data: {e}"); return

        if records_in_period == 0:
            await ctx.send(f"No viewer data found for the period: {period_name_display}"); return

        embed = discord.Embed(
            title=f"Peak Viewers for {UTA_TWITCH_CHANNEL_NAME or 'Configured Channel'}",
            description=f"Period: **{period_name_display}**",
            color=discord.Color.purple()
        )
        embed.add_field(name="🚀 Peak Viewer Count", value=f"**{peak_viewer_count:,}**", inline=False)
        if peak_viewer_timestamp_unix:
            peak_dt = datetime.fromtimestamp(peak_viewer_timestamp_unix, timezone.utc)
            embed.add_field(name="📅 Occurred At", value=f"{discord.utils.format_dt(peak_dt, 'F')} ({discord.utils.format_dt(peak_dt, 'R')})", inline=False)
        embed.set_footer(text=f"{records_in_period} data points analyzed in period.")
        await ctx.send(embed=embed)

@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.CommandNotFound): pass
    elif isinstance(error, commands.MissingRequiredArgument): await ctx.send(f"Missing argument for `{ctx.command.name}`. Use `{FCTD_COMMAND_PREFIX}help {ctx.command.name}` for more info.", delete_after=15)
    elif isinstance(error, commands.NotOwner): await ctx.send("Sorry, this command can only be used by the bot owner.", delete_after=10)
    elif isinstance(error, commands.CheckFailure): logger.warning(f"Command check failed for {ctx.author} on '{ctx.command}': {error}")
    elif isinstance(error, commands.CommandInvokeError): logger.error(f'Error in command {ctx.command}: {error.original}', exc_info=error.original); await ctx.send(f"An error occurred while executing the command: {error.original}", delete_after=10)
    else: logger.error(f'Unhandled command error for command {ctx.command}: {error}', exc_info=error)

async def main():
    async with bot:
        logger.info("Starting bot...")
        await bot.start(DISCORD_TOKEN)

if __name__ == "__main__":
    # Pre-run checks
    if UTA_ENABLED and UTA_RESTREAMER_ENABLED:
        if not shutil.which(UTA_STREAMLINK_PATH):
            logger.critical(f"UTA PRE-RUN FAIL: Streamlink executable ('{UTA_STREAMLINK_PATH}') not found in PATH or as absolute path.")
            exit(1)
        if not shutil.which(UTA_FFMPEG_PATH):
            logger.critical(f"UTA PRE-RUN FAIL: FFmpeg executable ('{UTA_FFMPEG_PATH}') not found in PATH or as absolute path.")
            exit(1)
        
        if effective_youtube_api_enabled(): # Use helper
            if not GOOGLE_API_AVAILABLE: # Should be caught by effective_youtube_api_enabled, but for directness
                logger.critical("UTA PRE-RUN FAIL: UTA_YOUTUBE_API_ENABLED is True, but Google API client libraries are not installed. Please install 'google-api-python-client google-auth-httplib2 google-auth-oauthlib'.")
                exit(1)
            if not os.path.exists(UTA_YOUTUBE_CLIENT_SECRET_FILE):
                logger.critical(f"UTA PRE-RUN FAIL: YouTube API client secret file ('{UTA_YOUTUBE_CLIENT_SECRET_FILE}') not found. Download from Google Cloud Console and place it correctly (or update path in config).")
                exit(1)
        elif UTA_YOUTUBE_API_ENABLED and not GOOGLE_API_AVAILABLE: # API intended but libs missing
             logger.critical("UTA PRE-RUN FAIL: UTA_YOUTUBE_API_ENABLED is True, but Google API client libraries are not installed. YouTube API features disabled.")
             # Bot will run, but API mode for restreamer will effectively be off.
        elif not UTA_YOUTUBE_API_ENABLED and (not UTA_YOUTUBE_STREAM_KEY or "YOUR_YOUTUBE_STREAM_KEY" in UTA_YOUTUBE_STREAM_KEY):
            logger.warning(f"UTA PRE-RUN WARN: YouTube API is disabled AND YOUTUBE_STREAM_KEY is not set or is a placeholder. Restreamer will not function unless API mode can be used.")

    if not MATPLOTLIB_AVAILABLE:
        logger.info("NOTE: Matplotlib is not installed. Plotting commands and graph attachments will be disabled. To enable: pip install matplotlib")

    if UTA_YOUTUBE_PLAYABILITY_CHECK_ENABLED and not STREAMLINK_LIB_AVAILABLE:
        logger.warning("UTA_YOUTUBE_PLAYABILITY_CHECK_ENABLED is true in config, but the Streamlink Python library is not installed. YouTube playability checks will be skipped. To enable: pip install streamlink")

    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received. Shutting down...")
    except discord.LoginFailure:
        logger.critical("CRITICAL: Invalid Discord Bot Token. Please check your config.json.")
    except Exception as e:
        logger.critical(f"Unexpected error during bot startup/runtime: {e}", exc_info=True)
    finally:
        logger.info("Initiating final cleanup sequence...")
        
        if bot and bot.loop and not bot.loop.is_closed():
            try:
                # Try to log the stop event
                asyncio.ensure_future(log_bot_session_event(BOT_EVENT_STOP, datetime.now(timezone.utc)), loop=bot.loop)
                # Give it a very brief moment to try and execute the log.
                # This is a best-effort attempt, as the loop might be closing.
                time.sleep(0.2) 
            except Exception as e_log_stop:
                logger.error(f"Error logging bot stop event during shutdown: {e_log_stop}")

        if UTA_ENABLED and (uta_clip_thread or uta_restreamer_thread or uta_stream_status_thread):
            logger.info("Main Shutdown: Setting shutdown event for UTA threads.")
            shutdown_event.set()
            threads_to_wait_for = []
            if uta_clip_thread and uta_clip_thread.is_alive(): threads_to_wait_for.append(uta_clip_thread)
            if uta_restreamer_thread and uta_restreamer_thread.is_alive(): threads_to_wait_for.append(uta_restreamer_thread)
            if uta_stream_status_thread and uta_stream_status_thread.is_alive(): threads_to_wait_for.append(uta_stream_status_thread)
            
            for t in threads_to_wait_for:
                logger.info(f"Main Shutdown: Waiting for {t.name} to exit...")
                t.join(timeout=7) # Increased timeout slightly for threads
                if t.is_alive(): logger.warning(f"Main Shutdown: {t.name} did not exit cleanly after 7s.")
                else: logger.info(f"Main Shutdown: {t.name} exited.")
            
            logger.info("Main Shutdown: Performing final UTA process cleanup (FFmpeg/Streamlink).")
            uta_cleanup_restream_processes()
        
        # Closing the bot connection if it's still open
        # This should ideally be handled by discord.py's graceful shutdown,
        # but we add an explicit check and attempt if loop indicates it's still running.
        if bot and not bot.is_closed() and bot.loop and bot.loop.is_running():
            logger.info("Main Shutdown: Bot connection appears open, attempting to close.")
            try:
                # Schedule the close on the loop if it's running
                asyncio.ensure_future(bot.close(), loop=bot.loop)
                # Give a moment for close to be processed
                # Note: bot.close() itself is a coroutine, ensure_future is appropriate here.
                # If loop isn't running, this might not execute fully.
                time.sleep(0.5)
            except Exception as e_close:
                logger.error(f"Error during explicit bot close: {e_close}")
        elif bot and bot.is_closed():
             logger.info("Main Shutdown: Bot connection already closed.")
        else:
             logger.info("Main Shutdown: Bot object or loop not in a state to attempt explicit close.")

        logger.info("Shutdown sequence finished. Exiting.")
