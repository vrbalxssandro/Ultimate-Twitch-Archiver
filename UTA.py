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
    plt = None # Placeholder
    mdates = None # Placeholder
    # logger.info("Matplotlib not found, plotting features will be disabled.") # Logged at startup now

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

BINARY_RECORD_FORMAT = '>II' # Timestamp, Count (Followers/Viewers)
BINARY_RECORD_SIZE = struct.calcsize(BINARY_RECORD_FORMAT)
STREAM_DURATION_RECORD_FORMAT = '>II' # Start TS, End TS
STREAM_DURATION_RECORD_SIZE = struct.calcsize(STREAM_DURATION_RECORD_FORMAT)

# Stream Activity Binary Log Constants
EVENT_TYPE_STREAM_START = 1
EVENT_TYPE_STREAM_END = 2
EVENT_TYPE_GAME_CHANGE = 3
EVENT_TYPE_TITLE_CHANGE = 4
EVENT_TYPE_TAGS_CHANGE = 5
# New event types for Ads, Polls, Predictions
EVENT_TYPE_AD_BREAK_START = 6 # Data: duration_seconds (I), is_automatic (B)
EVENT_TYPE_POLL_START = 7       # Data: poll_id (S), title (S), num_choices (H), choices (List of [id(S), title(S)]), duration_seconds (I), channel_points_voting_enabled (B), channel_points_per_vote (I)
EVENT_TYPE_POLL_PROGRESS = 8    # Data: poll_id (S), num_choices (H), choices (List of [id(S), title(S), votes(I), cp_votes(I), bits_votes(I)])
EVENT_TYPE_POLL_END = 9         # Data: poll_id (S), title (S), num_choices (H), choices (List of [id(S), title(S), votes(I), cp_votes(I), bits_votes(I)]), status (S), winning_choice_id (S)
EVENT_TYPE_PREDICTION_START = 10 # Data: prediction_id (S), title (S), num_outcomes (H), outcomes (List of [id(S), title(S), color(S)]), window_seconds (I)
EVENT_TYPE_PREDICTION_PROGRESS = 11 # Data: prediction_id (S), num_outcomes (H), outcomes (List of [id(S), title(S), users(I), channel_points(Q)])
EVENT_TYPE_PREDICTION_LOCK = 12   # Data: prediction_id (S), lock_timestamp (I)
EVENT_TYPE_PREDICTION_END = 13    # Data: prediction_id (S), title (S), winning_outcome_id (S), num_outcomes (H), outcomes (List of [id(S), title(S), users(I), channel_points(Q)]), status (S)


SA_BASE_HEADER_FORMAT = '>BI' # Event Type (Byte), Timestamp (Int)
SA_BASE_HEADER_SIZE = struct.calcsize(SA_BASE_HEADER_FORMAT)
SA_STRING_LEN_FORMAT = '>H' # Length of string (Unsigned Short)
SA_STRING_LEN_SIZE = struct.calcsize(SA_STRING_LEN_FORMAT)
SA_LIST_HEADER_FORMAT = '>H' # Number of items in a list (Unsigned Short) - Renamed from SA_TAG_LIST_HEADER_FORMAT for clarity
SA_LIST_HEADER_SIZE = struct.calcsize(SA_LIST_HEADER_FORMAT)
SA_INT_FORMAT = '>I' # For duration, peak_viewers, counts etc.
SA_INT_SIZE = struct.calcsize(SA_INT_FORMAT)
SA_BOOL_FORMAT = '>?' # For boolean flags
SA_BOOL_SIZE = struct.calcsize(SA_BOOL_FORMAT)
SA_LONG_LONG_FORMAT = '>Q' # For large numbers like channel points
SA_LONG_LONG_SIZE = struct.calcsize(SA_LONG_LONG_FORMAT)


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


# --- fctd: Twitch API Helper Class ---
class TwitchAPI:
    def __init__(self, client_id, client_secret):
        self.client_id = client_id; self.client_secret = client_secret
        self.access_token = None; self.token_expiry = datetime.now()

    def _log_api_error(self, e, response_obj, context_msg):
        logger.error(f"{context_msg}: {e}")
        if response_obj and hasattr(response_obj, 'text'):
            logger.error(f"Raw response text: {response_obj.text}")
        elif hasattr(e, 'response') and e.response is not None and hasattr(e.response, 'text'):
            logger.error(f"Response content: {e.response.text}")

    async def _get_app_access_token(self):
        if self.access_token and datetime.now() < self.token_expiry: return self.access_token
        url = "https://id.twitch.tv/oauth2/token"; params = {"client_id": self.client_id, "client_secret": self.client_secret, "grant_type": "client_credentials"}
        response_obj = None 
        try:
            response_obj = await asyncio.to_thread(requests.post, url, params=params, timeout=10); response_obj.raise_for_status()
            data = response_obj.json(); self.access_token = data['access_token']; self.token_expiry = datetime.now() + timedelta(seconds=data['expires_in'] - 300)
            logger.info("fctd.TwitchAPI: Obtained/refreshed Twitch App Access Token."); return self.access_token
        except requests.exceptions.RequestException as e: self._log_api_error(e, response_obj, "fctd.TwitchAPI: Error getting App Token"); return None
        except (KeyError, json.JSONDecodeError) as e: self._log_api_error(e, response_obj, "fctd.TwitchAPI: Error parsing App Token response"); return None

    async def get_user_id(self, username):
        token = await self._get_app_access_token(); 
        if not token: return None
        if not username: logger.warning("fctd.TwitchAPI: Attempted to get_user_id with None username."); return None
        url = f"https://api.twitch.tv/helix/users?login={username}"; headers = {"Client-ID": self.client_id, "Authorization": f"Bearer {token}"}
        response_obj = None
        try:
            response_obj = await asyncio.to_thread(requests.get, url, headers=headers, timeout=10); response_obj.raise_for_status()
            data = response_obj.json()
            if data.get('data'): return data['data'][0]['id']
            logger.warning(f"fctd.TwitchAPI: User '{username}' not found/API malformed: {data}"); return None
        except requests.exceptions.RequestException as e: self._log_api_error(e, response_obj, f"fctd.TwitchAPI: Error getting User ID for '{username}'"); return None
        except (KeyError, IndexError, json.JSONDecodeError) as e: self._log_api_error(e, response_obj, f"fctd.TwitchAPI: Error parsing User ID for '{username}'"); return None

    async def get_follower_count(self, user_id):
        token = await self._get_app_access_token()
        if not token or not user_id: return None
        url = f"https://api.twitch.tv/helix/channels/followers?broadcaster_id={user_id}"; headers = {"Client-ID": self.client_id, "Authorization": f"Bearer {token}"}
        response_obj = None
        try:
            response_obj = await asyncio.to_thread(requests.get, url, headers=headers, timeout=10); response_obj.raise_for_status()
            data = response_obj.json(); return data.get('total')
        except requests.exceptions.RequestException as e: self._log_api_error(e, response_obj, f"fctd.TwitchAPI: Error getting followers for User ID '{user_id}'"); return None
        except (KeyError, json.JSONDecodeError) as e: self._log_api_error(e, response_obj, f"fctd.TwitchAPI: Error parsing followers for User ID '{user_id}'"); return None

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
           UTA_VIEWER_COUNT_LOG_INTERVAL_SECONDS, UTA_VIEWER_COUNT_LOG_FILE

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
    if old_uta_twitch_channel_name != UTA_TWITCH_CHANNEL_NAME: logger.info(f"UTA Twitch channel name changed. Clearing broadcaster ID cache."); uta_broadcaster_id_cache = None 
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
        with open(filepath, 'ab') as f: f.write(data_bytes)
    except Exception as e: logger.error(f"Error writing binary data to {filepath}: {e}")

async def log_follower_data_binary(timestamp_dt, count):
    if FCTD_FOLLOWER_DATA_FILE:
        try:
            packed_data = struct.pack(BINARY_RECORD_FORMAT, int(timestamp_dt.timestamp()), int(count))
            await asyncio.to_thread(_write_binary_data_sync, FCTD_FOLLOWER_DATA_FILE, packed_data)
        except Exception as e: logger.error(f"Failed to log follower data to {FCTD_FOLLOWER_DATA_FILE}: {e}")

async def log_viewer_data_binary(timestamp_dt, count):
    if UTA_VIEWER_COUNT_LOGGING_ENABLED and UTA_VIEWER_COUNT_LOG_FILE:
        try:
            packed_data = struct.pack(BINARY_RECORD_FORMAT, int(timestamp_dt.timestamp()), int(count))
            await asyncio.to_thread(_write_binary_data_sync, UTA_VIEWER_COUNT_LOG_FILE, packed_data)
            logger.debug(f"UTA: Logged viewer count {count} to {UTA_VIEWER_COUNT_LOG_FILE}")
        except Exception as e: logger.error(f"UTA: Failed to log viewer count to {UTA_VIEWER_COUNT_LOG_FILE}: {e}")

async def log_stream_duration_binary(start_ts_unix: int, end_ts_unix: int):
    if UTA_STREAM_DURATION_LOG_FILE and UTA_ENABLED and UTA_RESTREAMER_ENABLED: 
        if end_ts_unix <= start_ts_unix : logger.warning(f"UTA: Invalid stream duration log: start={start_ts_unix}, end={end_ts_unix}. Skip."); return
        try:
            packed_data = struct.pack(STREAM_DURATION_RECORD_FORMAT, start_ts_unix, end_ts_unix)
            await asyncio.to_thread(_write_binary_data_sync, UTA_STREAM_DURATION_LOG_FILE, packed_data)
            logger.info(f"UTA: Logged restream duration: {datetime.fromtimestamp(start_ts_unix, tz=timezone.utc).isoformat()} to {datetime.fromtimestamp(end_ts_unix, tz=timezone.utc).isoformat()}")
        except Exception as e: logger.error(f"UTA: Failed to log stream duration to {UTA_STREAM_DURATION_LOG_FILE}: {e}")

def _pack_string_for_binary_log(s: str) -> bytes:
    s_bytes = s.encode('utf-8'); len_bytes = struct.pack(SA_STRING_LEN_FORMAT, len(s_bytes))
    return len_bytes + s_bytes

def _pack_tag_list_for_binary_log(tags: list[str]) -> bytes:
    tags_to_pack = tags if tags is not None else []
    num_tags = len(tags_to_pack)
    header_bytes = struct.pack(SA_LIST_HEADER_FORMAT, num_tags)
    tag_bytes_list = [header_bytes]
    for tag in tags_to_pack:
        tag_bytes_list.append(_pack_string_for_binary_log(tag))
    return b"".join(tag_bytes_list)

def _pack_poll_choice_list_for_binary_log(choices: list[dict], event_type: int) -> bytes:
    # event_type determines what fields are packed per choice
    choices_to_pack = choices if choices is not None else []
    num_choices = len(choices_to_pack)
    header_bytes = struct.pack(SA_LIST_HEADER_FORMAT, num_choices)
    choice_bytes_list = [header_bytes]
    for choice in choices_to_pack:
        choice_bytes_list.append(_pack_string_for_binary_log(choice.get('id', '')))
        choice_bytes_list.append(_pack_string_for_binary_log(choice.get('title', '')))
        if event_type == EVENT_TYPE_POLL_PROGRESS or event_type == EVENT_TYPE_POLL_END:
            choice_bytes_list.append(struct.pack(SA_INT_FORMAT, choice.get('votes', 0)))
            choice_bytes_list.append(struct.pack(SA_INT_FORMAT, choice.get('channel_points_votes', 0)))
            choice_bytes_list.append(struct.pack(SA_INT_FORMAT, choice.get('bits_votes', 0)))
    return b"".join(choice_bytes_list)

def _pack_prediction_outcome_list_for_binary_log(outcomes: list[dict], event_type: int) -> bytes:
    # event_type determines what fields are packed per outcome
    outcomes_to_pack = outcomes if outcomes is not None else []
    num_outcomes = len(outcomes_to_pack)
    header_bytes = struct.pack(SA_LIST_HEADER_FORMAT, num_outcomes)
    outcome_bytes_list = [header_bytes]
    for outcome in outcomes_to_pack:
        outcome_bytes_list.append(_pack_string_for_binary_log(outcome.get('id', '')))
        outcome_bytes_list.append(_pack_string_for_binary_log(outcome.get('title', '')))
        if event_type == EVENT_TYPE_PREDICTION_START:
            outcome_bytes_list.append(_pack_string_for_binary_log(outcome.get('color', '')))
        elif event_type == EVENT_TYPE_PREDICTION_PROGRESS or event_type == EVENT_TYPE_PREDICTION_END:
            outcome_bytes_list.append(struct.pack(SA_INT_FORMAT, outcome.get('users', 0)))
            outcome_bytes_list.append(struct.pack(SA_LONG_LONG_FORMAT, outcome.get('channel_points', 0)))
    return b"".join(outcome_bytes_list)


async def log_stream_activity_binary(event_type: int, timestamp_dt: datetime, **kwargs):
    # Note: Data for Ads, Polls, Predictions would ideally come from Twitch EventSub.
    # This function defines how it *would* be logged if the data is available.
    if not (UTA_STREAM_ACTIVITY_LOG_FILE and UTA_STREAM_STATUS_NOTIFICATIONS_ENABLED):
        return
    try:
        ts_unix = int(timestamp_dt.timestamp()); log_entry_bytes = struct.pack(SA_BASE_HEADER_FORMAT, event_type, ts_unix)
        if event_type == EVENT_TYPE_STREAM_START:
            title = kwargs.get("title", ""); game = kwargs.get("game", "")
            tags = kwargs.get("tags", []) 
            log_entry_bytes += _pack_string_for_binary_log(title)
            log_entry_bytes += _pack_string_for_binary_log(game)
            log_entry_bytes += _pack_tag_list_for_binary_log(tags)
        elif event_type == EVENT_TYPE_STREAM_END:
            duration = kwargs.get("duration_seconds", 0); peak_viewers = kwargs.get("peak_viewers", 0)
            log_entry_bytes += struct.pack(SA_INT_FORMAT, duration); log_entry_bytes += struct.pack(SA_INT_FORMAT, peak_viewers)
        elif event_type == EVENT_TYPE_GAME_CHANGE:
            old_game = kwargs.get("old_game", ""); new_game = kwargs.get("new_game", "")
            log_entry_bytes += _pack_string_for_binary_log(old_game); log_entry_bytes += _pack_string_for_binary_log(new_game)
        elif event_type == EVENT_TYPE_TITLE_CHANGE:
            old_title = kwargs.get("old_title", ""); new_title = kwargs.get("new_title", "")
            log_entry_bytes += _pack_string_for_binary_log(old_title); log_entry_bytes += _pack_string_for_binary_log(new_title)
        elif event_type == EVENT_TYPE_TAGS_CHANGE:
            old_tags = kwargs.get("old_tags", []); new_tags = kwargs.get("new_tags", [])
            log_entry_bytes += _pack_tag_list_for_binary_log(old_tags)
            log_entry_bytes += _pack_tag_list_for_binary_log(new_tags)
        elif event_type == EVENT_TYPE_AD_BREAK_START:
            log_entry_bytes += struct.pack(SA_INT_FORMAT, kwargs.get("duration_seconds", 0))
            log_entry_bytes += struct.pack(SA_BOOL_FORMAT, kwargs.get("is_automatic", False))
        elif event_type == EVENT_TYPE_POLL_START:
            log_entry_bytes += _pack_string_for_binary_log(kwargs.get("poll_id", ""))
            log_entry_bytes += _pack_string_for_binary_log(kwargs.get("title", ""))
            log_entry_bytes += _pack_poll_choice_list_for_binary_log(kwargs.get("choices", []), event_type)
            log_entry_bytes += struct.pack(SA_INT_FORMAT, kwargs.get("duration_seconds", 0))
            log_entry_bytes += struct.pack(SA_BOOL_FORMAT, kwargs.get("channel_points_voting_enabled", False))
            log_entry_bytes += struct.pack(SA_INT_FORMAT, kwargs.get("channel_points_per_vote", 0))
        elif event_type == EVENT_TYPE_POLL_PROGRESS:
            log_entry_bytes += _pack_string_for_binary_log(kwargs.get("poll_id", ""))
            log_entry_bytes += _pack_poll_choice_list_for_binary_log(kwargs.get("choices", []), event_type)
        elif event_type == EVENT_TYPE_POLL_END:
            log_entry_bytes += _pack_string_for_binary_log(kwargs.get("poll_id", ""))
            log_entry_bytes += _pack_string_for_binary_log(kwargs.get("title", ""))
            log_entry_bytes += _pack_poll_choice_list_for_binary_log(kwargs.get("choices", []), event_type)
            log_entry_bytes += _pack_string_for_binary_log(kwargs.get("status", ""))
            log_entry_bytes += _pack_string_for_binary_log(kwargs.get("winning_choice_id", ""))
        elif event_type == EVENT_TYPE_PREDICTION_START:
            log_entry_bytes += _pack_string_for_binary_log(kwargs.get("prediction_id", ""))
            log_entry_bytes += _pack_string_for_binary_log(kwargs.get("title", ""))
            log_entry_bytes += _pack_prediction_outcome_list_for_binary_log(kwargs.get("outcomes", []), event_type)
            log_entry_bytes += struct.pack(SA_INT_FORMAT, kwargs.get("prediction_window_seconds", 0))
        elif event_type == EVENT_TYPE_PREDICTION_PROGRESS:
            log_entry_bytes += _pack_string_for_binary_log(kwargs.get("prediction_id", ""))
            log_entry_bytes += _pack_prediction_outcome_list_for_binary_log(kwargs.get("outcomes", []), event_type)
        elif event_type == EVENT_TYPE_PREDICTION_LOCK:
            log_entry_bytes += _pack_string_for_binary_log(kwargs.get("prediction_id", ""))
            log_entry_bytes += struct.pack(SA_INT_FORMAT, kwargs.get("lock_timestamp", 0))
        elif event_type == EVENT_TYPE_PREDICTION_END:
            log_entry_bytes += _pack_string_for_binary_log(kwargs.get("prediction_id", ""))
            log_entry_bytes += _pack_string_for_binary_log(kwargs.get("title", ""))
            log_entry_bytes += _pack_string_for_binary_log(kwargs.get("winning_outcome_id", ""))
            log_entry_bytes += _pack_prediction_outcome_list_for_binary_log(kwargs.get("outcomes", []), event_type)
            log_entry_bytes += _pack_string_for_binary_log(kwargs.get("status", ""))
        else: logger.warning(f"UTA: Unknown stream activity event type for binary log: {event_type}"); return
        
        await asyncio.to_thread(_write_binary_data_sync, UTA_STREAM_ACTIVITY_LOG_FILE, log_entry_bytes)
        logger.info(f"UTA: Logged stream activity (binary): event {event_type}")
    except Exception as e: logger.error(f"UTA: Failed to log stream activity (binary) to {UTA_STREAM_ACTIVITY_LOG_FILE}: {e}", exc_info=True)

# --- Stream Activity Parsing Helper for Game Stats ---
def _read_string_from_file_handle_sync(file_handle):
    len_bytes = file_handle.read(SA_STRING_LEN_SIZE)
    if len(len_bytes) < SA_STRING_LEN_SIZE: return None, True 
    s_len = struct.unpack(SA_STRING_LEN_FORMAT, len_bytes)[0]
    s_bytes = file_handle.read(s_len)
    if len(s_bytes) < s_len: return None, True 
    return s_bytes.decode('utf-8', errors='replace'), False

def _read_tag_list_from_file_handle_sync(file_handle):
    num_tags_bytes = file_handle.read(SA_LIST_HEADER_SIZE) # Uses SA_LIST_HEADER_SIZE now
    if len(num_tags_bytes) < SA_LIST_HEADER_SIZE: return [], True
    num_tags = struct.unpack(SA_LIST_HEADER_FORMAT, num_tags_bytes)[0] # Uses SA_LIST_HEADER_FORMAT now
    
    tags_read = []
    for _ in range(num_tags):
        tag_str, incomplete = _read_string_from_file_handle_sync(file_handle)
        if incomplete: return tags_read, True 
        tags_read.append(tag_str)
    return tags_read, False

def _read_poll_choice_list_from_file_handle_sync(file_handle, event_type_for_fields: int):
    num_choices_bytes = file_handle.read(SA_LIST_HEADER_SIZE)
    if len(num_choices_bytes) < SA_LIST_HEADER_SIZE: return [], True
    num_choices = struct.unpack(SA_LIST_HEADER_FORMAT, num_choices_bytes)[0]
    
    choices_read = []
    for _ in range(num_choices):
        choice_data = {}
        choice_id, incomplete1 = _read_string_from_file_handle_sync(file_handle)
        choice_title, incomplete2 = _read_string_from_file_handle_sync(file_handle)
        if incomplete1 or incomplete2: return choices_read, True
        choice_data['id'] = choice_id
        choice_data['title'] = choice_title

        if event_type_for_fields == EVENT_TYPE_POLL_PROGRESS or event_type_for_fields == EVENT_TYPE_POLL_END:
            votes_bytes = file_handle.read(SA_INT_SIZE * 3) # votes, cp_votes, bits_votes
            if len(votes_bytes) < SA_INT_SIZE * 3: return choices_read, True
            votes, cp_votes, bits_votes = struct.unpack(f'>{SA_INT_FORMAT[1:]*3}', votes_bytes)
            choice_data.update({'votes': votes, 'channel_points_votes': cp_votes, 'bits_votes': bits_votes})
        choices_read.append(choice_data)
    return choices_read, False

def _read_prediction_outcome_list_from_file_handle_sync(file_handle, event_type_for_fields: int):
    num_outcomes_bytes = file_handle.read(SA_LIST_HEADER_SIZE)
    if len(num_outcomes_bytes) < SA_LIST_HEADER_SIZE: return [], True
    num_outcomes = struct.unpack(SA_LIST_HEADER_FORMAT, num_outcomes_bytes)[0]

    outcomes_read = []
    for _ in range(num_outcomes):
        outcome_data = {}
        outcome_id, incomplete1 = _read_string_from_file_handle_sync(file_handle)
        outcome_title, incomplete2 = _read_string_from_file_handle_sync(file_handle)
        if incomplete1 or incomplete2: return outcomes_read, True
        outcome_data['id'] = outcome_id
        outcome_data['title'] = outcome_title

        if event_type_for_fields == EVENT_TYPE_PREDICTION_START:
            outcome_color, incomplete3 = _read_string_from_file_handle_sync(file_handle)
            if incomplete3: return outcomes_read, True
            outcome_data['color'] = outcome_color
        elif event_type_for_fields == EVENT_TYPE_PREDICTION_PROGRESS or event_type_for_fields == EVENT_TYPE_PREDICTION_END:
            users_bytes = file_handle.read(SA_INT_SIZE)
            cp_bytes = file_handle.read(SA_LONG_LONG_SIZE)
            if len(users_bytes) < SA_INT_SIZE or len(cp_bytes) < SA_LONG_LONG_SIZE: return outcomes_read, True
            users = struct.unpack(SA_INT_FORMAT, users_bytes)[0]
            channel_points = struct.unpack(SA_LONG_LONG_FORMAT, cp_bytes)[0]
            outcome_data.update({'users': users, 'channel_points': channel_points})
        outcomes_read.append(outcome_data)
    return outcomes_read, False

def _consume_activity_event_body_sync(f, event_type):
    """Reads and discards the body of an event. Returns False if successful, True if incomplete read."""
    try:
        if event_type == EVENT_TYPE_STREAM_START:
            _, inc1 = _read_string_from_file_handle_sync(f); _, inc2 = _read_string_from_file_handle_sync(f)
            _, inc3 = _read_tag_list_from_file_handle_sync(f)
            return inc1 or inc2 or inc3
        elif event_type == EVENT_TYPE_STREAM_END:
            return len(f.read(SA_INT_SIZE * 2)) < SA_INT_SIZE * 2
        elif event_type == EVENT_TYPE_GAME_CHANGE:
            _, inc1 = _read_string_from_file_handle_sync(f); _, inc2 = _read_string_from_file_handle_sync(f)
            return inc1 or inc2
        elif event_type == EVENT_TYPE_TITLE_CHANGE:
            _, inc1 = _read_string_from_file_handle_sync(f); _, inc2 = _read_string_from_file_handle_sync(f)
            return inc1 or inc2
        elif event_type == EVENT_TYPE_TAGS_CHANGE:
            _, inc1 = _read_tag_list_from_file_handle_sync(f); _, inc2 = _read_tag_list_from_file_handle_sync(f)
            return inc1 or inc2
        elif event_type == EVENT_TYPE_AD_BREAK_START:
            return len(f.read(SA_INT_SIZE + SA_BOOL_SIZE)) < SA_INT_SIZE + SA_BOOL_SIZE
        elif event_type == EVENT_TYPE_POLL_START:
            _, inc1 = _read_string_from_file_handle_sync(f); _, inc2 = _read_string_from_file_handle_sync(f)
            _, inc3 = _read_poll_choice_list_from_file_handle_sync(f, event_type)
            inc4 = len(f.read(SA_INT_SIZE + SA_BOOL_SIZE + SA_INT_SIZE)) < SA_INT_SIZE + SA_BOOL_SIZE + SA_INT_SIZE
            return inc1 or inc2 or inc3 or inc4
        elif event_type == EVENT_TYPE_POLL_PROGRESS:
            _, inc1 = _read_string_from_file_handle_sync(f)
            _, inc2 = _read_poll_choice_list_from_file_handle_sync(f, event_type)
            return inc1 or inc2
        elif event_type == EVENT_TYPE_POLL_END:
            _, inc1 = _read_string_from_file_handle_sync(f); _, inc2 = _read_string_from_file_handle_sync(f)
            _, inc3 = _read_poll_choice_list_from_file_handle_sync(f, event_type)
            _, inc4 = _read_string_from_file_handle_sync(f); _, inc5 = _read_string_from_file_handle_sync(f)
            return inc1 or inc2 or inc3 or inc4 or inc5
        elif event_type == EVENT_TYPE_PREDICTION_START:
            _, inc1 = _read_string_from_file_handle_sync(f); _, inc2 = _read_string_from_file_handle_sync(f)
            _, inc3 = _read_prediction_outcome_list_from_file_handle_sync(f, event_type)
            inc4 = len(f.read(SA_INT_SIZE)) < SA_INT_SIZE
            return inc1 or inc2 or inc3 or inc4
        elif event_type == EVENT_TYPE_PREDICTION_PROGRESS:
            _, inc1 = _read_string_from_file_handle_sync(f)
            _, inc2 = _read_prediction_outcome_list_from_file_handle_sync(f, event_type)
            return inc1 or inc2
        elif event_type == EVENT_TYPE_PREDICTION_LOCK:
            _, inc1 = _read_string_from_file_handle_sync(f)
            inc2 = len(f.read(SA_INT_SIZE)) < SA_INT_SIZE
            return inc1 or inc2
        elif event_type == EVENT_TYPE_PREDICTION_END:
            _, inc1 = _read_string_from_file_handle_sync(f); _, inc2 = _read_string_from_file_handle_sync(f)
            _, inc3 = _read_string_from_file_handle_sync(f)
            _, inc4 = _read_prediction_outcome_list_from_file_handle_sync(f, event_type)
            _, inc5 = _read_string_from_file_handle_sync(f)
            return inc1 or inc2 or inc3 or inc4 or inc5
        else:
            # Unknown event type, cannot reliably consume. Better to stop.
            logger.warning(f"GameSegParser/ConsumeHelper: Unknown event type {event_type} at current file position. Cannot consume body.")
            return True # Signal error/inability to proceed
    except Exception as e:
        logger.error(f"GameSegParser/ConsumeHelper: Error consuming event type {event_type}: {e}")
        return True # Signal error

def _parse_stream_activity_for_game_segments_sync(filepath: str, query_start_unix: int = None, query_end_unix: int = None):
    """
    Parses stream_activity.bin to identify segments of gameplay for specific games.
    Returns a list of dicts: [{'game': str, 'start_ts': int, 'end_ts': int, 'title_at_start': str}]
    """
    if not filepath or not os.path.exists(filepath) or os.path.getsize(filepath) < SA_BASE_HEADER_SIZE:
        return []

    all_events = []
    try:
        with open(filepath, 'rb') as f:
            while True:
                header_chunk = f.read(SA_BASE_HEADER_SIZE)
                if not header_chunk: break
                if len(header_chunk) < SA_BASE_HEADER_SIZE:
                    logger.warning(f"GameSegParser: Incomplete activity header in {filepath}. Skipping rest."); break
                
                event_type, unix_ts = struct.unpack(SA_BASE_HEADER_FORMAT, header_chunk)
                event_data = {'type': event_type, 'timestamp': unix_ts}
                
                incomplete_body = False
                try:
                    if event_type == EVENT_TYPE_STREAM_START:
                        title, incomplete1 = _read_string_from_file_handle_sync(f)
                        game, incomplete2 = _read_string_from_file_handle_sync(f)
                        tags, incomplete3 = _read_tag_list_from_file_handle_sync(f)
                        if incomplete1 or incomplete2 or incomplete3: incomplete_body = True; logger.warning(f"GameSegParser: Incomplete STREAM_START data at {unix_ts}.")
                        else: event_data.update({'title': title, 'game': game, 'tags': tags})
                    elif event_type == EVENT_TYPE_GAME_CHANGE:
                        old_game, incomplete1 = _read_string_from_file_handle_sync(f)
                        new_game, incomplete2 = _read_string_from_file_handle_sync(f)
                        if incomplete1 or incomplete2: incomplete_body = True; logger.warning(f"GameSegParser: Incomplete GAME_CHANGE data at {unix_ts}.")
                        else: event_data.update({'old_game': old_game, 'new_game': new_game})
                    elif event_type == EVENT_TYPE_TITLE_CHANGE:
                        old_title, incomplete1 = _read_string_from_file_handle_sync(f)
                        new_title, incomplete2 = _read_string_from_file_handle_sync(f)
                        if incomplete1 or incomplete2: incomplete_body = True; logger.warning(f"GameSegParser: Incomplete TITLE_CHANGE data at {unix_ts}.")
                        else: event_data.update({'old_title': old_title, 'new_title': new_title})
                    # For other event types, GameSegParser doesn't need their data, just needs to skip them.
                    # It only appends STREAM_START, GAME_CHANGE, TITLE_CHANGE, STREAM_END to all_events for its logic.
                    else: # This includes STREAM_END and all new types
                        incomplete_body = _consume_activity_event_body_sync(f, event_type)
                        if incomplete_body: 
                            logger.warning(f"GameSegParser: Incomplete or unknown event body for type {event_type} at {unix_ts}."); 
                            # break from inner try, then outer while loop should break due to incomplete_body flag
                        if event_type == EVENT_TYPE_STREAM_END: # We still need to record STREAM_END for segmentation
                             pass # event_data already has type and timestamp

                    if incomplete_body: break # Break from while True if body was incomplete

                    # Only add events relevant to game segment parsing
                    if event_type in [EVENT_TYPE_STREAM_START, EVENT_TYPE_GAME_CHANGE, EVENT_TYPE_TITLE_CHANGE, EVENT_TYPE_STREAM_END]:
                        all_events.append(event_data)

                except struct.error as e:
                    logger.error(f"GameSegParser: Struct error processing event body (type {event_type}) at ts {unix_ts} in {filepath}: {e}"); break
                except Exception as e: 
                    logger.error(f"GameSegParser: Generic error processing event body (type {event_type}) at ts {unix_ts} in {filepath}: {e}"); break
                
                if incomplete_body: break # Break from outer while if body was incomplete
            
    except FileNotFoundError:
        logger.error(f"GameSegParser: File not found: {filepath}"); return []
    except Exception as e:
        logger.error(f"GameSegParser: Error opening or reading {filepath}: {e}"); return []
                
    all_events.sort(key=lambda x: x['timestamp'])

    segments = []
    active_stream_info = None # {'game': str, 'start_ts': int, 'title': str}

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
                logger.debug(f"GameSegParser: GAME_CHANGE event at {ts} without an active stream context. Stream might have started before log or outside query window.")
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
    """
    Finds follower/viewer counts.
    If inclusive_end_ts_for_start_val is None:
        - start_count/first_ts_unix: record at/just before cutoff_timestamp_unix.
        - end_count/last_ts_unix: latest record in the file.
    If inclusive_end_ts_for_start_val is not None (used for ranges like game sessions):
        - start_count/first_ts_unix: record at/just before cutoff_timestamp_unix (session start).
        - end_count/last_ts_unix: record at/just before inclusive_end_ts_for_start_val (session end).
    Returns: start_count, end_count, first_ts_unix, last_ts_unix, all_records_in_file (entire file content)
    """
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
        if inclusive_end_ts_for_start_val is None or temp_first_ts_candidate <= inclusive_end_ts_for_start_val : #Ensure first record is not past the range end
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
                if start_ts_unix <= ts < end_ts_unix: # Use < end_ts_unix for session
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


# =====================================================================================
# --- UTA (Universal Twitch Assistant) Integration ---
# =====================================================================================
uta_shared_access_token, uta_token_expiry_time, uta_token_refresh_lock = None, 0, threading.Lock()
uta_broadcaster_id_cache, uta_sent_clip_ids = None, set()
uta_streamlink_process, uta_ffmpeg_process, uta_is_restreaming_active = None, None, False
shutdown_event = threading.Event() 
uta_clip_thread, uta_restreamer_thread, uta_stream_status_thread = None, None, None 

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

def uta_start_restream(username):
    global uta_streamlink_process, uta_ffmpeg_process, uta_is_restreaming_active
    if not UTA_YOUTUBE_STREAM_KEY or "YOUR_YOUTUBE_STREAM_KEY" in UTA_YOUTUBE_STREAM_KEY: logger.error("UTA: YouTube Key N/A. No restream."); return False
    yt_url = f"{UTA_YOUTUBE_RTMP_URL_BASE.rstrip('/')}/{UTA_YOUTUBE_STREAM_KEY}"
    logger.info(f"UTA: Attempt restream for {username} to {UTA_YOUTUBE_RTMP_URL_BASE.rstrip('/')}/<KEY>")
    sl_cmd = [UTA_STREAMLINK_PATH, "--stdout", f"twitch.tv/{username}", "best", "--twitch-disable-hosting", "--hls-live-restart", "--retry-streams", "5", "--retry-open", "3"]
    ff_cmd = [UTA_FFMPEG_PATH, "-hide_banner", "-i", "pipe:0", "-c:v", "copy", "-c:a", "aac", "-b:a", "160k", "-map", "0:v:0?", "-map", "0:a:0?", "-f", "flv", "-bufsize", "4000k", "-flvflags", "no_duration_filesize", "-loglevel", "warning", yt_url]
    cur_slp, cur_ffp = None, None
    try:
        logger.info("UTA: Starting Streamlink..."); cur_slp = subprocess.Popen(sl_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE); uta_streamlink_process = cur_slp
        logger.info(f"UTA: Streamlink PID: {cur_slp.pid}")
        logger.info("UTA: Starting FFmpeg..."); cur_ffp = subprocess.Popen(ff_cmd, stdin=cur_slp.stdout, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE); uta_ffmpeg_process = cur_ffp
        logger.info(f"UTA: FFmpeg PID: {cur_ffp.pid}")
        if cur_slp.stdout: cur_slp.stdout.close()
        uta_is_restreaming_active = True; logger.info(f"UTA: Restreaming {username}. Monitoring...")
        ff_stderr = ""
        if cur_ffp.stderr:
            for line in iter(cur_ffp.stderr.readline, b''):
                if shutdown_event.is_set(): logger.info("UTA: Shutdown signal received, stopping FFmpeg log reading."); break
                decoded = line.decode('utf-8', errors='ignore').strip(); logger.debug(f"UTA_FFMPEG_LOG: {decoded}"); ff_stderr += decoded + "\n"
                if cur_slp and cur_slp.poll() is not None: logger.warning(f"UTA: Streamlink (PID: {cur_slp.pid}) ended (Code: {cur_slp.returncode}) during FFmpeg."); break
            cur_ffp.stderr.close()
        logger.info("UTA: Waiting for FFmpeg process to exit..."); cur_ffp.wait(); ff_exit = cur_ffp.poll()
        logger.info(f"UTA: FFmpeg (PID: {cur_ffp.pid if cur_ffp else 'N/A'}) exited with code: {ff_exit}")
        if ff_exit != 0 and ff_exit is not None:
            logger.error("UTA: --- FFmpeg Error Log ---")
            for l in ff_stderr.splitlines():
                if l.strip(): logger.error(l)
            logger.error("UTA: --- End FFmpeg Error Log ---")
        if cur_slp:
            sl_exit = cur_slp.poll()
            if sl_exit is None: logger.warning("UTA: FFmpeg exited, but Streamlink is still running. Terminating Streamlink..."); uta_terminate_process(cur_slp, "Streamlink")
            else: logger.info(f"UTA: Streamlink process had already exited with code: {sl_exit}")
            if cur_slp.stderr: 
                sl_err_bytes = b''; 
                try: sl_err_bytes = cur_slp.stderr.read()
                finally: cur_slp.stderr.close()
                if sl_err_bytes: logger.info(f"UTA: --- Streamlink Stderr Log ---\n{sl_err_bytes.decode('utf-8', errors='ignore').strip()}\n--- End Streamlink Stderr Log ---")
        return True
    except FileNotFoundError as e: logger.critical(f"UTA: ERROR: Command not found (Streamlink/FFmpeg): {e}"); return False
    except Exception as e: logger.error(f"UTA: Critical error during restreaming: {e}", exc_info=True); return False
    finally:
        if not shutdown_event.is_set(): 
            tslp, tffp = uta_streamlink_process, uta_ffmpeg_process 
            if cur_slp == tslp: uta_streamlink_process = None
            if cur_ffp == tffp: uta_ffmpeg_process = None
            uta_terminate_process(cur_ffp, "FFmpeg (start_restream finally)"); uta_terminate_process(cur_slp, "Streamlink (start_restream finally)")
        uta_is_restreaming_active = False

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
    global uta_is_restreaming_active
    logger.info(f"UTA: Restreamer Monitor thread ({threading.current_thread().name}) started.")
    active_stream_session_start_time_utc = None
    while not shutdown_event.is_set():
        try:
            if not UTA_TWITCH_CHANNEL_NAME: 
                logger.warning("UTA Restream: UTA_TWITCH_CHANNEL_NAME not set. Skipping restream check cycle.")
                if shutdown_event.wait(timeout=UTA_CHECK_INTERVAL_SECONDS_RESTREAMER): break; continue
            live, stream_data = uta_is_streamer_live(UTA_TWITCH_CHANNEL_NAME)
            if live and not uta_is_restreaming_active:
                logger.info(f"UTA Restream: {UTA_TWITCH_CHANNEL_NAME} LIVE! Starting restream...")
                active_stream_session_start_time_utc = datetime.now(timezone.utc)
                uta_send_discord_restream_status("start", UTA_TWITCH_CHANNEL_NAME, stream_data)
                if not (shutil.which(UTA_STREAMLINK_PATH) and shutil.which(UTA_FFMPEG_PATH) and \
                        UTA_YOUTUBE_STREAM_KEY and "YOUR_YOUTUBE_STREAM_KEY" not in UTA_YOUTUBE_STREAM_KEY):
                    logger.error("UTA Restream: Prerequisites for restreaming are missing. Aborting start.")
                    if shutdown_event.wait(timeout=UTA_CHECK_INTERVAL_SECONDS_RESTREAMER): break; continue
                uta_start_restream(UTA_TWITCH_CHANNEL_NAME) 
                stream_session_end_time_utc = datetime.now(timezone.utc); duration_seconds = 0
                if active_stream_session_start_time_utc: duration_seconds = (stream_session_end_time_utc - active_stream_session_start_time_utc).total_seconds()
                logger.info(f"UTA Restream: Restream attempt for {UTA_TWITCH_CHANNEL_NAME} concluded (Duration: {format_duration_human(int(duration_seconds))}).")
                if duration_seconds > 15 and active_stream_session_start_time_utc and bot.loop: 
                    asyncio.run_coroutine_threadsafe(log_stream_duration_binary(int(active_stream_session_start_time_utc.timestamp()), int(stream_session_end_time_utc.timestamp())), bot.loop)
                uta_send_discord_restream_status("stop", UTA_TWITCH_CHANNEL_NAME, stream_duration_seconds=duration_seconds)
                active_stream_session_start_time_utc = None
                logger.info(f"UTA Restream: Cooling down for {UTA_POST_RESTREAM_COOLDOWN_SECONDS}s before next check...")
                if shutdown_event.wait(timeout=UTA_POST_RESTREAM_COOLDOWN_SECONDS): break
            elif live and uta_is_restreaming_active:
                logger.info(f"UTA Restream: {UTA_TWITCH_CHANNEL_NAME} is still live. Current restream active. Check in {UTA_RESTREAM_CHECK_INTERVAL_WHEN_LIVE}s.")
                if shutdown_event.wait(timeout=UTA_RESTREAM_CHECK_INTERVAL_WHEN_LIVE): break
            elif not live and uta_is_restreaming_active:
                logger.warning(f"UTA Restream: {UTA_TWITCH_CHANNEL_NAME} offline, but restream active. Cleaning up.")
                uta_cleanup_restream_processes(); duration_seconds_on_cleanup = 0
                if active_stream_session_start_time_utc and bot.loop:
                    cleanup_time_utc = datetime.now(timezone.utc)
                    duration_seconds_on_cleanup = (cleanup_time_utc - active_stream_session_start_time_utc).total_seconds()
                    if duration_seconds_on_cleanup > 15: asyncio.run_coroutine_threadsafe(log_stream_duration_binary(int(active_stream_session_start_time_utc.timestamp()), int(cleanup_time_utc.timestamp())), bot.loop)
                uta_send_discord_restream_status("stop", UTA_TWITCH_CHANNEL_NAME, stream_duration_seconds=duration_seconds_on_cleanup)
                active_stream_session_start_time_utc = None
                logger.info(f"UTA Restream: Waiting {UTA_CHECK_INTERVAL_SECONDS_RESTREAMER}s before next check...")
                if shutdown_event.wait(timeout=UTA_CHECK_INTERVAL_SECONDS_RESTREAMER): break
            elif not live and not uta_is_restreaming_active:
                logger.info(f"UTA Restream: {UTA_TWITCH_CHANNEL_NAME} is offline. Waiting {UTA_CHECK_INTERVAL_SECONDS_RESTREAMER}s...")
                if shutdown_event.wait(timeout=UTA_CHECK_INTERVAL_SECONDS_RESTREAMER): break
        except Exception as e:
            logger.error(f"UTA Restreamer Monitor: An unexpected error in {threading.current_thread().name}: {e}", exc_info=True)
            logger.info("UTA Restreamer Monitor: Cleaning up processes due to error...")
            uta_cleanup_restream_processes(); duration_seconds_on_error = 0
            if active_stream_session_start_time_utc and bot.loop:
                error_time_utc = datetime.now(timezone.utc)
                duration_seconds_on_error = (error_time_utc - active_stream_session_start_time_utc).total_seconds()
                if duration_seconds_on_error > 15: asyncio.run_coroutine_threadsafe(log_stream_duration_binary(int(active_stream_session_start_time_utc.timestamp()), int(error_time_utc.timestamp())), bot.loop)
            uta_send_discord_restream_status("stop", UTA_TWITCH_CHANNEL_NAME, stream_duration_seconds=duration_seconds_on_error)
            active_stream_session_start_time_utc = None
            if shutdown_event.wait(timeout=60): break 
    logger.info(f"UTA: Restreamer Monitor thread ({threading.current_thread().name}) finished.")

def uta_stream_status_monitor_loop():
    # NOTE: This loop currently polls /streams. To log Ads, Polls, and Predictions,
    # this function would need to be significantly refactored to either:
    # 1. Integrate with Twitch EventSub (recommended for real-time events).
    # 2. Add polling for /ads, /polls, /predictions endpoints (less ideal due to rate limits & delay).
    # The log_stream_activity_binary function is now equipped to *store* this data if it's provided.
    logger.info(f"UTA: Stream Status Monitor thread ({threading.current_thread().name}) started.")
    is_live_status = False
    last_game_name = None; last_title = None; last_tags = None
    current_session_start_time = None; current_session_peak_viewers = 0
    last_viewer_log_time = 0

    while not shutdown_event.is_set():
        try:
            if not UTA_TWITCH_CHANNEL_NAME:
                logger.debug("UTA Status: No Twitch channel configured. Skipping status check.")
                if shutdown_event.wait(timeout=UTA_STREAM_STATUS_POLL_INTERVAL_SECONDS): break; continue

            logger.debug(f"UTA Status: Checking stream status for {UTA_TWITCH_CHANNEL_NAME}...")
            live, stream_data = uta_is_streamer_live(UTA_TWITCH_CHANNEL_NAME)
            current_utc_time = datetime.now(timezone.utc)

            if live:
                current_viewers = stream_data.get("viewer_count", 0)
                current_game_name = stream_data.get("game_name", "N/A")
                current_title = stream_data.get("title", "N/A")
                current_tags = stream_data.get("tags", []) 
                stream_started_at_str = stream_data.get("started_at")
                
                if not is_live_status: 
                    is_live_status = True
                    current_session_start_time = datetime.fromisoformat(stream_started_at_str.replace('Z', '+00:00')) if stream_started_at_str else current_utc_time
                    last_game_name = current_game_name; last_title = current_title; last_tags = list(current_tags or [])
                    current_session_peak_viewers = current_viewers
                    logger.info(f"UTA Status: {UTA_TWITCH_CHANNEL_NAME} is LIVE. Game: {current_game_name}, Title: {current_title}, Tags: {last_tags}")
                    if bot.loop: 
                        asyncio.run_coroutine_threadsafe(log_stream_activity_binary(EVENT_TYPE_STREAM_START, current_utc_time, title=current_title, game=current_game_name, tags=last_tags), bot.loop)
                        embed = discord.Embed(title=f"🔴 {UTA_TWITCH_CHANNEL_NAME} is LIVE!", description=f"**{current_title}**\nPlaying: **{current_game_name}**\n[Watch Stream](https://twitch.tv/{UTA_TWITCH_CHANNEL_NAME})", color=discord.Color.red(), timestamp=current_session_start_time)
                        if last_tags: embed.add_field(name="Tags", value=", ".join(last_tags[:8]) + ("..." if len(last_tags) > 8 else ""), inline=False)
                        asyncio.run_coroutine_threadsafe(_send_uta_notification_to_discord(None, embed=embed), bot.loop)
                    last_viewer_log_time = 0 
                else: 
                    if current_game_name != last_game_name:
                        logger.info(f"UTA Status: {UTA_TWITCH_CHANNEL_NAME} game changed from '{last_game_name}' to '{current_game_name}'.")
                        if bot.loop:
                            asyncio.run_coroutine_threadsafe(log_stream_activity_binary(EVENT_TYPE_GAME_CHANGE, current_utc_time, old_game=last_game_name, new_game=current_game_name), bot.loop)
                            embed = discord.Embed(title=f"🔄 Game Change for {UTA_TWITCH_CHANNEL_NAME}", description=f"Now playing: **{current_game_name}**\nWas: {last_game_name}\n[Watch Stream](https://twitch.tv/{UTA_TWITCH_CHANNEL_NAME})", color=discord.Color.blue(), timestamp=current_utc_time)
                            asyncio.run_coroutine_threadsafe(_send_uta_notification_to_discord(None, embed=embed), bot.loop)
                        last_game_name = current_game_name
                    if current_title != last_title:
                        logger.info(f"UTA Status: {UTA_TWITCH_CHANNEL_NAME} title changed from '{last_title}' to '{current_title}'.")
                        if bot.loop:
                            asyncio.run_coroutine_threadsafe(log_stream_activity_binary(EVENT_TYPE_TITLE_CHANGE, current_utc_time, old_title=last_title, new_title=current_title), bot.loop)
                            embed = discord.Embed(title=f"✍️ Title Change for {UTA_TWITCH_CHANNEL_NAME}", description=f"New title: **{current_title}**\n[Watch Stream](https://twitch.tv/{UTA_TWITCH_CHANNEL_NAME})", color=discord.Color.green(), timestamp=current_utc_time)
                            asyncio.run_coroutine_threadsafe(_send_uta_notification_to_discord(None, embed=embed), bot.loop)
                        last_title = current_title
                    if set(current_tags or []) != set(last_tags or []): 
                        logger.info(f"UTA Status: {UTA_TWITCH_CHANNEL_NAME} tags changed from '{last_tags}' to '{current_tags}'.")
                        if bot.loop:
                            asyncio.run_coroutine_threadsafe(log_stream_activity_binary(EVENT_TYPE_TAGS_CHANGE, current_utc_time, old_tags=last_tags, new_tags=(current_tags or [])), bot.loop)
                            embed = discord.Embed(title=f"🏷️ Tags Change for {UTA_TWITCH_CHANNEL_NAME}", color=discord.Color.orange(), timestamp=current_utc_time)
                            embed.add_field(name="Old Tags", value=", ".join(last_tags[:8]) + ("..." if len(last_tags) > 8 else "") or "None", inline=False)
                            embed.add_field(name="New Tags", value=", ".join((current_tags or [])[:8]) + ("..." if len(current_tags or []) > 8 else "") or "None", inline=False)
                            embed.add_field(name="Link", value=f"[Watch Stream](https://twitch.tv/{UTA_TWITCH_CHANNEL_NAME})", inline=False)
                            asyncio.run_coroutine_threadsafe(_send_uta_notification_to_discord(None, embed=embed), bot.loop)
                        last_tags = list(current_tags or [])


                current_session_peak_viewers = max(current_session_peak_viewers, current_viewers)
                if UTA_VIEWER_COUNT_LOGGING_ENABLED and bot.loop and (time.time() - last_viewer_log_time >= UTA_VIEWER_COUNT_LOG_INTERVAL_SECONDS):
                    asyncio.run_coroutine_threadsafe(log_viewer_data_binary(current_utc_time, current_viewers), bot.loop)
                    last_viewer_log_time = time.time()
            else: 
                if is_live_status: 
                    is_live_status = False; duration_seconds = 0
                    session_start_unix, session_end_unix = 0,0
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
                    if FCTD_FOLLOWER_DATA_FILE and FCTD_TWITCH_USERNAME and session_start_unix and session_end_unix:
                        s_foll, e_foll, _, _, _ = _read_and_find_records_sync(
                            FCTD_FOLLOWER_DATA_FILE, session_start_unix, session_end_unix
                        )
                        if s_foll is not None and e_foll is not None:
                            gain = e_foll - s_foll
                            follower_gain_str = f"{gain:+,} followers"
                        else:
                            follower_gain_str = "No follower data for session"


                    if bot.loop:
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
                        if FCTD_TWITCH_USERNAME == UTA_TWITCH_CHANNEL_NAME: 
                             embed.add_field(name="Follower Change (Session)", value=follower_gain_str, inline=False)
                        
                        asyncio.run_coroutine_threadsafe(_send_uta_notification_to_discord(None, embed=embed), bot.loop)

                    current_session_start_time = None; current_session_peak_viewers = 0; 
                    last_game_name = None; last_title = None; last_tags = None
            if shutdown_event.wait(timeout=UTA_STREAM_STATUS_POLL_INTERVAL_SECONDS): break
        except Exception as e:
            logger.error(f"UTA Stream Status Monitor: An unexpected error in {threading.current_thread().name}: {e}", exc_info=True)
            if shutdown_event.wait(timeout=60): break 
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
    global uta_clip_thread, uta_restreamer_thread, uta_stream_status_thread, uta_broadcaster_id_cache, uta_sent_clip_ids
    shutdown_event.clear(); logger.info(f"UTA: {reason} UTA threads. Shutdown event cleared.")
    uta_broadcaster_id_cache = None; uta_sent_clip_ids.clear(); logger.info(f"UTA: {reason} - Cleared broadcaster ID cache and sent clip IDs.")
    if not uta_get_twitch_access_token(): logger.critical(f"UTA: {reason} - Failed to get/refresh Twitch token for UTA. Functionality will be impaired.")
    if UTA_CLIP_MONITOR_ENABLED and UTA_TWITCH_CHANNEL_NAME:
        logger.info(f"UTA: {reason} Clip Monitor thread...")
        uta_clip_thread = threading.Thread(target=uta_clip_monitor_loop, name=f"UTAClipMon-{reason[:4]}", daemon=True); uta_clip_thread.start()
    else: logger.info(f"UTA: {reason} - Clip Monitor disabled or UTA_TWITCH_CHANNEL_NAME not set.")
    if UTA_RESTREAMER_ENABLED and UTA_TWITCH_CHANNEL_NAME and \
       shutil.which(UTA_STREAMLINK_PATH) and shutil.which(UTA_FFMPEG_PATH) and \
       UTA_YOUTUBE_STREAM_KEY and "YOUR_YOUTUBE_STREAM_KEY" not in UTA_YOUTUBE_STREAM_KEY:
        logger.info(f"UTA: {reason} Restreamer Monitor thread...")
        uta_restreamer_thread = threading.Thread(target=uta_restreamer_monitor_loop, name=f"UTARestream-{reason[:4]}", daemon=True); uta_restreamer_thread.start()
    else: logger.info(f"UTA: {reason} - Restreamer disabled (check: enabled flag, channel, paths, or YT key).")
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
    logger.info(f'Bot started at: {bot_start_time.isoformat()}'); logger.info(f'Cmd Prefix: {FCTD_COMMAND_PREFIX}')
    fctd_cmd_ch_msg = f'Listening for fctd cmds in ch ID: {FCTD_COMMAND_CHANNEL_ID}' if FCTD_COMMAND_CHANNEL_ID else 'Listening for fctd cmds in ALL channels.'
    logger.info(fctd_cmd_ch_msg); logger.info(f'Connected to {len(bot.guilds)} guilds.')
    if FCTD_TWITCH_USERNAME:
        logger.info(f'fctd: Targeting Twitch User for followers: {FCTD_TWITCH_USERNAME}')
        global fctd_current_twitch_user_id
        fctd_current_twitch_user_id = await fctd_twitch_api.get_user_id(FCTD_TWITCH_USERNAME)
        if not fctd_current_twitch_user_id: logger.error(f"fctd: CRITICAL: No Twitch User ID for {FCTD_TWITCH_USERNAME}. Follower features FAIL.")
        else:
            logger.info(f"fctd: Twitch User ID for {FCTD_TWITCH_USERNAME}: {fctd_current_twitch_user_id}")
            if FCTD_TARGET_CHANNEL_ID or FCTD_FOLLOWER_DATA_FILE:
                if not update_channel_name_and_log_followers.is_running(): update_channel_name_and_log_followers.start()
    else: logger.warning("fctd: FCTD_TWITCH_USERNAME not set. Follower tracking disabled.")
    if UTA_ENABLED:
        logger.info("--- UTA Module Enabled ---")
        if not UTA_TWITCH_CHANNEL_NAME or "target_twitch_username_for_uta" in UTA_TWITCH_CHANNEL_NAME: logger.error("UTA: UTA_TWITCH_CHANNEL_NAME not configured. UTA features limited/disabled.")
        else:
            logger.info(f"UTA: Targeting Twitch Channel: {UTA_TWITCH_CHANNEL_NAME}")
            if UTA_RESTREAMER_ENABLED: 
                if not shutil.which(UTA_STREAMLINK_PATH): logger.critical(f"UTA CRITICAL: Streamlink '{UTA_STREAMLINK_PATH}' not found. Restreamer disabled.")
                elif not shutil.which(UTA_FFMPEG_PATH): logger.critical(f"UTA CRITICAL: FFmpeg '{UTA_FFMPEG_PATH}' not found. Restreamer disabled.")
                else: logger.info("UTA: Streamlink and FFmpeg paths OK for Restreamer.")
            _start_uta_threads(reason="Initial Startup")
    else: logger.info("--- UTA Module Disabled ---")
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
@bot.command(name="uptime", help="Shows how long the bot has been running.")
async def uptime_command(ctx: commands.Context):
    if FCTD_COMMAND_CHANNEL_ID is not None and ctx.channel.id != FCTD_COMMAND_CHANNEL_ID: return
    global bot_start_time
    if bot_start_time is None: await ctx.send("Bot start time not recorded yet."); return
    uptime_delta = datetime.now(timezone.utc) - bot_start_time
    human_uptime = format_duration_human(int(uptime_delta.total_seconds()))
    embed = discord.Embed(title="Bot Uptime", description=f"I have been running for **{human_uptime}**.", color=discord.Color.green())
    embed.add_field(name="Started At", value=discord.utils.format_dt(bot_start_time, 'F'), inline=False)
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
    global config_data, fctd_twitch_api, fctd_current_twitch_user_id, uta_shared_access_token, uta_token_expiry_time, uta_broadcaster_id_cache, FCTD_UPDATE_INTERVAL_MINUTES
    await ctx.send("Attempting to reload configuration..."); logger.info(f"Configuration reload initiated by {ctx.author} (ID: {ctx.author.id}).")
    old_config_data_copy = config_data.copy() 
    success, new_loaded_data = load_config(initial_load=False) 
    if not success: await ctx.send(f"Configuration reload failed: {new_loaded_data}"); logger.error(f"Configuration reload failed: {new_loaded_data}"); return
    
    was_uta_enabled = old_config_data_copy.get('UTA_ENABLED', False)
    new_uta_enabled = new_loaded_data.get('UTA_ENABLED', False)
    
    uta_config_changed_structurally = False 
    if new_uta_enabled: 
        for key in new_loaded_data: 
            if key.startswith("UTA_") and (key.endswith("_ENABLED") or key in [
                "UTA_TWITCH_CHANNEL_NAME", "UTA_STREAMLINK_PATH", "UTA_FFMPEG_PATH", 
                "UTA_YOUTUBE_STREAM_KEY", "UTA_STREAM_STATUS_POLL_INTERVAL_SECONDS", 
                "UTA_CHECK_INTERVAL_SECONDS_CLIPS", "UTA_CHECK_INTERVAL_SECONDS_RESTREAMER",
                "UTA_CLIP_LOOKBACK_MINUTES", "UTA_RESTREAM_CHECK_INTERVAL_WHEN_LIVE",
                "UTA_POST_RESTREAM_COOLDOWN_SECONDS", "UTA_VIEWER_COUNT_LOG_INTERVAL_SECONDS"
            ]) and old_config_data_copy.get(key) != new_loaded_data.get(key):
                uta_config_changed_structurally = True; break
    
    if (was_uta_enabled != new_uta_enabled) or (new_uta_enabled and uta_config_changed_structurally):
        logger.info("Reload: UTA related configuration/status change detected. Managing UTA threads.")
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
        logger.info("Twitch client ID/secret changed. Re-init fctd.TwitchAPI, clear UTA token.")
        with uta_token_refresh_lock: uta_shared_access_token = None; uta_token_expiry_time = 0
        if FCTD_TWITCH_USERNAME: fctd_current_twitch_user_id = await fctd_twitch_api.get_user_id(FCTD_TWITCH_USERNAME); logger.info(f"Reload: Re-fetched fctd_current_twitch_user_id: {fctd_current_twitch_user_id}")

    if 'FCTD_TWITCH_USERNAME' in diff:
        logger.info(f"FCTD_TWITCH_USERNAME changed. Updating fctd_current_twitch_user_id.")
        if FCTD_TWITCH_USERNAME: fctd_current_twitch_user_id = await fctd_twitch_api.get_user_id(FCTD_TWITCH_USERNAME); logger.info(f"New fctd_current_twitch_user_id: {fctd_current_twitch_user_id}")
        else: fctd_current_twitch_user_id = None; logger.info("FCTD_TWITCH_USERNAME removed. User ID set to None.")
    
    if update_channel_name_and_log_followers.is_running() and \
       update_channel_name_and_log_followers.minutes != FCTD_UPDATE_INTERVAL_MINUTES:
        try:
            update_channel_name_and_log_followers.change_interval(minutes=FCTD_UPDATE_INTERVAL_MINUTES)
            logger.info(f"Follower update interval changed to {FCTD_UPDATE_INTERVAL_MINUTES} minutes.")
        except Exception as e:
            logger.error(f"Error changing follower task interval post-reload: {e}")
        
    should_run_fctd_task = bool(FCTD_TWITCH_USERNAME and fctd_current_twitch_user_id and (FCTD_TARGET_CHANNEL_ID or FCTD_FOLLOWER_DATA_FILE))
    if should_run_fctd_task and not update_channel_name_and_log_followers.is_running():
        logger.info("Reload: Starting follower task due to config changes.")
        update_channel_name_and_log_followers.start()
    elif not should_run_fctd_task and update_channel_name_and_log_followers.is_running():
        logger.info("Reload: Stopping follower task due to config changes.")
        update_channel_name_and_log_followers.cancel()

    if new_uta_enabled and ((was_uta_enabled != new_uta_enabled) or uta_config_changed_structurally):
        logger.info("Reload: UTA is enabled in new config and requires thread (re)start.")
        _start_uta_threads(reason="Reload")
    elif not new_uta_enabled and was_uta_enabled: 
        logger.info("Reload: UTA is now disabled. Threads were already stopped.")
    else: 
        logger.info("Reload: UTA status/config did not necessitate a thread restart.")

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
    if not time_delta:
        await ctx.send(period_name); return 

    now_utc = datetime.now(timezone.utc)
    cutoff_datetime_utc = now_utc - time_delta
    cutoff_timestamp_unix = int(cutoff_datetime_utc.timestamp())

    discord_file_to_send = None
    async with ctx.typing():
        start_c, end_c, first_ts_unix, last_ts_unix, _ = await asyncio.to_thread(
            _read_and_find_records_sync, 
            FCTD_FOLLOWER_DATA_FILE, 
            cutoff_timestamp_unix, 
            None 
        )

        if start_c is None or end_c is None or first_ts_unix is None or last_ts_unix is None or last_ts_unix <= first_ts_unix :
            await ctx.send(f"Could not retrieve sufficient distinct data points for `{period_name}` to calculate rates."); return
        
        gain = end_c - start_c
        actual_duration_seconds = last_ts_unix - first_ts_unix
        actual_duration_days = actual_duration_seconds / 86400.0

        if actual_duration_days < 1/24/4: 
            await ctx.send(f"Data range too short ({format_duration_human(actual_duration_seconds)}) to calculate meaningful rates for {period_name}."); return

        avg_per_day = gain / actual_duration_days if actual_duration_days > 0 else 0
        avg_per_week = avg_per_day * 7
        avg_per_month = avg_per_day * 30.4375 

        actual_start_dt = datetime.fromtimestamp(first_ts_unix, timezone.utc)
        actual_end_dt = datetime.fromtimestamp(last_ts_unix, timezone.utc)

        embed = discord.Embed(
            title=f"Follower Growth Rate for {FCTD_TWITCH_USERNAME}",
            description=f"Analysis period: **{period_name}**\nEffective data from {discord.utils.format_dt(actual_start_dt, 'R')} to {discord.utils.format_dt(actual_end_dt, 'R')}",
            color=discord.Color.green() if gain >=0 else discord.Color.red()
        )
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
                        if ts_plot >= (cutoff_timestamp_unix - time_delta.total_seconds()*0.1) and ts_plot <= int(now_utc.timestamp()): 
                            plot_timestamps.append(datetime.fromtimestamp(ts_plot, tz=timezone.utc))
                            plot_counts.append(count_plot)
                
                if len(plot_timestamps) > 1:
                    fig, ax = plt.subplots(figsize=(10, 5))
                    ax.plot(plot_timestamps, plot_counts, marker='.', linestyle='-', markersize=3, color='cyan')
                    ax.set_title(f"Follower Trend ({period_name})", fontsize=12)
                    ax.set_xlabel("Date/Time (UTC)", fontsize=10)
                    ax.set_ylabel("Follower Count", fontsize=10)
                    ax.grid(True, linestyle=':', alpha=0.7)
                    ax.tick_params(axis='x', labelrotation=30, labelsize=8)
                    ax.tick_params(axis='y', labelsize=8)
                    ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
                    fig.patch.set_alpha(0) 
                    ax.set_facecolor('#2C2F33') 
                    ax.spines['top'].set_visible(False); ax.spines['right'].set_visible(False)
                    ax.spines['bottom'].set_color('grey'); ax.spines['left'].set_color('grey')
                    ax.tick_params(colors='lightgrey')
                    ax.yaxis.label.set_color('lightgrey'); ax.xaxis.label.set_color('lightgrey')
                    ax.title.set_color('white')

                    img_bytes = io.BytesIO()
                    fig.savefig(img_bytes, format='png', bbox_inches='tight', facecolor=fig.get_facecolor())
                    img_bytes.seek(0)
                    plt.close(fig)
                    plot_filename = f"follrate_{FCTD_TWITCH_USERNAME}_{datetime.now().strftime('%Y%m%d%H%M')}.png"
                    discord_file_to_send = discord.File(fp=img_bytes, filename=plot_filename)
                    embed.set_image(url=f"attachment://{plot_filename}")
                else:
                    logger.info(f"Not enough data points ({len(plot_timestamps)}) for follrate plot.")
            except FileNotFoundError: logger.warning(f"follrate plot: {FCTD_FOLLOWER_DATA_FILE} not found.")
            except Exception as e_plot: logger.error(f"Error generating follrate plot: {e_plot}", exc_info=True)

    await ctx.send(embed=embed, file=discord_file_to_send if discord_file_to_send else None)


@bot.command(name="readdata", help="Dumps raw data. Keys: followers, viewers, durations, activity. Owner only.")
@commands.is_owner()
async def read_data_command(ctx: commands.Context, filename_key: str = "followers", max_records_str: str = "50"):
    if FCTD_COMMAND_CHANNEL_ID is not None and ctx.channel.id != FCTD_COMMAND_CHANNEL_ID: return
    
    filepath_to_read = None
    record_format_expected = BINARY_RECORD_FORMAT 
    record_size_expected = BINARY_RECORD_SIZE    
    is_duration_file = False; is_activity_file = False
    data_type_name = "Unknown"

    filename_key_lower = filename_key.lower()
    if filename_key_lower in ["followers", "foll"]: filepath_to_read = FCTD_FOLLOWER_DATA_FILE; data_type_name = "Follower"
    elif filename_key_lower in ["viewers", "views"]: filepath_to_read = UTA_VIEWER_COUNT_LOG_FILE; data_type_name = "Viewer Count"
    elif filename_key_lower in ["durations", "streamdurations"]:
        filepath_to_read = UTA_STREAM_DURATION_LOG_FILE
        record_format_expected = STREAM_DURATION_RECORD_FORMAT; record_size_expected = STREAM_DURATION_RECORD_SIZE
        is_duration_file = True; data_type_name = "Stream Duration"
    elif filename_key_lower in ["activity", "streamactivity"]:
        filepath_to_read = UTA_STREAM_ACTIVITY_LOG_FILE
        is_activity_file = True; data_type_name = "Stream Activity"
        record_size_expected = SA_BASE_HEADER_SIZE 
    else: await ctx.send(f"Unknown data file key '{filename_key}'. Use 'followers', 'viewers', 'durations', or 'activity'."); return

    if not filepath_to_read: await ctx.send(f"{data_type_name} data file not configured."); return
    
    try: max_r = min(max(1, int(max_records_str)), 200) 
    except ValueError: max_r = 50; await ctx.send("Invalid num for max_records, using 50.")

    async with ctx.typing():
        lines = []; basename = os.path.basename(filepath_to_read)
        chunks = []
        
        if not os.path.exists(filepath_to_read): chunks = [f"```Error: File '{filepath_to_read}' not found.```"]
        elif os.path.getsize(filepath_to_read) == 0: chunks = [f"```File '{filepath_to_read}' is empty.```"]
        elif not is_activity_file and os.path.getsize(filepath_to_read) < record_size_expected : 
            chunks = [f"```File '{basename}' too small ({os.path.getsize(filepath_to_read)}B < {record_size_expected}B for initial record).```"]
        elif is_activity_file and os.path.getsize(filepath_to_read) < SA_BASE_HEADER_SIZE:
            chunks = [f"```File '{basename}' too small ({os.path.getsize(filepath_to_read)}B < {SA_BASE_HEADER_SIZE}B for activity header).```"]
        else:
            lines.append(f"Reading: {basename}")
            if is_activity_file: lines.append(f"Format: EventType (Byte), Timestamp (Int), then event-specific data\n")
            elif is_duration_file: lines.append(f"Record size: {record_size_expected}B\nFormat: Start Timestamp (Unix), End Timestamp (Unix)\n")
            else: lines.append(f"Record size: {record_size_expected}B\nFormat: Timestamp (Unix), {data_type_name} Count\n")
            
            read_c, disp_c = 0, 0
            try:
                with open(filepath_to_read, 'rb') as f:
                    while True:
                        if disp_c >= max_r: break
                        if is_activity_file:
                            header_chunk = f.read(SA_BASE_HEADER_SIZE)
                            if not header_chunk: break
                            read_c += 1
                            if len(header_chunk) < SA_BASE_HEADER_SIZE: lines.append(f"\nWarn: Incomplete activity header ({len(header_chunk)}B)."); break
                            event_type, unix_ts = struct.unpack(SA_BASE_HEADER_FORMAT, header_chunk)
                            dt_obj = datetime.fromtimestamp(unix_ts, tz=timezone.utc)
                            line_prefix = f"{dt_obj.isoformat()} ({unix_ts}) | Event: {event_type} "
                            
                            event_desc = ""
                            incomplete_body = False
                            try:
                                if event_type == EVENT_TYPE_STREAM_START:
                                    title, inc1 = _read_string_from_file_handle_sync(f)
                                    game, inc2 = _read_string_from_file_handle_sync(f)
                                    tags, inc3 = _read_tag_list_from_file_handle_sync(f)
                                    if inc1 or inc2 or inc3: 
                                        incomplete_body=True
                                        event_desc = "INCOMPLETE STREAM_START"
                                    else: 
                                        event_desc = f"(START) | Title: '{title}' | Game: '{game}' | Tags: {tags if tags else '[]'}"
                                elif event_type == EVENT_TYPE_STREAM_END:
                                    d_bytes = f.read(SA_INT_SIZE*2)
                                    if len(d_bytes) < SA_INT_SIZE*2: 
                                        incomplete_body=True
                                        event_desc = "INCOMPLETE STREAM_END"
                                    else: 
                                        duration, peak_v = struct.unpack(f'>{SA_INT_FORMAT[1:]}{SA_INT_FORMAT[1:]}', d_bytes) 
                                        event_desc = f"(END) | Duration: {format_duration_human(duration)} | Peak Viewers: {peak_v}"
                                elif event_type == EVENT_TYPE_GAME_CHANGE:
                                    old_game, inc1 = _read_string_from_file_handle_sync(f)
                                    new_game, inc2 = _read_string_from_file_handle_sync(f)
                                    if inc1 or inc2: 
                                        incomplete_body=True
                                        event_desc = "INCOMPLETE GAME_CHANGE"
                                    else: 
                                        event_desc = f"(GAME_CHG) | From: '{old_game}' | To: '{new_game}'"
                                elif event_type == EVENT_TYPE_TITLE_CHANGE:
                                    old_title, inc1 = _read_string_from_file_handle_sync(f)
                                    new_title, inc2 = _read_string_from_file_handle_sync(f)
                                    if inc1 or inc2: 
                                        incomplete_body=True
                                        event_desc = "INCOMPLETE TITLE_CHANGE"
                                    else: 
                                        event_desc = f"(TITLE_CHG) | From: '{old_title}' | To: '{new_title}'"
                                elif event_type == EVENT_TYPE_TAGS_CHANGE:
                                    old_tags, inc1 = _read_tag_list_from_file_handle_sync(f)
                                    new_tags, inc2 = _read_tag_list_from_file_handle_sync(f)
                                    if inc1 or inc2: 
                                        incomplete_body=True
                                        event_desc = "INCOMPLETE TAGS_CHANGE"
                                    else: 
                                        event_desc = f"(TAGS_CHG) | Old: {old_tags if old_tags else '[]'} | New: {new_tags if new_tags else '[]'}"
                                elif event_type == EVENT_TYPE_AD_BREAK_START:
                                    dur_bytes = f.read(SA_INT_SIZE)
                                    auto_bytes = f.read(SA_BOOL_SIZE)
                                    if len(dur_bytes) < SA_INT_SIZE or len(auto_bytes) < SA_BOOL_SIZE: 
                                        incomplete_body=True
                                        event_desc="INCOMPLETE AD_BREAK"
                                    else: 
                                        dur = struct.unpack(SA_INT_FORMAT, dur_bytes)[0]
                                        auto = struct.unpack(SA_BOOL_FORMAT, auto_bytes)[0]
                                        event_desc = f"(AD_BREAK) | Duration: {dur}s | Auto: {auto}"
                                elif event_type == EVENT_TYPE_POLL_START:
                                    poll_id,i1=_read_string_from_file_handle_sync(f)
                                    title,i2=_read_string_from_file_handle_sync(f)
                                    choices,i3=_read_poll_choice_list_from_file_handle_sync(f, event_type)
                                    details_bytes = f.read(SA_INT_SIZE + SA_BOOL_SIZE + SA_INT_SIZE)
                                    if i1 or i2 or i3 or len(details_bytes) < (SA_INT_SIZE + SA_BOOL_SIZE + SA_INT_SIZE): 
                                        incomplete_body=True
                                        event_desc="INCOMPLETE POLL_START"
                                    else: 
                                        dur, cp_en, cp_val = struct.unpack(f">{SA_INT_FORMAT[1:]}{SA_BOOL_FORMAT[1:]}{SA_INT_FORMAT[1:]}", details_bytes)
                                        event_desc = f"(POLL_START) | ID: {poll_id} | Title: '{title}' | Choices: {len(choices)} ({'; '.join([c['title'] for c in choices[:3]])}{'...' if len(choices)>3 else ''}) | Dur: {dur}s | CP: {cp_en} (Cost: {cp_val})"
                                elif event_type == EVENT_TYPE_POLL_PROGRESS:
                                    poll_id,i1=_read_string_from_file_handle_sync(f)
                                    choices,i2=_read_poll_choice_list_from_file_handle_sync(f, event_type)
                                    if i1 or i2: 
                                        incomplete_body=True
                                        event_desc="INCOMPLETE POLL_PROGRESS"
                                    else: 
                                        event_desc = f"(POLL_PROG) | ID: {poll_id} | Choices: {len(choices)} ({'; '.join([f'{c["title"]}:{c["votes"]}' for c in choices[:2]])}{'...' if len(choices)>2 else ''})"
                                elif event_type == EVENT_TYPE_POLL_END:
                                    poll_id,i1=_read_string_from_file_handle_sync(f)
                                    title,i2=_read_string_from_file_handle_sync(f)
                                    choices,i3=_read_poll_choice_list_from_file_handle_sync(f, event_type)
                                    status,i4=_read_string_from_file_handle_sync(f)
                                    winner_id,i5=_read_string_from_file_handle_sync(f)
                                    if i1 or i2 or i3 or i4 or i5: 
                                        incomplete_body=True
                                        event_desc="INCOMPLETE POLL_END"
                                    else: 
                                        event_desc = f"(POLL_END) | ID: {poll_id} | Title: '{title}' | Status: {status} | Winner: {winner_id} | Choices: {len(choices)}"
                                elif event_type == EVENT_TYPE_PREDICTION_START:
                                    pred_id,i1=_read_string_from_file_handle_sync(f)
                                    title,i2=_read_string_from_file_handle_sync(f)
                                    outcomes,i3=_read_prediction_outcome_list_from_file_handle_sync(f, event_type)
                                    win_sec_bytes = f.read(SA_INT_SIZE)
                                    if i1 or i2 or i3 or len(win_sec_bytes) < SA_INT_SIZE: 
                                        incomplete_body=True
                                        event_desc="INCOMPLETE PRED_START"
                                    else: 
                                        win_sec = struct.unpack(SA_INT_FORMAT, win_sec_bytes)[0]
                                        event_desc = f"(PRED_START) | ID: {pred_id} | Title: '{title}' | Outcomes: {len(outcomes)} | Window: {win_sec}s"
                                elif event_type == EVENT_TYPE_PREDICTION_PROGRESS:
                                    pred_id,i1=_read_string_from_file_handle_sync(f)
                                    outcomes,i2=_read_prediction_outcome_list_from_file_handle_sync(f, event_type)
                                    if i1 or i2: 
                                        incomplete_body=True
                                        event_desc="INCOMPLETE PRED_PROGRESS"
                                    else: 
                                        event_desc = f"(PRED_PROG) | ID: {pred_id} | Outcomes: {len(outcomes)} ({'; '.join([f'{o["title"]}:{o["users"]}/{o["channel_points"]}' for o in outcomes[:2]])}{'...' if len(outcomes)>2 else ''})"
                                elif event_type == EVENT_TYPE_PREDICTION_LOCK:
                                    pred_id,i1=_read_string_from_file_handle_sync(f)
                                    lock_ts_bytes = f.read(SA_INT_SIZE)
                                    if i1 or len(lock_ts_bytes) < SA_INT_SIZE: 
                                        incomplete_body=True
                                        event_desc="INCOMPLETE PRED_LOCK"
                                    else: 
                                        lock_ts = struct.unpack(SA_INT_FORMAT, lock_ts_bytes)[0]
                                        event_desc = f"(PRED_LOCK) | ID: {pred_id} | LockedAt: {datetime.fromtimestamp(lock_ts,tz=timezone.utc).isoformat()}"
                                elif event_type == EVENT_TYPE_PREDICTION_END:
                                    pred_id,i1=_read_string_from_file_handle_sync(f)
                                    title,i2=_read_string_from_file_handle_sync(f)
                                    winner_id,i3=_read_string_from_file_handle_sync(f)
                                    outcomes,i4=_read_prediction_outcome_list_from_file_handle_sync(f, event_type)
                                    status,i5=_read_string_from_file_handle_sync(f)
                                    if i1 or i2 or i3 or i4 or i5: 
                                        incomplete_body=True
                                        event_desc="INCOMPLETE PRED_END"
                                    else: 
                                        event_desc = f"(PRED_END) | ID: {pred_id} | Title: '{title}' | Status: {status} | Winner: {winner_id} | Outcomes: {len(outcomes)}"
                                else: 
                                    event_desc = f"Unknown Event Type ({event_type}). Further parsing stopped for this record."
                                
                                lines.append(f"{line_prefix}{event_desc}")
                                if incomplete_body : break
                            except struct.error as se_inner:
                                lines.append(f"{line_prefix}Struct error parsing event body: {se_inner}"); break
                            except Exception as e_inner:
                                lines.append(f"{line_prefix}Error parsing event body: {e_inner}"); break
                        else: 
                            chunk = f.read(record_size_expected)
                            if not chunk: break
                            read_c += 1
                            if len(chunk) < record_size_expected: lines.append(f"\nWarn: Incomplete record ({len(chunk)}B)."); break
                            if is_duration_file:
                                start_ts, end_ts = struct.unpack(record_format_expected, chunk)
                                s_dt, e_dt = datetime.fromtimestamp(start_ts, tz=timezone.utc), datetime.fromtimestamp(end_ts, tz=timezone.utc)
                                lines.append(f"Start: {s_dt.isoformat()} ({start_ts}) | End: {e_dt.isoformat()} ({end_ts}) | Duration: {format_duration_human(end_ts - start_ts)}")
                            else:
                                unix_ts, count = struct.unpack(record_format_expected, chunk)
                                dt_obj = datetime.fromtimestamp(unix_ts, tz=timezone.utc)
                                lines.append(f"{dt_obj.isoformat()} ({unix_ts}) | {data_type_name}s: {count}")
                        disp_c += 1
                
                if not is_activity_file:
                    try:
                        total_recs = os.path.getsize(filepath_to_read) // record_size_expected if record_size_expected > 0 else 0
                        if disp_c < read_c or (total_recs > 0 and read_c < total_recs):
                            lines.append(f"\nDisplaying {disp_c} of {read_c} records read.")
                            if total_recs > 0 and read_c < total_recs: lines.append(f"(File has ~{total_recs} full records)")
                    except ZeroDivisionError: 
                         lines.append(f"\nDisplaying {disp_c} of {read_c} records read.")
                else: lines.append(f"\nDisplayed {disp_c} of {read_c} records read from activity log.")
                
                lines.append(f"\nTotal records displayed: {disp_c}.")
            except FileNotFoundError: chunks = [f"```Error: File '{filepath_to_read}' not found during read.```"]
            except struct.error as se: logger.error(f"Struct unpacking error in !readdata for {filepath_to_read}: {se}", exc_info=True); lines.append(f"\nError: Struct unpacking failed. ({se})")
            except Exception as e: logger.error(f"Error processing raw binary '{filepath_to_read}': {e}", exc_info=True); lines.append(f"\nError: {str(e)}")
            
            if not chunks : 
                cur_chunks, cur_chunk_str = [], "" 
                for line_item in lines:
                    if not cur_chunk_str: cur_chunk_str = line_item + "\n"; continue
                    if len(cur_chunk_str) + len(line_item) + 1 > (1990-8): 
                        if cur_chunk_str.strip(): cur_chunks.append(f"```\n{cur_chunk_str.strip()}\n```")
                        cur_chunk_str = line_item + "\n"
                    else: cur_chunk_str += line_item + "\n"
                if cur_chunk_str.strip(): cur_chunks.append(f"```\n{cur_chunk_str.strip()}\n```")
                chunks = cur_chunks if cur_chunks else ["```No data or unexpected empty.```"]

    if not chunks: await ctx.send(f"fctd: No {data_type_name.lower()} data or error. Check logs."); return
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
            if isinstance(res_foll, str): 
                embed.add_field(name=f"Followers ({target_twitch_user})", value=res_foll, inline=False)
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
                        plot_times = [datetime.fromtimestamp(r['ts'], tz=timezone.utc) for r in records_for_plot]
                        plot_counts = [r['count'] for r in records_for_plot]
                        fig, ax = plt.subplots(figsize=(8, 3)) 
                        ax.plot(plot_times, plot_counts, marker='.', linestyle='-', markersize=5, color='lightgreen')
                        ax.set_title(f"Follower Trend on {target_date_obj.isoformat()}", fontsize=10)
                        ax.set_xlabel("Time (UTC)", fontsize=8); ax.set_ylabel("Followers", fontsize=8)
                        ax.grid(True, linestyle=':', alpha=0.5)
                        ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
                        ax.tick_params(axis='x', rotation=20, labelsize=7); ax.tick_params(axis='y', labelsize=7)
                        fig.patch.set_alpha(0); ax.set_facecolor('#2C2F33')
                        ax.spines['top'].set_visible(False); ax.spines['right'].set_visible(False)
                        ax.spines['bottom'].set_color('grey'); ax.spines['left'].set_color('grey')
                        ax.tick_params(colors='lightgrey')
                        ax.yaxis.label.set_color('lightgrey'); ax.xaxis.label.set_color('lightgrey')
                        ax.title.set_color('white')
                        plt.tight_layout()
                        
                        img_bytes = io.BytesIO()
                        fig.savefig(img_bytes, format='png', bbox_inches='tight', facecolor=fig.get_facecolor())
                        img_bytes.seek(0)
                        plt.close(fig)
                        plot_filename = f"daystats_foll_{target_twitch_user}_{target_date_obj.isoformat()}.png"
                        discord_file_to_send = discord.File(fp=img_bytes, filename=plot_filename)
                        embed.set_image(url=f"attachment://{plot_filename}")
                    except Exception as e_plot: logger.error(f"Error generating daystats follower plot: {e_plot}", exc_info=True)
        elif target_twitch_user: embed.add_field(name=f"Followers ({target_twitch_user})", value="Follower data file not configured.", inline=False)
        
        if uta_target_user and UTA_STREAM_ACTIVITY_LOG_FILE and UTA_STREAM_STATUS_NOTIFICATIONS_ENABLED:
            game_segments = await asyncio.to_thread(
                _parse_stream_activity_for_game_segments_sync,
                UTA_STREAM_ACTIVITY_LOG_FILE,
                day_start_unix, day_end_unix
            )
            total_stream_time_on_day = sum(seg['end_ts'] - seg['start_ts'] for seg in game_segments)
            num_distinct_streams = 0
            if game_segments: 
                game_segments.sort(key=lambda s: s['start_ts'])
                num_distinct_streams = 1
                for i in range(1, len(game_segments)):
                    if game_segments[i]['start_ts'] - game_segments[i-1]['end_ts'] > 300: 
                        num_distinct_streams +=1

            if total_stream_time_on_day > 0:
                 embed.add_field(name=f"Total Stream Time ({uta_target_user})", 
                                 value=f"Streamed for **{format_duration_human(total_stream_time_on_day)}** across {num_distinct_streams} session(s).", 
                                 inline=False)
            else:
                 embed.add_field(name=f"Total Stream Time ({uta_target_user})", value="No streams logged via UTA activity log on this day.", inline=False)
        elif uta_target_user: embed.add_field(name=f"Total Stream Time ({uta_target_user})", value="UTA Stream Activity log not configured/enabled.", inline=False)

        if uta_target_user and UTA_VIEWER_COUNT_LOGGING_ENABLED and UTA_VIEWER_COUNT_LOG_FILE:
            avg_v, peak_v_day, num_dp = await asyncio.to_thread(
                 _get_viewer_stats_for_period_sync, UTA_VIEWER_COUNT_LOG_FILE, day_start_unix, day_end_unix
            )
            if avg_v is not None:
                embed.add_field(name=f"Avg Viewers ({uta_target_user})", value=f"{avg_v:,.1f} (from {num_dp} data points)\nPeak on day: {peak_v_day:,}", inline=False)
            else:
                embed.add_field(name=f"Avg Viewers ({uta_target_user})", value="No viewer data for this day.", inline=False)
        elif uta_target_user :
            embed.add_field(name=f"Avg Viewers ({uta_target_user})", value="Viewer logging disabled or file not found.", inline=False)


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
    
    current_tags_to_display = []
    if is_live:
        current_tags_to_display = stream_info.get("tags", [])
    elif channel_info : 
        current_tags_to_display = channel_info.get("tags", [])
    
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
    if not target_game_name:
        await ctx.send("Please provide a game name. Usage: `!gamestats \"Exact Game Name\" [period]`"); return

    query_start_unix, query_end_unix, period_name_display = None, None, "all time"
    if duration_input:
        time_delta, period_name = parse_duration_to_timedelta(duration_input)
        if not time_delta:
            await ctx.send(period_name); return 
        now_utc = datetime.now(timezone.utc)
        query_end_unix = int(now_utc.timestamp())
        query_start_unix = int((now_utc - time_delta).timestamp())
        period_name_display = period_name
    
    discord_file_to_send = None
    async with ctx.typing():
        game_segments = await asyncio.to_thread(
            _parse_stream_activity_for_game_segments_sync,
            UTA_STREAM_ACTIVITY_LOG_FILE,
            query_start_unix,
            query_end_unix
        )
        target_game_segments = [seg for seg in game_segments if seg['game'].lower() == target_game_name.lower()]

        if not target_game_segments:
            await ctx.send(f"No streaming data found for game '{target_game_name}' in {period_name_display}. Ensure exact game name from Twitch."); return

        total_time_streamed_sec = sum(seg['end_ts'] - seg['start_ts'] for seg in target_game_segments)
        
        avg_viewers_for_game, total_follower_gain_for_game = None, None
        sessions_with_follower_data = 0
        viewer_counts_for_plot = [] 
        total_viewer_datapoints_for_game = 0

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
                            if min_seg_start <= ts < max_seg_end: 
                                all_viewer_records_in_period.append({'ts': ts, 'count': count})
                except Exception as e: logger.error(f"Error reading viewer log for gamestats: {e}")
            
            if all_viewer_records_in_period:
                for seg in target_game_segments: 
                    for vr in all_viewer_records_in_period:
                        if seg['start_ts'] <= vr['ts'] < seg['end_ts']:
                             viewer_counts_for_plot.append(vr['count'])
                if viewer_counts_for_plot:
                    avg_viewers_for_game = sum(viewer_counts_for_plot) / len(viewer_counts_for_plot)
                    total_viewer_datapoints_for_game = len(viewer_counts_for_plot)


        if FCTD_FOLLOWER_DATA_FILE and os.path.exists(FCTD_FOLLOWER_DATA_FILE) and FCTD_TWITCH_USERNAME == UTA_TWITCH_CHANNEL_NAME:
            current_total_gain = 0
            for seg in target_game_segments:
                s_foll, e_foll, _, _, _ = await asyncio.to_thread(
                    _read_and_find_records_sync, FCTD_FOLLOWER_DATA_FILE, seg['start_ts'], seg['end_ts']
                )
                if s_foll is not None and e_foll is not None:
                    current_total_gain += (e_foll - s_foll)
                    sessions_with_follower_data +=1 
            total_follower_gain_for_game = current_total_gain

        embed = discord.Embed(
            title=f"Game Stats for: {target_game_name}",
            description=f"Channel: {UTA_TWITCH_CHANNEL_NAME}\nPeriod: {period_name_display}",
            color=discord.Color.blue()
        )
        embed.add_field(name="Total Time Streamed", value=format_duration_human(total_time_streamed_sec), inline=False)
        
        if avg_viewers_for_game is not None:
            embed.add_field(name="Average Viewers", value=f"{avg_viewers_for_game:,.0f} (from {total_viewer_datapoints_for_game} data points)" if total_viewer_datapoints_for_game > 0 else "No viewer data during these game sessions.", inline=True)
        else:
            embed.add_field(name="Average Viewers", value="Viewer count logging not enabled or no data.", inline=True)

        if total_follower_gain_for_game is not None:
            gain_str = f"{total_follower_gain_for_game:+,}" if total_follower_gain_for_game != 0 else "0"
            embed.add_field(name="Follower Gain During Game", value=f"{gain_str} followers ({sessions_with_follower_data} sessions w/ data)", inline=True)
        elif FCTD_TWITCH_USERNAME == UTA_TWITCH_CHANNEL_NAME : 
            embed.add_field(name="Follower Gain During Game", value="Follower count logging not enabled or no data.", inline=True)
        
        embed.set_footer(text=f"{len(target_game_segments)} play session(s) found for '{target_game_name}'.")

        if MATPLOTLIB_AVAILABLE and viewer_counts_for_plot and len(viewer_counts_for_plot) > 1:
            try:
                fig, ax = plt.subplots(figsize=(8, 4))
                ax.hist(viewer_counts_for_plot, bins=15, edgecolor='black', color='skyblue')
                ax.set_title(f"Viewer Distribution for '{target_game_name}'", fontsize=10)
                ax.set_xlabel("Viewer Count", fontsize=9); ax.set_ylabel("Frequency (Data Points)", fontsize=9)
                ax.grid(True, linestyle=':', alpha=0.5, axis='y')
                ax.tick_params(labelsize=8)
                fig.patch.set_alpha(0); ax.set_facecolor('#2C2F33')
                ax.spines['top'].set_visible(False); ax.spines['right'].set_visible(False)
                ax.spines['bottom'].set_color('grey'); ax.spines['left'].set_color('grey')
                ax.tick_params(colors='lightgrey')
                ax.yaxis.label.set_color('lightgrey'); ax.xaxis.label.set_color('lightgrey')
                ax.title.set_color('white')
                plt.tight_layout()

                img_bytes = io.BytesIO()
                fig.savefig(img_bytes, format='png', bbox_inches='tight', facecolor=fig.get_facecolor())
                img_bytes.seek(0)
                plt.close(fig)
                plot_filename = f"gamestats_viewers_{target_game_name.replace(' ','_')}_{datetime.now().strftime('%Y%m%d%H%M')}.png"
                discord_file_to_send = discord.File(fp=img_bytes, filename=plot_filename)
                embed.set_image(url=f"attachment://{plot_filename}")
            except Exception as e_plot: logger.error(f"Error generating gamestats viewer plot: {e_plot}", exc_info=True)

    await ctx.send(embed=embed, file=discord_file_to_send if discord_file_to_send else None)


@bot.command(name="exportdata", help="Exports data to CSV. Usage: !exportdata <type> [period|all]. Owner only.")
@commands.is_owner()
async def export_data_command(ctx: commands.Context, data_type: str, period_input: str = "all"):
    if FCTD_COMMAND_CHANNEL_ID is not None and ctx.channel.id != FCTD_COMMAND_CHANNEL_ID: return

    data_type = data_type.lower()
    filepath_to_export, record_format, record_size, is_activity = None, None, None, False
    headers, data_name_for_file = [], "export"

    if data_type in ["followers", "foll"]:
        filepath_to_export = FCTD_FOLLOWER_DATA_FILE
        record_format, record_size = BINARY_RECORD_FORMAT, BINARY_RECORD_SIZE
        headers = ["Timestamp", "DateTimeUTC", "FollowerCount"]
        data_name_for_file = "followers"
    elif data_type in ["viewers", "views"]:
        filepath_to_export = UTA_VIEWER_COUNT_LOG_FILE
        record_format, record_size = BINARY_RECORD_FORMAT, BINARY_RECORD_SIZE
        headers = ["Timestamp", "DateTimeUTC", "ViewerCount"]
        data_name_for_file = "viewers"
    elif data_type in ["durations", "streamdurations"]:
        filepath_to_export = UTA_STREAM_DURATION_LOG_FILE
        record_format, record_size = STREAM_DURATION_RECORD_FORMAT, STREAM_DURATION_RECORD_SIZE
        headers = ["StartTimestamp", "StartDateTimeUTC", "EndTimestamp", "EndDateTimeUTC", "DurationSeconds"]
        data_name_for_file = "stream_durations"
    elif data_type in ["activity", "streamactivity"]:
        filepath_to_export = UTA_STREAM_ACTIVITY_LOG_FILE
        is_activity = True 
        headers = ["Timestamp", "DateTimeUTC", "EventType", "Details1", "Details2", "Details3", "Details4", "Details5", "Details6", "Details7"]
        data_name_for_file = "stream_activity"
    else:
        await ctx.send("Invalid data type. Choose: `followers`, `viewers`, `durations`, `activity`."); return

    if not filepath_to_export:
        await ctx.send(f"File path for '{data_type}' not configured."); return
    if not os.path.exists(filepath_to_export) or os.path.getsize(filepath_to_export) == 0:
        await ctx.send(f"Data file '{os.path.basename(filepath_to_export)}' not found or is empty."); return

    query_start_unix, query_end_unix = None, None
    if period_input.lower() != "all":
        delta, period_name_p = parse_duration_to_timedelta(period_input)
        if not delta: await ctx.send(f"Invalid period format: '{period_input}'. Error: {period_name_p}"); return
        now_utc = datetime.now(timezone.utc)
        query_start_unix = int((now_utc - delta).timestamp())
        query_end_unix = int(now_utc.timestamp())

    await ctx.send(f"Processing `{data_name_for_file}` data for export... This might take a moment.")
    async with ctx.typing():
        csv_rows = [headers]
        try:
            with open(filepath_to_export, 'rb') as f:
                while True:
                    row_data_values = [] 
                    if is_activity:
                        header_chunk = f.read(SA_BASE_HEADER_SIZE)
                        if not header_chunk: break
                        if len(header_chunk) < SA_BASE_HEADER_SIZE: break 
                        event_type, ts = struct.unpack(SA_BASE_HEADER_FORMAT, header_chunk)
                        
                        dt_obj_iso = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
                        base_event_data = [ts, dt_obj_iso, event_type]
                        specific_event_data = [] 
                        incomplete_event = False
                        
                        try: 
                            if event_type == EVENT_TYPE_STREAM_START:
                                title,i1 = _read_string_from_file_handle_sync(f); game,i2 = _read_string_from_file_handle_sync(f)
                                tags,i3 = _read_tag_list_from_file_handle_sync(f)
                                if i1 or i2 or i3: incomplete_event=True
                                else: specific_event_data = [title, game, ",".join(tags)]
                            elif event_type == EVENT_TYPE_STREAM_END:
                                d_bytes = f.read(SA_INT_SIZE*2)
                                if len(d_bytes) < SA_INT_SIZE*2: incomplete_event=True
                                else: 
                                    dur, peak = struct.unpack(f'>{SA_INT_FORMAT[1:]}{SA_INT_FORMAT[1:]}', d_bytes)
                                    specific_event_data = [dur, peak]
                            elif event_type == EVENT_TYPE_GAME_CHANGE:
                                old_g,i1=_read_string_from_file_handle_sync(f);new_g,i2=_read_string_from_file_handle_sync(f)
                                if i1 or i2: incomplete_event=True
                                else: specific_event_data = [old_g, new_g]
                            elif event_type == EVENT_TYPE_TITLE_CHANGE:
                                old_t,i1=_read_string_from_file_handle_sync(f);new_t,i2=_read_string_from_file_handle_sync(f)
                                if i1 or i2: incomplete_event=True
                                else: specific_event_data = [old_t, new_t]
                            elif event_type == EVENT_TYPE_TAGS_CHANGE:
                                old_tags,i1 = _read_tag_list_from_file_handle_sync(f)
                                new_tags,i2 = _read_tag_list_from_file_handle_sync(f)
                                if i1 or i2: incomplete_event=True
                                else: specific_event_data = [",".join(old_tags), ",".join(new_tags)]
                            elif event_type == EVENT_TYPE_AD_BREAK_START:
                                dur_b = f.read(SA_INT_SIZE); auto_b = f.read(SA_BOOL_SIZE)
                                if len(dur_b) < SA_INT_SIZE or len(auto_b) < SA_BOOL_SIZE: incomplete_event=True
                                else: 
                                    dur=struct.unpack(SA_INT_FORMAT,dur_b)[0]
                                    auto=struct.unpack(SA_BOOL_FORMAT,auto_b)[0]
                                    specific_event_data=[dur, auto]
                            elif event_type == EVENT_TYPE_POLL_START:
                                pid,i1=_read_string_from_file_handle_sync(f);tit,i2=_read_string_from_file_handle_sync(f)
                                cho,i3=_read_poll_choice_list_from_file_handle_sync(f, event_type)
                                det_b = f.read(SA_INT_SIZE + SA_BOOL_SIZE + SA_INT_SIZE)
                                if i1 or i2 or i3 or len(det_b) < (SA_INT_SIZE+SA_BOOL_SIZE+SA_INT_SIZE): incomplete_event=True
                                else: 
                                    dr,cpe,cppv = struct.unpack(f">{SA_INT_FORMAT[1:]}{SA_BOOL_FORMAT[1:]}{SA_INT_FORMAT[1:]}", det_b)
                                    specific_event_data=[pid, tit, json.dumps(cho), dr, cpe, cppv]
                            elif event_type == EVENT_TYPE_POLL_PROGRESS:
                                pid,i1=_read_string_from_file_handle_sync(f);cho,i2=_read_poll_choice_list_from_file_handle_sync(f, event_type)
                                if i1 or i2: incomplete_event=True
                                else: specific_event_data=[pid, json.dumps(cho)]
                            elif event_type == EVENT_TYPE_POLL_END:
                                pid,i1=_read_string_from_file_handle_sync(f);tit,i2=_read_string_from_file_handle_sync(f)
                                cho,i3=_read_poll_choice_list_from_file_handle_sync(f, event_type)
                                stat,i4=_read_string_from_file_handle_sync(f);wid,i5=_read_string_from_file_handle_sync(f)
                                if i1 or i2 or i3 or i4 or i5: incomplete_event=True
                                else: specific_event_data=[pid, tit, json.dumps(cho), stat, wid]
                            elif event_type == EVENT_TYPE_PREDICTION_START:
                                pid,i1=_read_string_from_file_handle_sync(f);tit,i2=_read_string_from_file_handle_sync(f)
                                out,i3=_read_prediction_outcome_list_from_file_handle_sync(f, event_type)
                                win_b = f.read(SA_INT_SIZE)
                                if i1 or i2 or i3 or len(win_b) < SA_INT_SIZE: incomplete_event=True
                                else: 
                                    win=struct.unpack(SA_INT_FORMAT,win_b)[0]
                                    specific_event_data=[pid,tit,json.dumps(out),win]
                            elif event_type == EVENT_TYPE_PREDICTION_PROGRESS:
                                pid,i1=_read_string_from_file_handle_sync(f);out,i2=_read_prediction_outcome_list_from_file_handle_sync(f,event_type)
                                if i1 or i2: incomplete_event=True
                                else: specific_event_data=[pid, json.dumps(out)]
                            elif event_type == EVENT_TYPE_PREDICTION_LOCK:
                                pid,i1=_read_string_from_file_handle_sync(f);lts_b=f.read(SA_INT_SIZE)
                                if i1 or len(lts_b) < SA_INT_SIZE: incomplete_event=True
                                else: 
                                    lts=struct.unpack(SA_INT_FORMAT,lts_b)[0]
                                    specific_event_data=[pid, lts]
                            elif event_type == EVENT_TYPE_PREDICTION_END:
                                pid,i1=_read_string_from_file_handle_sync(f);tit,i2=_read_string_from_file_handle_sync(f)
                                wid,i3=_read_string_from_file_handle_sync(f)
                                out,i4=_read_prediction_outcome_list_from_file_handle_sync(f, event_type)
                                stat,i5=_read_string_from_file_handle_sync(f)
                                if i1 or i2 or i3 or i4 or i5: incomplete_event=True
                                else: specific_event_data=[pid,tit,wid,json.dumps(out),stat]
                            else: 
                                specific_event_data=["Unknown Event Type"] 
                                # This case should ideally be caught by _consume_activity_event_body_sync if called by other functions
                                # For export, we just mark it and try to continue if possible, or break if it implies file corruption
                                incomplete_event = _consume_activity_event_body_sync(f, event_type) # Try to skip it

                        except Exception as e_parse_body: 
                            logger.error(f"Export: Error parsing activity event body type {event_type} at {ts}: {e_parse_body}")
                            incomplete_event = True; specific_event_data=["PARSE_ERROR_BODY"]
                        
                        if incomplete_event: break 

                        if (query_start_unix and ts < query_start_unix) or \
                           (query_end_unix and ts > query_end_unix):
                            continue 

                        padded_specific_data = list(map(str, specific_event_data)) + [''] * (len(headers) - len(base_event_data) - len(specific_event_data))
                        row_data_values = base_event_data + padded_specific_data

                    else: 
                        chunk = f.read(record_size)
                        if not chunk: break
                        if len(chunk) < record_size: break 
                        
                        if data_type in ["followers", "viewers"]:
                            ts, count = struct.unpack(record_format, chunk)
                            if (query_start_unix and ts < query_start_unix) or \
                               (query_end_unix and ts > query_end_unix): continue
                            dt_obj_iso = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
                            row_data_values = [ts, dt_obj_iso, count]
                        elif data_type == "durations":
                            s_ts, e_ts = struct.unpack(record_format, chunk)
                            if query_start_unix and query_end_unix: 
                                if e_ts < query_start_unix or s_ts > query_end_unix: continue 
                            elif query_start_unix: 
                                if e_ts < query_start_unix: continue
                            elif query_end_unix: 
                                if s_ts > query_end_unix: continue
                            
                            s_dt_iso = datetime.fromtimestamp(s_ts, tz=timezone.utc).isoformat()
                            e_dt_iso = datetime.fromtimestamp(e_ts, tz=timezone.utc).isoformat()
                            row_data_values = [s_ts, s_dt_iso, e_ts, e_dt_iso, e_ts - s_ts]
                    
                    if row_data_values: csv_rows.append(row_data_values)
            
            if len(csv_rows) <= 1 : 
                await ctx.send(f"No data found for '{data_name_for_file}' in the specified period or file is effectively empty for the period."); return

            string_io = io.StringIO()
            csv_writer = csv.writer(string_io)
            csv_writer.writerows(csv_rows)
            string_io.seek(0)
            
            filename_period = re.sub(r'[^a-zA-Z0-9_.-]', '_', period_input.lower()) if period_input.lower() != "all" else "all"
            
            csv_filename = f"{data_name_for_file}_export_{filename_period}_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
            discord_file = discord.File(fp=io.BytesIO(string_io.getvalue().encode()), filename=csv_filename)
            await ctx.send(f"Export for `{data_name_for_file}` ({len(csv_rows)-1} records):", file=discord_file)

        except FileNotFoundError: await ctx.send(f"Error: File '{filepath_to_export}' not found during export.")
        except struct.error as e: await ctx.send(f"Error processing binary data: {e}. File might be corrupt or have unexpected format.")
        except Exception as e: logger.error(f"Error during data export for {data_type}: {e}", exc_info=True); await ctx.send(f"An unexpected error occurred: {e}")

# --- Plotting Commands (Optional, require matplotlib) ---
async def _send_plot_if_available(ctx, fig, filename_prefix, embed_to_attach_to=None):
    if not MATPLOTLIB_AVAILABLE:
        return None 
    
    img_bytes = io.BytesIO()
    plot_filename = f"{filename_prefix}_{datetime.now().strftime('%Y%m%d%H%M%S')}.png"
    try:
        fig.patch.set_alpha(0) 
        for ax in fig.get_axes():
            ax.set_facecolor('#2C2F33') 
            ax.spines['top'].set_visible(False)
            ax.spines['right'].set_visible(False)
            ax.spines['bottom'].set_color('grey')
            ax.spines['left'].set_color('grey')
            ax.tick_params(colors='lightgrey')
            if ax.yaxis.label.get_text(): ax.yaxis.label.set_color('lightgrey')
            if ax.xaxis.label.get_text(): ax.xaxis.label.set_color('lightgrey')
            if ax.title.get_text(): ax.title.set_color('white')

        fig.savefig(img_bytes, format='png', bbox_inches='tight', facecolor=fig.get_facecolor()) 
        img_bytes.seek(0)
        
        discord_file = discord.File(fp=img_bytes, filename=plot_filename)
        if embed_to_attach_to:
            embed_to_attach_to.set_image(url=f"attachment://{plot_filename}")
            return discord_file 
        else: 
            await ctx.send(file=discord_file)
            return None 

    except Exception as e:
        logger.error(f"Error generating or sending plot '{filename_prefix}': {e}", exc_info=True)
        await ctx.send(f"Error generating plot: {e}")
        return None
    finally:
        plt.close(fig) 


@bot.command(name="plotfollowers", help="Plots follower count over time. Usage: !plotfollowers <period>. Owner only.")
@commands.is_owner()
async def plot_followers_command(ctx: commands.Context, *, duration_input: str = None):
    if FCTD_COMMAND_CHANNEL_ID is not None and ctx.channel.id != FCTD_COMMAND_CHANNEL_ID: return
    if not MATPLOTLIB_AVAILABLE: await ctx.send("Plotting library (matplotlib) not installed."); return
    if not FCTD_FOLLOWER_DATA_FILE or not os.path.exists(FCTD_FOLLOWER_DATA_FILE):
        await ctx.send("Follower data file not found or not configured."); return
    
    period_name = "all time"
    query_start_unix = None
    now_utc_unix = int(datetime.now(timezone.utc).timestamp())

    if duration_input:
        delta, period_name_parsed = parse_duration_to_timedelta(duration_input)
        if not delta: await ctx.send(period_name_parsed); return
        query_start_unix = int((datetime.now(timezone.utc) - delta).timestamp())
        period_name = period_name_parsed

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
                    if ts > now_utc_unix + 3600 : continue 
                    timestamps.append(datetime.fromtimestamp(ts, tz=timezone.utc))
                    counts.append(count)
        except FileNotFoundError: await ctx.send(f"File {FCTD_FOLLOWER_DATA_FILE} not found."); return
        except Exception as e_read: await ctx.send(f"Error reading follower data: {e_read}"); return

        if not timestamps or len(timestamps) < 2: 
            await ctx.send("Not enough follower data found for the specified period to plot."); return

        fig, ax = plt.subplots(figsize=(12, 6))
        ax.plot(timestamps, counts, marker='.', linestyle='-', markersize=4, color='cyan')
        ax.set_title(f"Follower Count for {FCTD_TWITCH_USERNAME or 'User'} ({period_name})")
        ax.set_xlabel("Date/Time (UTC)")
        ax.set_ylabel("Follower Count")
        ax.grid(True, linestyle=':', alpha=0.7)
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M'))
        plt.xticks(rotation=45, ha="right")
        plt.tight_layout() 
        await _send_plot_if_available(ctx, fig, f"followers_{FCTD_TWITCH_USERNAME or 'user'}")

@bot.command(name="plotstreamdurations", help="Plots histogram of stream durations. Usage: !plotstreamdurations <period>. Owner only.")
@commands.is_owner()
async def plot_stream_durations_command(ctx: commands.Context, *, duration_input: str = None):
    if FCTD_COMMAND_CHANNEL_ID is not None and ctx.channel.id != FCTD_COMMAND_CHANNEL_ID: return
    if not MATPLOTLIB_AVAILABLE: await ctx.send("Plotting library (matplotlib) not installed."); return
    
    target_file, data_source_name, is_activity_log = None, "", False
    if UTA_STREAM_ACTIVITY_LOG_FILE and os.path.exists(UTA_STREAM_ACTIVITY_LOG_FILE) and UTA_STREAM_STATUS_NOTIFICATIONS_ENABLED:
        target_file = UTA_STREAM_ACTIVITY_LOG_FILE
        data_source_name = "Stream Activity Durations"
        is_activity_log = True
    elif UTA_STREAM_DURATION_LOG_FILE and os.path.exists(UTA_STREAM_DURATION_LOG_FILE):
        target_file = UTA_STREAM_DURATION_LOG_FILE
        data_source_name = "Restream Durations"
    else:
        await ctx.send(f"No suitable stream duration data file found (checked UTA Activity Log and Restream Log)."); return

    period_name = "all time"
    query_start_unix, query_end_unix = None, None
    now_utc = datetime.now(timezone.utc)
    if duration_input:
        delta, period_name_parsed = parse_duration_to_timedelta(duration_input)
        if not delta: await ctx.send(period_name_parsed); return
        query_start_unix = int((now_utc - delta).timestamp())
        query_end_unix = int(now_utc.timestamp())
        period_name = period_name_parsed
    
    await ctx.send(f"Generating {data_source_name} plot for {UTA_TWITCH_CHANNEL_NAME or 'configured channel'} ({period_name})...")
    async with ctx.typing():
        durations_hours = []
        try:
            if is_activity_log:
                all_events_in_period = []
                with open(target_file, 'rb') as f:
                    active_stream_start_ts = None
                    while True:
                        header_chunk = f.read(SA_BASE_HEADER_SIZE)
                        if not header_chunk: break
                        if len(header_chunk) < SA_BASE_HEADER_SIZE: break
                        event_type, ts = struct.unpack(SA_BASE_HEADER_FORMAT, header_chunk)

                        incomplete_body = False
                        if (query_start_unix and ts < query_start_unix - 86400*14) and event_type != EVENT_TYPE_STREAM_START : 
                             incomplete_body = _consume_activity_event_body_sync(f, event_type)
                             if incomplete_body: break
                             continue
                        if query_end_unix and ts > query_end_unix and active_stream_start_ts is None: 
                            pass


                        if event_type == EVENT_TYPE_STREAM_START:
                            active_stream_start_ts = ts
                            incomplete_body = _consume_activity_event_body_sync(f, event_type)
                        elif event_type == EVENT_TYPE_STREAM_END:
                            if active_stream_start_ts is not None:
                                stream_s = active_stream_start_ts; stream_e = ts
                                is_relevant = True
                                if query_start_unix and stream_e < query_start_unix : is_relevant = False
                                if query_end_unix and stream_s > query_end_unix : is_relevant = False
                                
                                if is_relevant:
                                    eff_s = max(stream_s, query_start_unix) if query_start_unix else stream_s
                                    eff_e = min(stream_e, query_end_unix) if query_end_unix else stream_e
                                    if eff_e > eff_s: durations_hours.append((eff_e - eff_s) / 3600.0)
                            active_stream_start_ts = None
                            incomplete_body = _consume_activity_event_body_sync(f, event_type)
                        else: 
                            incomplete_body = _consume_activity_event_body_sync(f, event_type)
                        
                        if incomplete_body: 
                            logger.warning(f"PlotStreamDurations: Incomplete body for event {event_type} at {ts}. Stopping read for this file."); break
                
                if active_stream_start_ts and (query_end_unix is None or active_stream_start_ts < query_end_unix): 
                    eff_s = max(active_stream_start_ts, query_start_unix) if query_start_unix else active_stream_start_ts
                    eff_e = query_end_unix if query_end_unix else int(now_utc.timestamp()) 
                    if eff_e > eff_s: durations_hours.append((eff_e - eff_s) / 3600.0)

            else: 
                with open(target_file, 'rb') as f:
                    while True:
                        chunk = f.read(STREAM_DURATION_RECORD_SIZE)
                        if not chunk: break
                        if len(chunk) < STREAM_DURATION_RECORD_SIZE: break
                        s_ts, e_ts = struct.unpack(STREAM_DURATION_RECORD_FORMAT, chunk)
                        
                        is_relevant = True
                        if query_start_unix and e_ts < query_start_unix : is_relevant = False
                        if query_end_unix and s_ts > query_end_unix : is_relevant = False

                        if is_relevant:
                            eff_s_ts = max(s_ts, query_start_unix) if query_start_unix else s_ts
                            eff_e_ts = min(e_ts, query_end_unix) if query_end_unix else e_ts
                            if eff_e_ts > eff_s_ts:
                                durations_hours.append((eff_e_ts - eff_s_ts) / 3600.0)
        except FileNotFoundError: await ctx.send(f"File {target_file} not found."); return
        except Exception as e_read: await ctx.send(f"Error reading duration data: {e_read}"); logger.error(f"Error in plotstreamdurations read: {e_read}", exc_info=True); return

        if not durations_hours or len(durations_hours) == 0: 
            await ctx.send(f"No {data_source_name.lower()} data found for the specified period to plot."); return

        fig, ax = plt.subplots(figsize=(10, 6))
        ax.hist(durations_hours, bins=max(1,min(15, len(set(durations_hours)) // 2 if len(set(durations_hours)) > 2 else 5)), edgecolor='black', color='skyblue')
        ax.set_title(f"Histogram of {data_source_name} for {UTA_TWITCH_CHANNEL_NAME or 'Channel'} ({period_name})")
        ax.set_xlabel("Duration (Hours)")
        ax.set_ylabel("Number of Streams")
        ax.grid(axis='y', alpha=0.75, linestyle=':')
        plt.tight_layout()
        await _send_plot_if_available(ctx, fig, f"stream_durations_hist_{UTA_TWITCH_CHANNEL_NAME or 'channel'}")


@bot.command(name="commands", aliases=['help'], help="Lists all available commands.")
async def list_commands_command(ctx: commands.Context):
    if FCTD_COMMAND_CHANNEL_ID is not None and ctx.channel.id != FCTD_COMMAND_CHANNEL_ID: return
    embed = discord.Embed(title="Bot Commands", description=f"Prefix: `{FCTD_COMMAND_PREFIX}`", color=discord.Color.blue())
    sorted_commands = sorted(bot.commands, key=lambda c: c.name)
    
    for cmd in sorted_commands:
        if cmd.hidden: continue
        if cmd.name in ["plotfollowers", "plotstreamdurations"] and not MATPLOTLIB_AVAILABLE:
            continue

        name_aliases = f"`{FCTD_COMMAND_PREFIX}{cmd.name}`"
        if cmd.aliases:
            name_aliases += f" (or {', '.join([f'`{FCTD_COMMAND_PREFIX}{a}`' for a in cmd.aliases])})"
        
        desc = cmd.help or "No description."
        embed.add_field(name=name_aliases, value=desc, inline=False)
        
    if not embed.fields: embed.description = "No commands available."
    if not MATPLOTLIB_AVAILABLE:
        embed.set_footer(text="Plotting commands are hidden as Matplotlib is not installed.")
    await ctx.send(embed=embed)

@bot.command(name="utastatus", help="Shows status of UTA modules. (Bot owner only)")
@commands.is_owner()
async def uta_status_command(ctx: commands.Context):
    if FCTD_COMMAND_CHANNEL_ID is not None and ctx.channel.id != FCTD_COMMAND_CHANNEL_ID: return
    
    embed = discord.Embed(title="Bot & UTA Module Status", color=discord.Color.orange())
    uptime_delta = datetime.now(timezone.utc) - bot_start_time
    human_uptime = format_duration_human(int(uptime_delta.total_seconds()))
    embed.add_field(name="Bot Uptime", value=f"{human_uptime} (Since: {discord.utils.format_dt(bot_start_time, 'F')})", inline=False)

    if not UTA_ENABLED: 
        embed.add_field(name="UTA Status", value="UTA module disabled in config.", inline=False)
        await ctx.send(embed=embed)
        return

    embed.add_field(name="UTA Enabled", value=str(UTA_ENABLED), inline=False)
    embed.add_field(name="Target Twitch Channel", value=UTA_TWITCH_CHANNEL_NAME or "Not Set", inline=False)
    
    clip_status = "Disabled in Config"
    if UTA_CLIP_MONITOR_ENABLED: 
        clip_thread_status = "Not Active"
        if uta_clip_thread and uta_clip_thread.is_alive(): 
            clip_thread_status = f"Active ({uta_clip_thread.name})"
        clip_status = f"Enabled. Thread: {clip_thread_status}. Sent Clips Cache: {len(uta_sent_clip_ids)}"
    embed.add_field(name="Clip Monitor", value=clip_status, inline=False)
    
    restream_status = "Disabled in Config"
    if UTA_RESTREAMER_ENABLED: 
        restream_thread_status = "Not Active"
        if uta_restreamer_thread and uta_restreamer_thread.is_alive(): 
            restream_thread_status = f"Active ({uta_restreamer_thread.name})"
        restream_status = f"Enabled. Thread: {restream_thread_status}. Currently Restreaming: {uta_is_restreaming_active}"
        if uta_is_restreaming_active:
            sl_pid = uta_streamlink_process.pid if uta_streamlink_process and hasattr(uta_streamlink_process, 'pid') else "N/A"
            ff_pid = uta_ffmpeg_process.pid if uta_ffmpeg_process and hasattr(uta_ffmpeg_process, 'pid') else "N/A"
            restream_status += f"\n  Streamlink PID: {sl_pid}, FFmpeg PID: {ff_pid}"
    embed.add_field(name="Restreamer", value=restream_status, inline=False)

    stream_status_mon_text = "Disabled in Config"
    if UTA_STREAM_STATUS_NOTIFICATIONS_ENABLED: 
        status_thread_active = "Not Active"
        if uta_stream_status_thread and uta_stream_status_thread.is_alive():
            status_thread_active = f"Active ({uta_stream_status_thread.name})"
        stream_status_mon_text = f"Enabled. Thread: {status_thread_active}."
        if UTA_VIEWER_COUNT_LOGGING_ENABLED:
            stream_status_mon_text += f"\n  Viewer Logging: Enabled (Interval: {UTA_VIEWER_COUNT_LOG_INTERVAL_SECONDS}s)"
        else:
            stream_status_mon_text += "\n  Viewer Logging: Disabled"
        stream_status_mon_text += f"\n  Activity Log File: `{UTA_STREAM_ACTIVITY_LOG_FILE}`"
    embed.add_field(name="Stream Status Monitor & Activity Logger", value=stream_status_mon_text, inline=False)
        
    token_status = "No Token or Error"
    if uta_shared_access_token and uta_token_expiry_time > 0:
        expiry_dt = datetime.fromtimestamp(uta_token_expiry_time)
        token_status = f"Token Acquired. Expires: {discord.utils.format_dt(expiry_dt, 'R')} ({discord.utils.format_dt(expiry_dt, 'f')})"
    elif uta_token_expiry_time == 0 and not uta_shared_access_token: 
        token_status = "Failed to acquire token or token expired and failed refresh."
    embed.add_field(name="UTA Twitch API Token", value=token_status, inline=False)
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
    async with bot: logger.info("Starting bot..."); await bot.start(DISCORD_TOKEN)

if __name__ == "__main__":
    if UTA_ENABLED and UTA_RESTREAMER_ENABLED:
        if not shutil.which(UTA_STREAMLINK_PATH): logger.critical(f"UTA PRE-RUN FAIL: Streamlink '{UTA_STREAMLINK_PATH}' not found."); exit(1)
        if not shutil.which(UTA_FFMPEG_PATH): logger.critical(f"UTA PRE-RUN FAIL: FFmpeg '{UTA_FFMPEG_PATH}' not found."); exit(1)
        if not UTA_YOUTUBE_STREAM_KEY or "YOUR_YOUTUBE_STREAM_KEY" in UTA_YOUTUBE_STREAM_KEY : logger.warning(f"UTA PRE-RUN WARN: YOUTUBE_STREAM_KEY not set. Restreamer will not function.")
    
    if not MATPLOTLIB_AVAILABLE:
        logger.info("NOTE: Matplotlib is not installed. Plotting commands (!plotfollowers, !plotstreamdurations) and graph attachments will be disabled. To enable them, run: pip install matplotlib")

    loop = asyncio.get_event_loop()
    try: loop.run_until_complete(main())
    except KeyboardInterrupt: logger.info("KeyboardInterrupt received. Shutting down...")
    except discord.LoginFailure: logger.critical("CRITICAL: Invalid Discord Bot Token.")
    except Exception as e: logger.critical(f"Unexpected error during bot startup/runtime: {e}", exc_info=True)
    finally:
        logger.info("Initiating final cleanup sequence...")
        if UTA_ENABLED and (uta_clip_thread or uta_restreamer_thread or uta_stream_status_thread): 
            logger.info("Main Shutdown: Setting shutdown event for UTA threads."); shutdown_event.set()
            threads_to_wait_for = []
            if uta_clip_thread and uta_clip_thread.is_alive(): threads_to_wait_for.append(uta_clip_thread)
            if uta_restreamer_thread and uta_restreamer_thread.is_alive(): threads_to_wait_for.append(uta_restreamer_thread)
            if uta_stream_status_thread and uta_stream_status_thread.is_alive(): threads_to_wait_for.append(uta_stream_status_thread)
            for t in threads_to_wait_for:
                logger.info(f"Main Shutdown: Waiting for {t.name} to exit..."); t.join(timeout=10)
                if t.is_alive(): logger.warning(f"Main Shutdown: {t.name} did not exit cleanly.")
                else: logger.info(f"Main Shutdown: {t.name} exited.")
            logger.info("Main Shutdown: Performing final UTA process cleanup."); uta_cleanup_restream_processes()
        if bot and not bot.is_closed(): logger.info("Bot connection not closed (may be handled by discord.py).")
        logger.info("Shutdown sequence finished. Exiting.")
