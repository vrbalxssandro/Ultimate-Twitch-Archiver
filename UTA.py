import discord
from discord.ext import commands, tasks
import requests
import json
import asyncio
import logging
from datetime import datetime, timedelta, timezone
import struct
import os
import re # For parsing duration strings
import time # UTA
import subprocess # UTA
import shutil # UTA
import signal # UTA
import threading # UTA

# --- Configuration Loading ---
CONFIG_FILE = 'config.json'
config_data = {}

def load_config():
    global config_data
    try:
        with open(CONFIG_FILE, 'r') as f:
            config_data = json.load(f)
        # Basic validation for essential keys
        required_keys = ['DISCORD_TOKEN', 'TWITCH_CLIENT_ID', 'TWITCH_CLIENT_SECRET']
        for key in required_keys:
            if not config_data.get(key) or "YOUR_" in str(config_data.get(key)):
                print(f"ERROR: Essential config key '{key}' is missing or has a placeholder in {CONFIG_FILE}.")
                exit(1)
        return config_data
    except FileNotFoundError:
        print(f"ERROR: {CONFIG_FILE} not found. Please create it based on the template and fill in your details.")
        exit(1)
    except json.JSONDecodeError:
        print(f"ERROR: Error decoding {CONFIG_FILE}. It's not valid JSON. Please check for syntax errors.")
        exit(1)

load_config() # Load config into global config_data

# --- Logging Setup ---
# (fctd part already sets this up well)
logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(levelname)s:%(name)s:[%(threadName)s]: %(message)s')
logger = logging.getLogger('discord_twitch_bot') # Unified logger

# --- fctd: Global Config Variables ---
DISCORD_TOKEN = config_data.get('DISCORD_TOKEN')
FCTD_TWITCH_USERNAME = config_data.get('FCTD_TWITCH_USERNAME')
FCTD_TARGET_CHANNEL_ID = int(config_data.get('FCTD_TARGET_CHANNEL_ID')) if config_data.get('FCTD_TARGET_CHANNEL_ID') else None
FCTD_COMMAND_CHANNEL_ID = int(config_data.get('FCTD_COMMAND_CHANNEL_ID')) if config_data.get('FCTD_COMMAND_CHANNEL_ID') else None
FCTD_COMMAND_PREFIX = config_data.get('FCTD_COMMAND_PREFIX', '!')
FCTD_UPDATE_INTERVAL_MINUTES = config_data.get('FCTD_UPDATE_INTERVAL_MINUTES', 2)
FCTD_CHANNEL_NAME_PREFIX = config_data.get('FCTD_CHANNEL_NAME_PREFIX', "Followers: ")
FCTD_CHANNEL_NAME_SUFFIX = config_data.get('FCTD_CHANNEL_NAME_SUFFIX', "")
FCTD_FOLLOWER_DATA_FILE = config_data.get('FCTD_FOLLOWER_DATA_FILE', "follower_counts.bin")

# Shared Twitch API Credentials
TWITCH_CLIENT_ID = config_data.get('TWITCH_CLIENT_ID')
TWITCH_CLIENT_SECRET = config_data.get('TWITCH_CLIENT_SECRET')

# --- fctd: Binary Data Constants ---
BINARY_RECORD_FORMAT = '>II'
BINARY_RECORD_SIZE = struct.calcsize(BINARY_RECORD_FORMAT)

# --- fctd: Twitch API Helper Class (for follower counts) ---
class TwitchAPI:
    def __init__(self, client_id, client_secret):
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token = None
        self.token_expiry = datetime.now()

    async def _get_app_access_token(self):
        if self.access_token and datetime.now() < self.token_expiry:
            return self.access_token
        url = "https://id.twitch.tv/oauth2/token"
        params = {"client_id": self.client_id, "client_secret": self.client_secret, "grant_type": "client_credentials"}
        try:
            # CORRECTED: Removed async with requests.Session()
            response = await asyncio.to_thread(requests.post, url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            self.access_token = data['access_token']
            self.token_expiry = datetime.now() + timedelta(seconds=data['expires_in'] - 300) # 5 min buffer
            logger.info("fctd.TwitchAPI: Successfully obtained/refreshed Twitch App Access Token.")
            return self.access_token
        except requests.exceptions.RequestException as e:
            logger.error(f"fctd.TwitchAPI: Error getting Twitch App Access Token: {e}")
            if hasattr(e, 'response') and e.response is not None: # Check if response exists on exception
                logger.error(f"fctd.TwitchAPI: Response content: {e.response.text}")
            return None
        except (KeyError, json.JSONDecodeError) as e: # Added json.JSONDecodeError
            logger.error(f"fctd.TwitchAPI: Error parsing access token response from Twitch: {e}")
            # It's useful to log the response text if parsing fails
            if 'response' in locals() and hasattr(response, 'text'):
                 logger.error(f"fctd.TwitchAPI: Raw response text: {response.text}")
            return None

    async def get_user_id(self, username):
        token = await self._get_app_access_token()
        if not token: return None
        url = f"https://api.twitch.tv/helix/users?login={username}"
        headers = {"Client-ID": self.client_id, "Authorization": f"Bearer {token}"}
        try:
            # CORRECTED: Removed async with requests.Session()
            response = await asyncio.to_thread(requests.get, url, headers=headers, timeout=10)
            response.raise_for_status()
            data = response.json()
            if data.get('data'): return data['data'][0]['id']
            else:
                logger.warning(f"fctd.TwitchAPI: Twitch user '{username}' not found or API response malformed: {data}")
                return None
        except requests.exceptions.RequestException as e:
            logger.error(f"fctd.TwitchAPI: Error getting Twitch User ID for '{username}': {e}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"fctd.TwitchAPI: Response content: {e.response.text}")
            return None
        except (KeyError, IndexError, json.JSONDecodeError) as e: # Added json.JSONDecodeError
            logger.error(f"fctd.TwitchAPI: Error parsing user ID response for '{username}': {e}")
            if 'response' in locals() and hasattr(response, 'text'):
                 logger.error(f"fctd.TwitchAPI: Raw response text: {response.text}")
            return None

    async def get_follower_count(self, user_id):
        if not user_id: return None
        token = await self._get_app_access_token()
        if not token: return None
        url = f"https://api.twitch.tv/helix/channels/followers?broadcaster_id={user_id}"
        headers = {"Client-ID": self.client_id, "Authorization": f"Bearer {token}"}
        try:
            # CORRECTED: Removed async with requests.Session()
            response = await asyncio.to_thread(requests.get, url, headers=headers, timeout=10)
            response.raise_for_status()
            data = response.json()
            return data.get('total')
        except requests.exceptions.RequestException as e:
            logger.error(f"fctd.TwitchAPI: Error getting Twitch follower count for user ID '{user_id}': {e}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"fctd.TwitchAPI: Response content: {e.response.text}")
            return None
        except (KeyError, json.JSONDecodeError) as e: # Added json.JSONDecodeError
            logger.error(f"fctd.TwitchAPI: Unexpected response format from Twitch followers API for user ID '{user_id}': {e}")
            if 'response' in locals() and hasattr(response, 'text'):
                 logger.error(f"fctd.TwitchAPI: Raw response text: {response.text}")
            return None

fctd_twitch_api = TwitchAPI(TWITCH_CLIENT_ID, TWITCH_CLIENT_SECRET)
fctd_current_twitch_user_id = None # For follower counting

# --- fctd: Data Logging Helper ---
def _write_binary_data_sync(filepath, data_bytes):
    with open(filepath, 'ab') as f:
        f.write(data_bytes)

async def log_follower_data_binary(timestamp_dt, count):
    if FCTD_FOLLOWER_DATA_FILE:
        try:
            unix_timestamp = int(timestamp_dt.timestamp())
            follower_count_int = int(count)
            packed_data = struct.pack(BINARY_RECORD_FORMAT, unix_timestamp, follower_count_int)
            await asyncio.to_thread(_write_binary_data_sync, FCTD_FOLLOWER_DATA_FILE, packed_data)
        except Exception as e:
            logger.error(f"Failed to log binary follower data to {FCTD_FOLLOWER_DATA_FILE}: {e}")

# --- fctd: Duration Parsing ---
def parse_duration_to_timedelta(duration_str: str):
    if not duration_str: return None, "No duration provided."
    duration_str = duration_str.lower().strip()
    match = re.fullmatch(r"(\d+)\s*(m|min|mins|minute|minutes|h|hr|hrs|hour|hours|d|day|days|w|wk|wks|week|weeks|mo|mon|mth|month|months|y|yr|yrs|year|years)", duration_str)
    if not match: return None, "Invalid duration format. Use N<unit> (e.g., '10min', '2days'). Type `!followers` for examples."
    value = int(match.group(1))
    unit = match.group(2)
    if value <= 0: return None, "Duration value must be > 0."
    delta = None
    period_name = ""
    if unit in ["m", "min", "mins", "minute", "minutes"]:
        delta = timedelta(minutes=value); period_name = f"last {value} minute" if value == 1 else f"last {value} minutes"
    elif unit in ["h", "hr", "hrs", "hour", "hours"]:
        delta = timedelta(hours=value); period_name = f"last {value} hour" if value == 1 else f"last {value} hours"
    elif unit in ["d", "day", "days"]:
        delta = timedelta(days=value); period_name = f"last {value} day" if value == 1 else f"last {value} days"
    elif unit in ["w", "wk", "wks", "week", "weeks"]:
        delta = timedelta(weeks=value); period_name = f"last {value} week" if value == 1 else f"last {value} weeks"
    elif unit in ["mo", "mon", "mth", "month", "months"]:
        delta = timedelta(days=value * 30); period_name = f"last {value} month (approx. {value*30}d)" if value == 1 else f"last {value} months (approx. {value*30}d)"
    elif unit in ["y", "yr", "yrs", "year", "years"]:
        delta = timedelta(days=value * 365); period_name = f"last {value} year (approx. {value*365}d)" if value == 1 else f"last {value} years (approx. {value*365}d)"
    return (delta, period_name) if delta else (None, "Internal error: Unrecognized unit.")

# --- fctd: Follower Gain Calculation Logic ---
def _read_and_find_records_sync(filepath, cutoff_timestamp_unix):
    start_count, end_count, first_ts_unix, last_ts_unix, all_records = None, None, None, None, []
    if not os.path.exists(filepath) or os.path.getsize(filepath) < BINARY_RECORD_SIZE: return None, None, None, None, None
    with open(filepath, 'rb') as f:
        while True:
            chunk = f.read(BINARY_RECORD_SIZE)
            if not chunk: break
            if len(chunk) < BINARY_RECORD_SIZE: logger.warning("Incomplete record at EOD file during read."); break
            unix_ts, count = struct.unpack(BINARY_RECORD_FORMAT, chunk)
            all_records.append((unix_ts, count))
    if not all_records: return None, None, None, None, None
    last_ts_unix, end_count = all_records[-1]
    for ts, count in reversed(all_records):
        if ts <= cutoff_timestamp_unix: start_count, first_ts_unix = count, ts; break
    if start_count is None and all_records: first_ts_unix, start_count = all_records[0]
    return start_count, end_count, first_ts_unix, last_ts_unix, all_records

async def get_follower_gain_for_period(time_delta: timedelta, period_name_full: str):
    now_utc = datetime.now(timezone.utc)
    cutoff_datetime = now_utc - time_delta
    cutoff_timestamp_unix = int(cutoff_datetime.timestamp())
    start_count, end_count, first_ts_unix, last_ts_unix, all_records_read = await asyncio.to_thread(
        _read_and_find_records_sync, FCTD_FOLLOWER_DATA_FILE, cutoff_timestamp_unix)

    if end_count is None or last_ts_unix is None: return f"Not enough data in `{FCTD_FOLLOWER_DATA_FILE}`."
    if start_count is None or first_ts_unix is None:
        if all_records_read:
            oldest_dt = datetime.fromtimestamp(all_records_read[0][0], timezone.utc)
            gain = end_count - all_records_read[0][1]
            g_msg = f"gained {gain:,}" if gain > 0 else f"lost {-gain:,}" if gain < 0 else "no change in"
            return (f"Not enough data for {period_name_full}. Oldest data: {discord.utils.format_dt(oldest_dt, 'R')}.\n"
                    f"Since then, {FCTD_TWITCH_USERNAME} has {g_msg} followers. Current: {end_count:,}")
        return "Could not determine start point."
    gain = end_count - start_count
    actual_start_dt = datetime.fromtimestamp(first_ts_unix, timezone.utc)
    actual_end_dt = datetime.fromtimestamp(last_ts_unix, timezone.utc)
    actual_span = actual_end_dt - actual_start_dt
    final_period_desc = period_name_full
    if actual_span < (time_delta * 0.8) and actual_span > timedelta(0):
        days, rem_s = actual_span.days, actual_span.seconds
        hrs, rem_s = divmod(rem_s, 3600)
        mins, _ = divmod(rem_s, 60)
        span_parts = [f"{d}d" for d in [days] if d > 0] + [f"{h}h" for h in [hrs] if h > 0] + [f"{m}m" for m in [mins] if m > 0]
        human_span = ' '.join(span_parts) if span_parts else ("<1m" if actual_span.total_seconds() > 0 else "moment")
        final_period_desc = f"{period_name_full} (data covers ~{human_span})"
    if gain == 0: return f"{FCTD_TWITCH_USERNAME} follower count stable at {end_count:,} in {final_period_desc}.\n(Data: {discord.utils.format_dt(actual_start_dt, 'R')} to {discord.utils.format_dt(actual_end_dt, 'R')})"
    g_text, c_text = ("gained", f"{abs(gain):,}") if gain > 0 else ("lost", f"{abs(gain):,}")
    return (f"{FCTD_TWITCH_USERNAME} {g_text} {c_text} followers in {final_period_desc}.\n"
            f"From {start_count:,} ({discord.utils.format_dt(actual_start_dt, 'R')}) "
            f"to {end_count:,} ({discord.utils.format_dt(actual_end_dt, 'R')}).")

# --- fctd: Raw Data Reading Function (for !readdata command) ---
def get_raw_follower_data_for_discord(filepath: str, max_records_to_display: int = 50) -> list[str]:
    lines = []
    if not os.path.exists(filepath): return [f"```Error: File '{filepath}' not found.```"]
    file_size = os.path.getsize(filepath)
    if file_size == 0: return [f"```File '{filepath}' is empty.```"]
    if file_size < BINARY_RECORD_SIZE: return [f"```File '{os.path.basename(filepath)}' too small (size: {file_size}B, req: {BINARY_RECORD_SIZE}B).```"]
    lines.extend([f"Reading: {os.path.basename(filepath)}", f"Record size: {BINARY_RECORD_SIZE}B", f"Format: Timestamp (Unix), Follower Count\n"])
    read_count, display_count = 0, 0
    try:
        with open(filepath, 'rb') as f:
            while True:
                if display_count >= max_records_to_display: break
                chunk = f.read(BINARY_RECORD_SIZE)
                if not chunk: break
                read_count += 1
                if len(chunk) < BINARY_RECORD_SIZE: lines.append(f"\nWarn: Incomplete record (got {len(chunk)}B)."); break
                unix_ts, count = struct.unpack(BINARY_RECORD_FORMAT, chunk)
                dt_obj = datetime.fromtimestamp(unix_ts, tz=timezone.utc)
                lines.append(f"{dt_obj.isoformat()} ({unix_ts}) | Followers: {count}")
                display_count += 1
        total_records = file_size // BINARY_RECORD_SIZE
        if display_count < read_count or read_count < total_records:
            lines.append(f"\nDisplaying {display_count} of {read_count} records read.")
            if read_count < total_records: lines.append(f"(File has ~{total_records} full records)")
        lines.append(f"\nTotal records displayed: {display_count}.")
    except Exception as e: logger.error(f"Error processing raw binary data file '{filepath}': {e}", exc_info=True); lines.append(f"\nError: {str(e)}")
    
    chunks, current_chunk = [], ""
    for line in lines:
        if not current_chunk: current_chunk = line + "\n"; continue
        if len(current_chunk) + len(line) + 1 > (1990 - 8): # ```\n & \n```
            if current_chunk.strip(): chunks.append(f"```\n{current_chunk.strip()}\n```")
            current_chunk = line + "\n"
        else: current_chunk += line + "\n"
    if current_chunk.strip(): chunks.append(f"```\n{current_chunk.strip()}\n```")
    if not chunks and lines: return ["```Could not format data. Check logs.```"]
    return chunks if chunks else ["```No data or unexpected empty state.```"]

# =====================================================================================
# --- UTA (Universal Twitch Assistant) Integration Start ---
# =====================================================================================

# --- UTA: Global Config Variables ---
UTA_ENABLED = config_data.get('UTA_ENABLED', False)
UTA_TWITCH_CHANNEL_NAME = config_data.get('UTA_TWITCH_CHANNEL_NAME')

UTA_CLIP_MONITOR_ENABLED = config_data.get('UTA_CLIP_MONITOR_ENABLED', False)
UTA_DISCORD_WEBHOOK_URL_CLIPS = config_data.get('UTA_DISCORD_WEBHOOK_URL_CLIPS')
UTA_CHECK_INTERVAL_SECONDS_CLIPS = config_data.get('UTA_CHECK_INTERVAL_SECONDS_CLIPS', 300)
UTA_CLIP_LOOKBACK_MINUTES = config_data.get('UTA_CLIP_LOOKBACK_MINUTES', 5)

UTA_RESTREAMER_ENABLED = config_data.get('UTA_RESTREAMER_ENABLED', False)
UTA_DISCORD_WEBHOOK_URL_RESTREAMER = config_data.get('UTA_DISCORD_WEBHOOK_URL_RESTREAMER')
UTA_YOUTUBE_RTMP_URL_BASE = config_data.get('UTA_YOUTUBE_RTMP_URL_BASE')
UTA_YOUTUBE_STREAM_KEY = config_data.get('UTA_YOUTUBE_STREAM_KEY')
UTA_CHECK_INTERVAL_SECONDS_RESTREAMER = config_data.get('UTA_CHECK_INTERVAL_SECONDS_RESTREAMER', 60)
UTA_RESTREAM_CHECK_INTERVAL_WHEN_LIVE = config_data.get('UTA_RESTREAM_CHECK_INTERVAL_WHEN_LIVE', 300)
UTA_POST_RESTREAM_COOLDOWN_SECONDS = config_data.get('UTA_POST_RESTREAM_COOLDOWN_SECONDS', 60)
UTA_STREAMLINK_PATH = config_data.get('UTA_STREAMLINK_PATH', "streamlink")
UTA_FFMPEG_PATH = config_data.get('UTA_FFMPEG_PATH', "ffmpeg")

# --- UTA: Global Constants & State ---
UTA_TWITCH_API_BASE_URL = "https://api.twitch.tv/helix"
UTA_TWITCH_AUTH_URL = "https://id.twitch.tv/oauth2/token"

# Twitch Auth (for UTA sync functions)
uta_shared_access_token = None
uta_token_expiry_time = 0
uta_token_refresh_lock = threading.Lock()

# Clip Monitor
uta_broadcaster_id_cache = None # For clips/restream target
uta_sent_clip_ids = set()

# Restreamer
uta_streamlink_process = None
uta_ffmpeg_process = None
uta_is_restreaming_active = False # Tracks if WE are actively trying to restream

# Control
shutdown_event = threading.Event() # Shared event for all threads to monitor for shutdown
uta_clip_thread = None
uta_restreamer_thread = None


# --- UTA: Twitch API Helper Functions (Synchronous, for UTA Threads) ---
def uta_get_twitch_access_token():
    global uta_shared_access_token, uta_token_expiry_time
    with uta_token_refresh_lock:
        current_time = time.time()
        if uta_shared_access_token and current_time < uta_token_expiry_time - 60:
            return uta_shared_access_token
        logger.info("UTA: Attempting to fetch/refresh Twitch API access token...")
        params = {"client_id": TWITCH_CLIENT_ID, "client_secret": TWITCH_CLIENT_SECRET, "grant_type": "client_credentials"}
        try:
            response = requests.post(UTA_TWITCH_AUTH_URL, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            uta_shared_access_token = data["access_token"]
            uta_token_expiry_time = current_time + data["expires_in"]
            logger.info("UTA: Successfully obtained/refreshed Twitch access token.")
            return uta_shared_access_token
        except requests.exceptions.RequestException as e:
            logger.error(f"UTA: Error getting Twitch access token: {e}")
            if hasattr(response, 'text'): logger.error(f"UTA: Response content: {response.text}")
            uta_shared_access_token, uta_token_expiry_time = None, 0
            return None
        except KeyError:
            logger.error(f"UTA: Error parsing access token response: {response.text if hasattr(response, 'text') else 'No response text'}")
            uta_shared_access_token, uta_token_expiry_time = None, 0
            return None

def _uta_make_twitch_api_request(endpoint, params=None, method='GET', max_retries=1):
    url = f"{UTA_TWITCH_API_BASE_URL}/{endpoint.lstrip('/')}"
    for attempt in range(max_retries + 1):
        access_token = uta_get_twitch_access_token()
        if not access_token:
            logger.error(f"UTA: Cannot make API request to {url}: No access token.")
            return None
        headers = {"Client-ID": TWITCH_CLIENT_ID, "Authorization": f"Bearer {access_token}"}
        response_obj = None # To store response for error logging
        try:
            if method.upper() == 'GET':
                response_obj = requests.get(url, headers=headers, params=params, timeout=10)
            elif method.upper() == 'POST':
                response_obj = requests.post(url, headers=headers, json=params, timeout=10)
            else:
                logger.error(f"UTA: Unsupported HTTP method: {method}"); return None
            
            if response_obj.status_code == 401 and attempt < max_retries:
                logger.warning(f"UTA: API returned 401 for {url}. Invalidating token and retrying (attempt {attempt + 1}/{max_retries + 1}).")
                global uta_shared_access_token, uta_token_expiry_time
                with uta_token_refresh_lock: uta_shared_access_token, uta_token_expiry_time = None, 0
                continue
            response_obj.raise_for_status()
            return response_obj.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"UTA: Error during API request to {url} (attempt {attempt+1}): {e}")
            if response_obj and hasattr(response_obj, 'text'): logger.error(f"UTA: Response content: {response_obj.text}")
            if attempt >= max_retries: return None
            time.sleep(2**attempt)
        except (KeyError, IndexError, json.JSONDecodeError) as e:
            logger.error(f"UTA: Error parsing API response from {url}: {e}")
            if response_obj and hasattr(response_obj, 'text'): logger.error(f"UTA: Response content: {response_obj.text}")
            return None
    logger.error(f"UTA: Failed request to {url} after {max_retries + 1} attempts.")
    return None

def uta_get_broadcaster_id(channel_name):
    global uta_broadcaster_id_cache
    if uta_broadcaster_id_cache: return uta_broadcaster_id_cache
    data = _uta_make_twitch_api_request("/users", params={"login": channel_name})
    if data and data.get("data"):
        uta_broadcaster_id_cache = data["data"][0]["id"]
        logger.info(f"UTA: Found broadcaster ID for {channel_name}: {uta_broadcaster_id_cache}")
        return uta_broadcaster_id_cache
    logger.error(f"UTA: Could not find broadcaster ID for channel: {channel_name}"); return None

def uta_get_recent_clips(broadcaster_id, lookback_minutes):
    if not broadcaster_id: logger.error("UTA: Cannot get clips without broadcaster ID."); return []
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(minutes=lookback_minutes)
    params = {"broadcaster_id": broadcaster_id, "started_at": start_time.isoformat("T") + "Z", "first": 20}
    data = _uta_make_twitch_api_request("/clips", params=params)
    return data.get("data", []) if data else []

def uta_is_streamer_live(channel_name):
    params = {"user_login": channel_name}
    data = _uta_make_twitch_api_request("/streams", params=params)
    if data and data.get("data") and data["data"][0].get("type") == "live":
        return True, data["data"][0]
    return False, None

# --- UTA: Discord Webhook Functions ---
def uta_send_discord_clip_notification(clip_url, clip_title, channel_name):
    if not UTA_DISCORD_WEBHOOK_URL_CLIPS or "YOUR_DISCORD_WEBHOOK_URL" in UTA_DISCORD_WEBHOOK_URL_CLIPS:
        logger.warning("UTA: Discord webhook URL for clips not configured. Skipping notification.")
        return
    message = f"🎬 New clip from **{channel_name}**!\n**{clip_title}**\n{clip_url}"
    payload = {"content": message}
    try:
        response = requests.post(UTA_DISCORD_WEBHOOK_URL_CLIPS, json=payload, timeout=10)
        response.raise_for_status()
        logger.info(f"UTA: Successfully sent clip to Discord: {clip_url}")
    except requests.exceptions.RequestException as e:
        logger.error(f"UTA: Error sending clip notification to Discord: {e}")
        if hasattr(response, 'text'): logger.error(f"UTA: Response content: {response.text}")

def uta_send_discord_restream_status(status_type, username, stream_data=None):
    if not UTA_DISCORD_WEBHOOK_URL_RESTREAMER or "YOUR_DISCORD_WEBHOOK_URL" in UTA_DISCORD_WEBHOOK_URL_RESTREAMER:
        logger.warning("UTA: Discord webhook URL for restreamer not configured. Skipping notification.")
        return
    color = 15158332 if status_type == "stop" else 3066993 # Red/Green
    title_prefix = ":stop_button: Restream STOPPED" if status_type == "stop" else ":satellite: Restream STARTED"
    description = f"Restreaming of **{username}**'s Twitch stream to YouTube has stopped."
    if status_type == "start":
        stream_title = stream_data.get("title", "No Title") if stream_data else "N/A"
        game_name = stream_data.get("game_name", "N/A") if stream_data else "N/A"
        description = (f"Now restreaming **{username}** to YouTube.\n"
                       f"Twitch Title: **{stream_title}**\nGame: **{game_name}**\n"
                       f"[Watch on Twitch](https://twitch.tv/{username})")
    payload = {"content": f"{title_prefix} for **{username}**",
               "embeds": [{"title": title_prefix, "description": description, "color": color,
                           "timestamp": datetime.utcnow().isoformat(),
                           "author": {"name": username, "url": f"https://twitch.tv/{username}"},
                           "footer": {"text": "Twitch Monitor & Restreamer (UTA)"}}]}
    try:
        response = requests.post(UTA_DISCORD_WEBHOOK_URL_RESTREAMER, json=payload, timeout=10)
        response.raise_for_status()
        logger.info(f"UTA: Sent Discord notification for restream {status_type} for {username}")
    except requests.exceptions.RequestException as e:
        logger.error(f"UTA: Error sending restream status to Discord: {e}")
        if hasattr(response, 'text'): logger.error(f"UTA: Response content: {response.text}")

# --- UTA: Restreamer Core Functions ---
def uta_terminate_process(process, name):
    if process and process.poll() is None:
        logger.info(f"UTA: Terminating {name} process (PID: {process.pid})...")
        try:
            process.terminate(); process.wait(timeout=10)
            logger.info(f"UTA: {name} process terminated (Exit Code: {process.poll()}).")
        except subprocess.TimeoutExpired:
            logger.warning(f"UTA: {name} (PID: {process.pid}) did not terminate gracefully, killing..."); process.kill(); process.wait()
            logger.info(f"UTA: {name} process killed (Exit Code: {process.poll()}).")
        except Exception as e: logger.error(f"UTA: Error during {name} process termination (PID: {process.pid}): {e}")

def uta_cleanup_restream_processes():
    global uta_streamlink_process, uta_ffmpeg_process, uta_is_restreaming_active
    logger.info("UTA: Cleaning up restream processes...")
    uta_terminate_process(uta_ffmpeg_process, "FFmpeg")
    uta_ffmpeg_process = None
    uta_terminate_process(uta_streamlink_process, "Streamlink")
    uta_streamlink_process = None
    uta_is_restreaming_active = False
    logger.info("UTA: Restream process cleanup finished.")

def uta_start_restream(username):
    global uta_streamlink_process, uta_ffmpeg_process, uta_is_restreaming_active

    if not UTA_YOUTUBE_STREAM_KEY or "YOUR_YOUTUBE_STREAM_KEY" in UTA_YOUTUBE_STREAM_KEY:
        logger.error("UTA: YouTube Stream Key missing/placeholder. Cannot start restream.")
        return False

    stream_url_twitch = f"twitch.tv/{username}"
    youtube_rtmp_full_url = f"{UTA_YOUTUBE_RTMP_URL_BASE.rstrip('/')}/{UTA_YOUTUBE_STREAM_KEY}"
    logger.info(f"UTA: Attempting restream for {username} to {UTA_YOUTUBE_RTMP_URL_BASE.rstrip('/')}/<KEY>")

    sl_command = [UTA_STREAMLINK_PATH, "--stdout", stream_url_twitch, "best", "--twitch-disable-hosting",
                  "--hls-live-restart", "--retry-streams", "5", "--retry-open", "3"]
    ffmpeg_command = [UTA_FFMPEG_PATH, "-hide_banner", "-i", "pipe:0", "-c:v", "copy", "-c:a", "aac",
                      "-b:a", "160k", "-map", "0:v:0?", "-map", "0:a:0?", "-f", "flv", "-bufsize", "4000k",
                      "-flvflags", "no_duration_filesize", "-loglevel", "warning", youtube_rtmp_full_url]
    
    current_slp, current_ffp = None, None # Local references for this specific attempt

    try:
        logger.info("UTA: Starting Streamlink process..."); current_slp = subprocess.Popen(sl_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        uta_streamlink_process = current_slp # Assign to global
        logger.info(f"UTA: Streamlink PID: {current_slp.pid}")
        logger.info("UTA: Starting FFmpeg process..."); current_ffp = subprocess.Popen(ffmpeg_command, stdin=current_slp.stdout, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        uta_ffmpeg_process = current_ffp # Assign to global
        logger.info(f"UTA: FFmpeg PID: {current_ffp.pid}")
        if current_slp.stdout: current_slp.stdout.close()
        
        uta_is_restreaming_active = True
        logger.info(f"UTA: Restreaming started for {username}. Monitoring FFmpeg...")
        
        ffmpeg_stderr_output = ""
        if current_ffp.stderr:
            for line in iter(current_ffp.stderr.readline, b''):
                if shutdown_event.is_set(): logger.info("UTA: Shutdown signal, stopping FFmpeg log reading."); break
                decoded_line = line.decode('utf-8', errors='ignore').strip()
                logger.debug(f"UTA_FFMPEG_LOG: {decoded_line}")
                ffmpeg_stderr_output += decoded_line + "\n"
                if current_slp and current_slp.poll() is not None:
                    logger.warning(f"UTA: Streamlink (PID: {current_slp.pid}) ended unexpectedly (Code: {current_slp.returncode}) while FFmpeg ran.")
                    break
            current_ffp.stderr.close()

        logger.info("UTA: Waiting for FFmpeg process to exit..."); current_ffp.wait()
        ff_exit_code = current_ffp.poll()
        logger.info(f"UTA: FFmpeg (PID: {current_ffp.pid if current_ffp else 'N/A'}) exited code: {ff_exit_code}")
        if ff_exit_code != 0 and ff_exit_code is not None:
             logger.error("UTA: --- FFmpeg Error Log ---"); [logger.error(line) for line in ffmpeg_stderr_output.splitlines() if line.strip()]; logger.error("UTA: --- End FFmpeg Error Log ---")
        
        if current_slp:
            sl_exit_code = current_slp.poll()
            if sl_exit_code is None: logger.warning("UTA: FFmpeg exited, Streamlink running. Terminating..."); uta_terminate_process(current_slp, "Streamlink")
            else:
                 logger.info(f"UTA: Streamlink exited code: {sl_exit_code}")
                 if current_slp.stderr:
                      try:
                          sl_stderr_bytes = current_slp.stderr.read()
                          if sl_stderr_bytes: logger.info(f"UTA: --- Streamlink Stderr Log ---\n{sl_stderr_bytes.decode('utf-8', errors='ignore').strip()}\n--- End Streamlink Stderr Log ---")
                      finally: current_slp.stderr.close()
        return True
    except FileNotFoundError as e:
        logger.critical(f"UTA: ERROR: Command not found (Streamlink/FFmpeg). Check paths. Details: {e}")
        if str(e.filename) == UTA_STREAMLINK_PATH: logger.critical(f"'{UTA_STREAMLINK_PATH}' not found.")
        elif str(e.filename) == UTA_FFMPEG_PATH: logger.critical(f"'{UTA_FFMPEG_PATH}' not found.")
        return False
    except Exception as e:
        logger.error(f"UTA: Unexpected critical error during restreaming: {e}", exc_info=True)
        return False
    finally:
        # Cleanup processes specific to this attempt if not shutting down globally
        if not shutdown_event.is_set():
            # If these are the current global processes, nullify globals before terminating locally
            # to prevent double-termination by global cleanup if it runs concurrently.
            temp_slp, temp_ffp = uta_streamlink_process, uta_ffmpeg_process
            if current_slp == temp_slp: uta_streamlink_process = None
            if current_ffp == temp_ffp: uta_ffmpeg_process = None
            
            uta_terminate_process(current_ffp, "FFmpeg (start_restream finally)")
            uta_terminate_process(current_slp, "Streamlink (start_restream finally)")
        uta_is_restreaming_active = False # Always set to false after an attempt concludes

# --- UTA: Clip Monitor Thread Function ---
def uta_clip_monitor_loop():
    logger.info("UTA: Clip Monitor thread started.")
    b_id = uta_get_broadcaster_id(UTA_TWITCH_CHANNEL_NAME)
    if not b_id: logger.error(f"UTA Clip Monitor: Failed to get broadcaster ID for {UTA_TWITCH_CHANNEL_NAME}. Will retry.")
    if b_id:
        logger.info(f"UTA Clip Monitor: Initial clip scan for last {UTA_CLIP_LOOKBACK_MINUTES} min...")
        initial_clips = uta_get_recent_clips(b_id, UTA_CLIP_LOOKBACK_MINUTES)
        for clip_data in initial_clips: uta_sent_clip_ids.add(clip_data['id'])
        logger.info(f"UTA Clip Monitor: Primed {len(uta_sent_clip_ids)} clips.")

    while not shutdown_event.is_set():
        try:
            current_b_id = uta_broadcaster_id_cache or uta_get_broadcaster_id(UTA_TWITCH_CHANNEL_NAME)
            if not current_b_id:
                logger.warning(f"UTA Clip Monitor: No broadcaster ID for {UTA_TWITCH_CHANNEL_NAME}. Skipping cycle.")
                if shutdown_event.wait(timeout=UTA_CHECK_INTERVAL_SECONDS_CLIPS): break; continue
            
            logger.info(f"UTA Clip Monitor: Checking new clips for {UTA_TWITCH_CHANNEL_NAME}...")
            clips = uta_get_recent_clips(current_b_id, UTA_CLIP_LOOKBACK_MINUTES)
            if not clips: logger.info("UTA Clip Monitor: No clips in lookback window.")
            else:
                new_clips_found = 0
                for clip_data in reversed(clips): # Process oldest new clip first
                    if shutdown_event.is_set(): break
                    if clip_data['id'] not in uta_sent_clip_ids:
                        logger.info(f"UTA Clip Monitor: New clip: {clip_data['title']} - {clip_data['url']}")
                        uta_send_discord_clip_notification(clip_data['url'], clip_data['title'], UTA_TWITCH_CHANNEL_NAME)
                        uta_sent_clip_ids.add(clip_data['id']); new_clips_found += 1
                        if shutdown_event.wait(timeout=1): break # Small interruptible delay
                if new_clips_found == 0 and clips: logger.info("UTA Clip Monitor: No *new* clips found.")
            
            if shutdown_event.is_set(): break
            logger.info(f"UTA Clip Monitor: Waiting {UTA_CHECK_INTERVAL_SECONDS_CLIPS // 60} min ({UTA_CHECK_INTERVAL_SECONDS_CLIPS}s) for next check...")
            if shutdown_event.wait(timeout=UTA_CHECK_INTERVAL_SECONDS_CLIPS): break
        except Exception as e:
            logger.error(f"UTA Clip Monitor: Unexpected error: {e}", exc_info=True)
            if shutdown_event.wait(timeout=60): break
    logger.info("UTA: Clip Monitor thread finished.")

# --- UTA: Restreamer Monitor Thread Function ---
def uta_restreamer_monitor_loop():
    global uta_is_restreaming_active
    logger.info("UTA: Restreamer Monitor thread started.")
    while not shutdown_event.is_set():
        try:
            live, stream_data = uta_is_streamer_live(UTA_TWITCH_CHANNEL_NAME)
            if live and not uta_is_restreaming_active:
                logger.info(f"UTA Restreamer: {UTA_TWITCH_CHANNEL_NAME} LIVE! Starting restream...")
                uta_send_discord_restream_status("start", UTA_TWITCH_CHANNEL_NAME, stream_data)
                uta_start_restream(UTA_TWITCH_CHANNEL_NAME) # This is blocking for the duration of the restream
                logger.info(f"UTA Restreamer: Restream attempt for {UTA_TWITCH_CHANNEL_NAME} concluded.")
                uta_send_discord_restream_status("stop", UTA_TWITCH_CHANNEL_NAME) # Always send stop after attempt
                logger.info(f"UTA Restreamer: Cooling down for {UTA_POST_RESTREAM_COOLDOWN_SECONDS}s...")
                if shutdown_event.wait(timeout=UTA_POST_RESTREAM_COOLDOWN_SECONDS): break
            elif live and uta_is_restreaming_active:
                logger.info(f"UTA Restreamer: {UTA_TWITCH_CHANNEL_NAME} still live. Restream should be active. Check in {UTA_RESTREAM_CHECK_INTERVAL_WHEN_LIVE}s.")
                if shutdown_event.wait(timeout=UTA_RESTREAM_CHECK_INTERVAL_WHEN_LIVE): break
            elif not live and uta_is_restreaming_active:
                logger.warning(f"UTA Restreamer: {UTA_TWITCH_CHANNEL_NAME} offline, but restream marked active. Cleaning up.")
                uta_cleanup_restream_processes() # This sets uta_is_restreaming_active to False
                uta_send_discord_restream_status("stop", UTA_TWITCH_CHANNEL_NAME)
                logger.info(f"UTA Restreamer: Waiting {UTA_CHECK_INTERVAL_SECONDS_RESTREAMER}s...")
                if shutdown_event.wait(timeout=UTA_CHECK_INTERVAL_SECONDS_RESTREAMER): break
            elif not live and not uta_is_restreaming_active:
                logger.info(f"UTA Restreamer: {UTA_TWITCH_CHANNEL_NAME} offline. Waiting {UTA_CHECK_INTERVAL_SECONDS_RESTREAMER}s...")
                if shutdown_event.wait(timeout=UTA_CHECK_INTERVAL_SECONDS_RESTREAMER): break
        except Exception as e:
            logger.error(f"UTA Restreamer Monitor: Unexpected error: {e}", exc_info=True)
            logger.info("UTA Restreamer Monitor: Cleaning up processes due to error...")
            uta_cleanup_restream_processes()
            if shutdown_event.wait(timeout=60): break
    logger.info("UTA: Restreamer Monitor thread finished.")

# =====================================================================================
# --- UTA (Universal Twitch Assistant) Integration End ---
# =====================================================================================

# --- Bot Setup (fctd) ---
intents = discord.Intents.default()
intents.message_content = True
# Pass owner_id if specified in config for @commands.is_owner() to work correctly
# if it's not the token owner. Otherwise, discord.py infers from token.
owner_id_from_config = config_data.get('DISCORD_BOT_OWNER_ID')
bot = commands.Bot(command_prefix=FCTD_COMMAND_PREFIX, intents=intents, help_command=None,
                   owner_id=int(owner_id_from_config) if owner_id_from_config else None)


# --- Bot Events & Tasks (fctd) ---
@bot.event
async def on_ready():
    logger.info(f'{bot.user.name} (ID: {bot.user.id}) has connected to Discord!')
    logger.info(f'Command Prefix: {FCTD_COMMAND_PREFIX}')
    if FCTD_COMMAND_CHANNEL_ID: logger.info(f'Listening for fctd commands in channel ID: {FCTD_COMMAND_CHANNEL_ID}')
    else: logger.info(f'Listening for fctd commands in ALL channels.')
    logger.info(f'Connected to {len(bot.guilds)} guilds.')

    # fctd follower count setup
    if FCTD_TWITCH_USERNAME:
        logger.info(f'fctd: Targeting Twitch User for followers: {FCTD_TWITCH_USERNAME}')
        global fctd_current_twitch_user_id
        fctd_current_twitch_user_id = await fctd_twitch_api.get_user_id(FCTD_TWITCH_USERNAME)
        if not fctd_current_twitch_user_id:
            logger.error(f"fctd: CRITICAL: Could not fetch Twitch User ID for {FCTD_TWITCH_USERNAME}. Follower count features will NOT work.")
        else:
            logger.info(f"fctd: Successfully fetched Twitch User ID for {FCTD_TWITCH_USERNAME}: {fctd_current_twitch_user_id}")
            if FCTD_TARGET_CHANNEL_ID or FCTD_FOLLOWER_DATA_FILE:
                 update_channel_name_and_log_followers.start()
    else:
        logger.warning("fctd: FCTD_TWITCH_USERNAME not set. Follower tracking features disabled.")

    # UTA Setup and Thread Starting
    if UTA_ENABLED:
        logger.info("--- UTA Module Enabled ---")
        if not UTA_TWITCH_CHANNEL_NAME or "target_twitch_username_for_uta" in UTA_TWITCH_CHANNEL_NAME :
            logger.error("UTA: UTA_TWITCH_CHANNEL_NAME is not configured correctly. UTA features will be limited/disabled.")
        else:
            logger.info(f"UTA: Targeting Twitch Channel: {UTA_TWITCH_CHANNEL_NAME}")
            # Validate paths for Streamlink and FFmpeg if restreamer is enabled
            if UTA_RESTREAMER_ENABLED:
                if not shutil.which(UTA_STREAMLINK_PATH):
                    logger.critical(f"UTA CRITICAL: Streamlink path '{UTA_STREAMLINK_PATH}' not found or not executable. Restreamer disabled.")
                elif not shutil.which(UTA_FFMPEG_PATH):
                    logger.critical(f"UTA CRITICAL: FFmpeg path '{UTA_FFMPEG_PATH}' not found or not executable. Restreamer disabled.")
                else:
                    logger.info("UTA: Streamlink and FFmpeg paths appear valid.")
            
            # Initial UTA Twitch token fetch (important for threads)
            if not uta_get_twitch_access_token(): # Try to get token for UTA
                logger.critical("UTA: Failed to get initial Twitch access token for UTA. UTA functionality may be impaired.")

            global uta_clip_thread, uta_restreamer_thread
            if UTA_CLIP_MONITOR_ENABLED and UTA_TWITCH_CHANNEL_NAME:
                logger.info("UTA: Starting Clip Monitor thread...")
                uta_clip_thread = threading.Thread(target=uta_clip_monitor_loop, name="UTAClipMonitor", daemon=True)
                uta_clip_thread.start()
            else:
                logger.info("UTA: Clip Monitor disabled or UTA_TWITCH_CHANNEL_NAME not set.")

            if UTA_RESTREAMER_ENABLED and UTA_TWITCH_CHANNEL_NAME and \
               shutil.which(UTA_STREAMLINK_PATH) and shutil.which(UTA_FFMPEG_PATH) and \
               UTA_YOUTUBE_STREAM_KEY and "YOUR_YOUTUBE_STREAM_KEY" not in UTA_YOUTUBE_STREAM_KEY:
                logger.info("UTA: Starting Restreamer Monitor thread...")
                uta_restreamer_thread = threading.Thread(target=uta_restreamer_monitor_loop, name="UTARestreamerMonitor", daemon=True)
                uta_restreamer_thread.start()
            else:
                logger.info("UTA: Restreamer disabled due to config (enabled flag, channel name, paths, or YouTube key).")
    else:
        logger.info("--- UTA Module Disabled ---")


@tasks.loop(minutes=FCTD_UPDATE_INTERVAL_MINUTES)
async def update_channel_name_and_log_followers():
    if not fctd_current_twitch_user_id: return
    follower_count = await fctd_twitch_api.get_follower_count(fctd_current_twitch_user_id)
    current_time_utc = datetime.now(timezone.utc)
    if follower_count is not None:
        if FCTD_FOLLOWER_DATA_FILE: await log_follower_data_binary(current_time_utc, follower_count)
        if FCTD_TARGET_CHANNEL_ID:
            channel = bot.get_channel(FCTD_TARGET_CHANNEL_ID)
            if channel:
                new_name = f"{FCTD_CHANNEL_NAME_PREFIX}{follower_count:,}{FCTD_CHANNEL_NAME_SUFFIX}"
                if channel.name != new_name:
                    try: await channel.edit(name=new_name); logger.info(f"fctd: Channel name for {FCTD_TWITCH_USERNAME} updated to: {new_name}")
                    except discord.Forbidden: logger.error(f"fctd: Bot lacks 'Manage Channels' for channel {FCTD_TARGET_CHANNEL_ID}.")
                    except discord.HTTPException as e: logger.error(f"fctd: Failed to edit channel name (HTTPException): {e}")
                    except Exception as e: logger.error(f"fctd: Unexpected error editing channel name: {e}")
            else: logger.warning(f"fctd: Target channel {FCTD_TARGET_CHANNEL_ID} not found/accessible.")
    else: logger.warning(f"fctd: Could not retrieve follower count for {FCTD_TWITCH_USERNAME}. Skipping update/log.")

@update_channel_name_and_log_followers.before_loop
async def before_update_task():
    await bot.wait_until_ready()
    if fctd_current_twitch_user_id and (FCTD_TARGET_CHANNEL_ID or FCTD_FOLLOWER_DATA_FILE):
        logger.info(f"fctd: Follower update/log task for {FCTD_TWITCH_USERNAME} starting.")
    else:
        logger.info("fctd: Follower update/log task will not start (config/ID issue).")
        update_channel_name_and_log_followers.cancel()

# --- Bot Commands (fctd part, can be expanded for UTA) ---
@bot.command(name="followers", aliases=['foll', 'followerstats'])
async def followers_command(ctx: commands.Context, *, duration_input: str = None):
    if FCTD_COMMAND_CHANNEL_ID is not None and ctx.channel.id != FCTD_COMMAND_CHANNEL_ID: return
    if not FCTD_TWITCH_USERNAME: await ctx.send("fctd: Twitch username not configured for follower tracking."); return
    if duration_input is None:
        # (Help embed for followers command - kept brief for merge, same as before)
        embed = discord.Embed(title=f"{FCTD_TWITCH_USERNAME} Follower Stats", description=f"Use `{FCTD_COMMAND_PREFIX}followers <duration>`.", color=discord.Color.purple())
        embed.add_field(name="Format", value="`10m`, `2h`, `3d`, `1w`, `1mo`, `1y`", inline=False)
        await ctx.send(embed=embed); return
    if not FCTD_FOLLOWER_DATA_FILE or not os.path.exists(FCTD_FOLLOWER_DATA_FILE) or os.path.getsize(FCTD_FOLLOWER_DATA_FILE) < BINARY_RECORD_SIZE:
        await ctx.send(f"fctd: Not enough follower data stored for {FCTD_TWITCH_USERNAME}. Wait for data collection."); return
    time_delta, period_name = parse_duration_to_timedelta(duration_input)
    if not time_delta: await ctx.send(period_name); return # period_name has error
    async with ctx.typing(): message_content = await get_follower_gain_for_period(time_delta, period_name)
    await ctx.send(message_content or "fctd: Error fetching follower data.")

@bot.command(name="readdata", help="Dumps raw follower data. (Bot owner only)")
@commands.is_owner()
async def read_data_command(ctx: commands.Context, max_records_str: str = "50"):
    if FCTD_COMMAND_CHANNEL_ID is not None and ctx.channel.id != FCTD_COMMAND_CHANNEL_ID: return
    if not FCTD_FOLLOWER_DATA_FILE: await ctx.send("fctd: FOLLOWER_DATA_FILE not configured."); return
    try: max_r = int(max_records_str); max_r = min(max(1, max_r), 500) # Cap 1-500
    except ValueError: max_r = 50; await ctx.send("Invalid number for max_records, using 50.")
    async with ctx.typing(): chunks = await asyncio.to_thread(get_raw_follower_data_for_discord, FCTD_FOLLOWER_DATA_FILE, max_r)
    if not chunks: await ctx.send("fctd: No data or error retrieving. Check logs."); return
    for i, chunk in enumerate(chunks):
        if i > 0: await asyncio.sleep(0.5)
        await ctx.send(chunk)

@bot.command(name="commands", aliases=['help'], help="Lists all available commands.")
async def list_commands_command(ctx: commands.Context):
    if FCTD_COMMAND_CHANNEL_ID is not None and ctx.channel.id != FCTD_COMMAND_CHANNEL_ID: return
    embed = discord.Embed(title="Bot Commands", description=f"Prefix: `{FCTD_COMMAND_PREFIX}`", color=discord.Color.blue())
    for cmd in bot.commands:
        if cmd.hidden: continue
        name_aliases = f"`{FCTD_COMMAND_PREFIX}{cmd.name}`" + (f" (Aliases: {', '.join([f'`{FCTD_COMMAND_PREFIX}{a}`' for a in cmd.aliases])})" if cmd.aliases else "")
        desc = cmd.help or "No description."
        embed.add_field(name=name_aliases, value=desc, inline=False)
    if not embed.fields: embed.description = "No commands available."
    await ctx.send(embed=embed)

#UTA related command example (basic status)
@bot.command(name="utastatus", help="Shows status of UTA modules. (Bot owner only)")
@commands.is_owner()
async def uta_status_command(ctx: commands.Context):
    if FCTD_COMMAND_CHANNEL_ID is not None and ctx.channel.id != FCTD_COMMAND_CHANNEL_ID: return
    if not UTA_ENABLED:
        await ctx.send("UTA module is disabled in config.")
        return

    embed = discord.Embed(title="UTA Module Status", color=discord.Color.orange())
    embed.add_field(name="UTA Enabled", value=str(UTA_ENABLED), inline=False)
    embed.add_field(name="Target Twitch Channel", value=UTA_TWITCH_CHANNEL_NAME or "Not Set", inline=False)

    # Clip Monitor Status
    clip_status = "Disabled in Config"
    if UTA_CLIP_MONITOR_ENABLED:
        clip_status = f"Enabled. Thread Active: {uta_clip_thread.is_alive() if uta_clip_thread else 'No'}. Sent Clips: {len(uta_sent_clip_ids)}"
    embed.add_field(name="Clip Monitor", value=clip_status, inline=False)

    # Restreamer Status
    restream_status = "Disabled in Config"
    if UTA_RESTREAMER_ENABLED:
        restream_status = f"Enabled. Thread Active: {uta_restreamer_thread.is_alive() if uta_restreamer_thread else 'No'}. Currently Restreaming: {uta_is_restreaming_active}"
        if uta_is_restreaming_active:
            sl_pid = uta_streamlink_process.pid if uta_streamlink_process else "N/A"
            ff_pid = uta_ffmpeg_process.pid if uta_ffmpeg_process else "N/A"
            restream_status += f"\n  Streamlink PID: {sl_pid}, FFmpeg PID: {ff_pid}"
    embed.add_field(name="Restreamer", value=restream_status, inline=False)
    
    # UTA Twitch Token
    token_status = "No Token"
    if uta_shared_access_token:
        expiry_dt = datetime.fromtimestamp(uta_token_expiry_time)
        token_status = f"Token Acquired. Expires: {discord.utils.format_dt(expiry_dt, 'R')} ({discord.utils.format_dt(expiry_dt)})"
    embed.add_field(name="UTA Twitch API Token", value=token_status, inline=False)

    await ctx.send(embed=embed)


@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.CommandNotFound):
        if FCTD_COMMAND_CHANNEL_ID is None or (FCTD_COMMAND_CHANNEL_ID is not None and ctx.channel.id == FCTD_COMMAND_CHANNEL_ID):
            # Minimal response to avoid spam, could check if it was for one of our commands
            # logger.debug(f"Command not found: {ctx.message.content}")
            pass 
    elif isinstance(error, commands.MissingRequiredArgument):
        await ctx.send(f"Missing argument for `{ctx.command.name}`. Use `{FCTD_COMMAND_PREFIX}help`.", delete_after=15)
    elif isinstance(error, commands.NotOwner):
        await ctx.send("Sorry, this command is for the bot owner only.", delete_after=10)
    elif isinstance(error, commands.CheckFailure):
        logger.warning(f"Command check failed for {ctx.author} on '{ctx.command}': {error}")
    elif isinstance(error, commands.CommandInvokeError):
        logger.error(f'Error in command {ctx.command}: {error.original}', exc_info=error.original)
        await ctx.send(f"Error executing command: {error.original}", delete_after=10)
    else:
        logger.error(f'Unhandled command error for {ctx.command}: {error}', exc_info=error)

# --- Main Execution & Shutdown Handling ---
async def main():
    async with bot:
        # Setup signal handlers for graceful shutdown of UTA components
        # discord.py handles SIGINT for bot.close()
        # We primarily need to tell our threads to stop
        # Note: Signal handlers run in the main thread, not the bot's event loop.
        
        # This custom signal handler is tricky with asyncio.
        # For now, rely on try/finally around bot.start() and discord.py's SIGINT handling.
        # The shutdown_event will be set in the finally block.
        
        logger.info("Starting bot...")
        await bot.start(DISCORD_TOKEN)

if __name__ == "__main__":
    # Perform pre-run checks for UTA paths if enabled
    if UTA_ENABLED and UTA_RESTREAMER_ENABLED:
        if not shutil.which(UTA_STREAMLINK_PATH):
            logger.critical(f"UTA PRE-RUN FAIL: Streamlink path '{UTA_STREAMLINK_PATH}' not found. Fix config or PATH.")
            exit(1)
        if not shutil.which(UTA_FFMPEG_PATH):
            logger.critical(f"UTA PRE-RUN FAIL: FFmpeg path '{UTA_FFMPEG_PATH}' not found. Fix config or PATH.")
            exit(1)
        if not UTA_YOUTUBE_STREAM_KEY or "YOUR_YOUTUBE_STREAM_KEY" in UTA_YOUTUBE_STREAM_KEY :
             logger.warning(f"UTA PRE-RUN WARN: YOUTUBE_STREAM_KEY is not set or is placeholder. Restreamer will not function.")


    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received. Shutting down...")
    except discord.LoginFailure:
        logger.critical("CRITICAL: Invalid Discord Bot Token. Please check your config.json.")
    except Exception as e:
        logger.critical(f"An unexpected error occurred during bot startup or runtime: {e}", exc_info=True)
    finally:
        logger.info("Initiating final cleanup...")
        shutdown_event.set() # Signal all UTA threads to stop

        if UTA_ENABLED: # Only try to cleanup UTA if it was enabled
            # Synchronously clean up any lingering UTA processes
            # This is important if threads didn't exit cleanly or if processes were orphaned
            logger.info("Cleaning up UTA restream processes (if any)...")
            uta_cleanup_restream_processes() # This is synchronous

            # Join UTA threads
            if uta_clip_thread and uta_clip_thread.is_alive():
                logger.info("Waiting for UTA Clip Monitor thread to exit...")
                uta_clip_thread.join(timeout=10)
                if uta_clip_thread.is_alive(): logger.warning("UTA Clip Monitor thread did not exit cleanly.")
            
            if uta_restreamer_thread and uta_restreamer_thread.is_alive():
                logger.info("Waiting for UTA Restreamer Monitor thread to exit...")
                uta_restreamer_thread.join(timeout=10) # Restreamer might take longer if mid-ffmpeg
                if uta_restreamer_thread.is_alive(): logger.warning("UTA Restreamer Monitor thread did not exit cleanly.")

        # The bot.close() should have been handled by discord.py if shutdown was via SIGINT,
        # or by the `async with bot:` context manager.
        # If loop is still running, ensure it's stopped.
        if loop.is_running() and not bot.is_closed():
            logger.info("Closing bot connection from finally block (if not already closed)...")
            # This needs to be done carefully if the loop is from run_until_complete
            # loop.run_until_complete(bot.close()) # This might error if loop is stopping
            # Simpler: bot.is_closed() check suffices. If not, something else went wrong.
        
        # Gather any remaining tasks and cancel them if loop still usable
        # tasks = [t for t in asyncio.all_tasks(loop=loop) if t is not asyncio.current_task(loop=loop)]
        # if tasks:
        #     logger.info(f"Cancelling {len(tasks)} outstanding asyncio tasks...")
        #     for task in tasks: task.cancel()
        #     loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True))
        
        # loop.close() # Close the loop itself if we are fully done

        logger.info("Shutdown sequence finished. Exiting.")
