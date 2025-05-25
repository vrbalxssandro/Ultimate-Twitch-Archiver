import logging
import json
import os
import sys
from datetime import datetime, timedelta, timezone
import struct
import requests
import asyncio

# --- Library Availability Checks ---
try:
    from google.oauth2.credentials import Credentials as GoogleCredentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from google.auth.transport.requests import Request as GoogleAuthRequest
    from googleapiclient.discovery import build as google_build
    from googleapiclient.errors import HttpError as GoogleHttpError
    GOOGLE_API_AVAILABLE = True
    logger_google_api_client = logging.getLogger('googleapiclient.discovery_cache')
    logger_google_api_client.setLevel(logging.ERROR)
except ImportError:
    GOOGLE_API_AVAILABLE = False
    GoogleCredentials, InstalledAppFlow, GoogleAuthRequest, google_build, GoogleHttpError = None, None, None, None, None

try:
    import streamlink
    STREAMLINK_LIB_AVAILABLE = True
except ImportError:
    STREAMLINK_LIB_AVAILABLE = False
    streamlink = None

try:
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    MATPLOTLIB_AVAILABLE = True
    logger_mpl = logging.getLogger('matplotlib')
    logger_mpl.setLevel(logging.WARNING)
except ImportError:
    MATPLOTLIB_AVAILABLE = False
    plt = None
    mdates = None

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(levelname)s:%(name)s:[%(threadName)s]: %(message)s')
logger = logging.getLogger('discord_twitch_bot')

# --- Bot Start Time ---
bot_start_time = datetime.now(timezone.utc)

# --- Configuration Loading ---
CONFIG_FILE = 'config.json'
config_data = {} # This will hold the raw loaded dict

# --- Global Config Variables ---
DISCORD_TOKEN: str = None
FCTD_TWITCH_USERNAME: str = None
TWITCH_CLIENT_ID: str = None
TWITCH_CLIENT_SECRET: str = None
FCTD_TARGET_CHANNEL_ID: int = None
FCTD_COMMAND_CHANNEL_ID: int = None
FCTD_COMMAND_PREFIX: str = '!'
FCTD_UPDATE_INTERVAL_MINUTES: int = 2
FCTD_CHANNEL_NAME_PREFIX: str = "Followers: "
FCTD_CHANNEL_NAME_SUFFIX: str = ""
FCTD_FOLLOWER_DATA_FILE: str = "follower_counts.bin"
owner_id_from_config: str = None # Stored as string from config, converted by bot instance
UTA_STREAM_DURATION_LOG_FILE: str = "stream_durations.bin"
UTA_ENABLED: bool = False
UTA_TWITCH_CHANNEL_NAME: str = None
UTA_CLIP_MONITOR_ENABLED: bool = False
UTA_DISCORD_WEBHOOK_URL_CLIPS: str = None
UTA_CHECK_INTERVAL_SECONDS_CLIPS: int = 300
UTA_CLIP_LOOKBACK_MINUTES: int = 5
UTA_RESTREAMER_ENABLED: bool = False
UTA_DISCORD_WEBHOOK_URL_RESTREAMER: str = None
UTA_YOUTUBE_RTMP_URL_BASE: str = "rtmp://a.rtmp.youtube.com/live2"
UTA_YOUTUBE_STREAM_KEY: str = None
UTA_CHECK_INTERVAL_SECONDS_RESTREAMER: int = 60
UTA_RESTREAM_CHECK_INTERVAL_WHEN_LIVE: int = 300
UTA_POST_RESTREAM_COOLDOWN_SECONDS: int = 60
UTA_STREAMLINK_PATH: str = "streamlink"
UTA_FFMPEG_PATH: str = "ffmpeg"
UTA_STREAM_STATUS_NOTIFICATIONS_ENABLED: bool = False
UTA_STREAM_STATUS_WEBHOOK_URL: str = None
UTA_STREAM_STATUS_CHANNEL_ID: int = None
UTA_STREAM_STATUS_POLL_INTERVAL_SECONDS: int = 60
UTA_STREAM_ACTIVITY_LOG_FILE: str = "stream_activity.bin"
UTA_VIEWER_COUNT_LOGGING_ENABLED: bool = False
UTA_VIEWER_COUNT_LOG_INTERVAL_SECONDS: int = 300
UTA_VIEWER_COUNT_LOG_FILE: str = "viewer_counts.bin"
BOT_SESSION_LOG_FILE_KEY: str = "BOT_SESSION_LOG_FILE" # Key in config.json
BOT_SESSION_LOG_FILE_PATH: str = "bot_sessions.bin" # Actual path variable
UTA_YOUTUBE_API_ENABLED: bool = False
UTA_YOUTUBE_CLIENT_SECRET_FILE: str = "client_secret.json"
UTA_YOUTUBE_TOKEN_FILE: str = "youtube_token.json"
UTA_YOUTUBE_PLAYLIST_ID: str = None
UTA_YOUTUBE_DEFAULT_PRIVACY: str = "unlisted"
UTA_YOUTUBE_MAKE_PUBLIC_AFTER_STREAM: bool = False
UTA_YOUTUBE_SCHEDULED_ROLLOVER_HOURS: float = 0.0
UTA_YOUTUBE_DYNAMIC_TITLE_TEMPLATE: str = "{twitch_username} - {twitch_title} ({game_name}) - Part {part_num} [{date}]"
UTA_YOUTUBE_DYNAMIC_DESCRIPTION_TEMPLATE: str = "Originally streamed by {twitch_username} on Twitch: https://twitch.tv/{twitch_username}\nGame: {game_name}\nTitle: {twitch_title}\n\nArchived by UTA."
UTA_YOUTUBE_API_SCOPES: list = ["https://www.googleapis.com/auth/youtube.force-ssl"]
UTA_RESTREAM_MAX_CONSECUTIVE_FAILURES: int = 3
UTA_RESTREAM_LONG_COOLDOWN_SECONDS: int = 300
UTA_YOUTUBE_PLAYABILITY_CHECK_ENABLED: bool = True
UTA_YOUTUBE_PLAYABILITY_CHECK_RETRIES: int = 2
UTA_YOUTUBE_PLAYABILITY_CHECK_DELAY_SECONDS: int = 15
UTA_FFMPEG_STARTUP_WAIT_SECONDS: int = 10
UTA_YOUTUBE_AUTO_CHAPTERS_ENABLED: bool = True
UTA_YOUTUBE_MIN_CHAPTER_DURATION_SECONDS: int = 60
UTA_YOUTUBE_DESCRIPTION_CHAPTER_MARKER: str = "## UTA Auto Chapters ##"
UTA_YOUTUBE_CHAPTER_TITLE_TEMPLATE: str = "{game_name} - {twitch_title}"

# New Twitch Chat Configs
TWITCH_CHAT_ENABLED: bool = False
TWITCH_CHAT_NICKNAME: str = "YourBotTwitchNickname"
TWITCH_CHAT_OAUTH_TOKEN: str = "oauth:yourtwitchtoken" # Must start with oauth:
TWITCH_CHAT_LOG_INTERVAL_SECONDS: int = 60
TWITCH_CHAT_ACTIVITY_LOG_FILE: str = "chat_activity.bin"
DISCORD_TWITCH_CHAT_MIRROR_ENABLED: bool = False
DISCORD_TWITCH_CHAT_MIRROR_CHANNEL_ID: int = None


# Global state variables (managed by services, but potentially read elsewhere)
uta_broadcaster_id_cache: str = None
uta_shared_access_token: str = None
uta_token_expiry_time: float = 0.0 # Unix timestamp
_are_uta_threads_active: bool = False # For internal tracking within config_manager, primarily used by services to know thread status

uta_is_restreaming_active: bool = False
twitch_session_active_global: bool = False # Tracks if the target Twitch channel is live (updated by StatusService)
youtube_api_session_active_global: bool = False
UTA_CURRENT_YT_BROADCAST_ID_FOR_LOGGING: str = None
UTA_CURRENT_YT_VIDEO_ID_FOR_LOGGING: str = None
uta_current_restream_part_number: int = 1
uta_current_youtube_live_stream_id: str = None # The persistent liveStream ID from YT API
uta_current_youtube_rtmp_url: str = None # RTMP URL from the liveStream resource
uta_current_youtube_stream_key: str = None # Stream key from the liveStream resource
uta_youtube_next_rollover_time_utc: datetime = None
UTA_FFMPEG_PID: int = None
UTA_STREAMLINK_PID: int = None
UTA_PIPE_START_TIME_UTC: datetime = None # Stores the UTC datetime of when the current VOD part's ffmpeg pipe started
UTA_RESTREAM_CONSECUTIVE_FAILURES: int = 0
UTA_LAST_PLAYABILITY_CHECK_STATUS: str = "N/A"

uta_yt_service = None # YouTube API service object

UTA_MANUAL_FFMPEG_RESTART_REQUESTED: bool = False
UTA_MANUAL_NEW_PART_REQUESTED: bool = False

fctd_twitch_api: 'TwitchAPIHelper' = None
fctd_current_twitch_user_id: str = None

last_known_title_for_ended_part: str = None
last_known_game_for_ended_part: str = None


def load_config(initial_load=False):
    global config_data
    try:
        with open(CONFIG_FILE, 'r') as f:
            loaded_json = json.load(f)

        required_keys = ['DISCORD_TOKEN', 'TWITCH_CLIENT_ID', 'TWITCH_CLIENT_SECRET']
        for key in required_keys:
            if not loaded_json.get(key) or "YOUR_" in str(loaded_json.get(key)):
                err_msg = f"ERROR: Essential configuration key '{key}' is missing or contains a placeholder value in {CONFIG_FILE}."
                if initial_load:
                    print(err_msg)
                    sys.exit(1)
                logger.error(f"Reload Attempt: {err_msg}")
                return False, err_msg

        if initial_load:
            config_data = loaded_json
        return True, loaded_json

    except FileNotFoundError:
        err_msg = f"ERROR: {CONFIG_FILE} not found. Please create it based on the template or documentation."
        if initial_load: print(err_msg); sys.exit(1)
        logger.error(f"Reload Attempt: {err_msg}"); return False, err_msg
    except json.JSONDecodeError as e:
        err_msg = f"ERROR: Error decoding {CONFIG_FILE}. Please check its JSON syntax. Details: {e}"
        if initial_load: print(err_msg); sys.exit(1)
        logger.error(f"Reload Attempt: {err_msg}"); return False, err_msg

def apply_config_globally(source_config_dict):
    logger.info("Applying configuration dictionary to global variables...")
    global DISCORD_TOKEN, FCTD_TWITCH_USERNAME, TWITCH_CLIENT_ID, TWITCH_CLIENT_SECRET, \
           FCTD_TARGET_CHANNEL_ID, FCTD_COMMAND_CHANNEL_ID, FCTD_COMMAND_PREFIX, \
           FCTD_UPDATE_INTERVAL_MINUTES, FCTD_CHANNEL_NAME_PREFIX, FCTD_CHANNEL_NAME_SUFFIX, \
           FCTD_FOLLOWER_DATA_FILE, owner_id_from_config, \
           UTA_STREAM_DURATION_LOG_FILE, UTA_ENABLED, UTA_TWITCH_CHANNEL_NAME, \
           UTA_CLIP_MONITOR_ENABLED, UTA_DISCORD_WEBHOOK_URL_CLIPS, \
           UTA_CHECK_INTERVAL_SECONDS_CLIPS, UTA_CLIP_LOOKBACK_MINUTES, \
           UTA_RESTREAMER_ENABLED, UTA_DISCORD_WEBHOOK_URL_RESTREAMER, \
           UTA_YOUTUBE_RTMP_URL_BASE, UTA_YOUTUBE_STREAM_KEY, \
           UTA_CHECK_INTERVAL_SECONDS_RESTREAMER, UTA_RESTREAM_CHECK_INTERVAL_WHEN_LIVE, \
           UTA_POST_RESTREAM_COOLDOWN_SECONDS, UTA_STREAMLINK_PATH, UTA_FFMPEG_PATH, \
           UTA_STREAM_STATUS_NOTIFICATIONS_ENABLED, UTA_STREAM_STATUS_WEBHOOK_URL, \
           UTA_STREAM_STATUS_CHANNEL_ID, UTA_STREAM_STATUS_POLL_INTERVAL_SECONDS, \
           UTA_STREAM_ACTIVITY_LOG_FILE, UTA_VIEWER_COUNT_LOGGING_ENABLED, \
           UTA_VIEWER_COUNT_LOG_INTERVAL_SECONDS, UTA_VIEWER_COUNT_LOG_FILE, \
           BOT_SESSION_LOG_FILE_PATH, \
           UTA_YOUTUBE_API_ENABLED, UTA_YOUTUBE_CLIENT_SECRET_FILE, UTA_YOUTUBE_TOKEN_FILE, \
           UTA_YOUTUBE_PLAYLIST_ID, UTA_YOUTUBE_DEFAULT_PRIVACY, \
           UTA_YOUTUBE_MAKE_PUBLIC_AFTER_STREAM, UTA_YOUTUBE_SCHEDULED_ROLLOVER_HOURS, \
           UTA_YOUTUBE_DYNAMIC_TITLE_TEMPLATE, UTA_YOUTUBE_DYNAMIC_DESCRIPTION_TEMPLATE, \
           UTA_RESTREAM_MAX_CONSECUTIVE_FAILURES, UTA_RESTREAM_LONG_COOLDOWN_SECONDS, \
           UTA_YOUTUBE_PLAYABILITY_CHECK_ENABLED, UTA_YOUTUBE_PLAYABILITY_CHECK_RETRIES, \
           UTA_YOUTUBE_PLAYABILITY_CHECK_DELAY_SECONDS, UTA_FFMPEG_STARTUP_WAIT_SECONDS, \
           UTA_YOUTUBE_AUTO_CHAPTERS_ENABLED, UTA_YOUTUBE_MIN_CHAPTER_DURATION_SECONDS, \
           UTA_YOUTUBE_DESCRIPTION_CHAPTER_MARKER, UTA_YOUTUBE_CHAPTER_TITLE_TEMPLATE, \
           TWITCH_CHAT_ENABLED, TWITCH_CHAT_NICKNAME, TWITCH_CHAT_OAUTH_TOKEN, \
           TWITCH_CHAT_LOG_INTERVAL_SECONDS, TWITCH_CHAT_ACTIVITY_LOG_FILE, \
           DISCORD_TWITCH_CHAT_MIRROR_ENABLED, DISCORD_TWITCH_CHAT_MIRROR_CHANNEL_ID, \
           fctd_twitch_api, uta_broadcaster_id_cache


    DISCORD_TOKEN = source_config_dict.get('DISCORD_TOKEN')
    FCTD_TWITCH_USERNAME = source_config_dict.get('FCTD_TWITCH_USERNAME')
    TWITCH_CLIENT_ID = source_config_dict.get('TWITCH_CLIENT_ID')
    TWITCH_CLIENT_SECRET = source_config_dict.get('TWITCH_CLIENT_SECRET')
    FCTD_TARGET_CHANNEL_ID = int(source_config_dict.get('FCTD_TARGET_CHANNEL_ID')) if source_config_dict.get('FCTD_TARGET_CHANNEL_ID') else None
    FCTD_COMMAND_CHANNEL_ID = int(source_config_dict.get('FCTD_COMMAND_CHANNEL_ID')) if source_config_dict.get('FCTD_COMMAND_CHANNEL_ID') else None
    FCTD_COMMAND_PREFIX = source_config_dict.get('FCTD_COMMAND_PREFIX', '!')
    FCTD_UPDATE_INTERVAL_MINUTES = source_config_dict.get('FCTD_UPDATE_INTERVAL_MINUTES', 2)
    FCTD_CHANNEL_NAME_PREFIX = source_config_dict.get('FCTD_CHANNEL_NAME_PREFIX', "Followers: ")
    FCTD_CHANNEL_NAME_SUFFIX = source_config_dict.get('FCTD_CHANNEL_NAME_SUFFIX', "")
    FCTD_FOLLOWER_DATA_FILE = source_config_dict.get('FCTD_FOLLOWER_DATA_FILE', "follower_counts.bin")
    owner_id_from_config = source_config_dict.get('DISCORD_BOT_OWNER_ID')
    UTA_STREAM_DURATION_LOG_FILE = source_config_dict.get('UTA_STREAM_DURATION_LOG_FILE', "stream_durations.bin")
    UTA_ENABLED = source_config_dict.get('UTA_ENABLED', False)
    old_uta_twitch_channel_name = UTA_TWITCH_CHANNEL_NAME
    UTA_TWITCH_CHANNEL_NAME = source_config_dict.get('UTA_TWITCH_CHANNEL_NAME')
    if old_uta_twitch_channel_name != UTA_TWITCH_CHANNEL_NAME and UTA_TWITCH_CHANNEL_NAME is not None:
        logger.info(f"UTA Twitch channel name changed from '{old_uta_twitch_channel_name}' to '{UTA_TWITCH_CHANNEL_NAME}'. Clearing broadcaster ID cache.")
        uta_broadcaster_id_cache = None
    UTA_CLIP_MONITOR_ENABLED = source_config_dict.get('UTA_CLIP_MONITOR_ENABLED', False)
    UTA_DISCORD_WEBHOOK_URL_CLIPS = source_config_dict.get('UTA_DISCORD_WEBHOOK_URL_CLIPS')
    UTA_CHECK_INTERVAL_SECONDS_CLIPS = source_config_dict.get('UTA_CHECK_INTERVAL_SECONDS_CLIPS', 300)
    UTA_CLIP_LOOKBACK_MINUTES = source_config_dict.get('UTA_CLIP_LOOKBACK_MINUTES', 5)
    UTA_RESTREAMER_ENABLED = source_config_dict.get('UTA_RESTREAMER_ENABLED', False)
    UTA_DISCORD_WEBHOOK_URL_RESTREAMER = source_config_dict.get('UTA_DISCORD_WEBHOOK_URL_RESTREAMER')
    UTA_YOUTUBE_RTMP_URL_BASE = source_config_dict.get('UTA_YOUTUBE_RTMP_URL_BASE', "rtmp://a.rtmp.youtube.com/live2")
    UTA_YOUTUBE_STREAM_KEY = source_config_dict.get('UTA_YOUTUBE_STREAM_KEY')
    UTA_CHECK_INTERVAL_SECONDS_RESTREAMER = source_config_dict.get('UTA_CHECK_INTERVAL_SECONDS_RESTREAMER', 60)
    UTA_RESTREAM_CHECK_INTERVAL_WHEN_LIVE = source_config_dict.get('UTA_RESTREAM_CHECK_INTERVAL_WHEN_LIVE', 300)
    UTA_POST_RESTREAM_COOLDOWN_SECONDS = source_config_dict.get('UTA_POST_RESTREAM_COOLDOWN_SECONDS', 60)
    UTA_STREAMLINK_PATH = source_config_dict.get('UTA_STREAMLINK_PATH', "streamlink")
    UTA_FFMPEG_PATH = source_config_dict.get('UTA_FFMPEG_PATH', "ffmpeg")
    UTA_STREAM_STATUS_NOTIFICATIONS_ENABLED = source_config_dict.get('UTA_STREAM_STATUS_NOTIFICATIONS_ENABLED', False)
    UTA_STREAM_STATUS_WEBHOOK_URL = source_config_dict.get('UTA_STREAM_STATUS_WEBHOOK_URL')
    UTA_STREAM_STATUS_CHANNEL_ID = int(source_config_dict.get('UTA_STREAM_STATUS_CHANNEL_ID')) if source_config_dict.get('UTA_STREAM_STATUS_CHANNEL_ID') else None
    UTA_STREAM_STATUS_POLL_INTERVAL_SECONDS = source_config_dict.get('UTA_STREAM_STATUS_POLL_INTERVAL_SECONDS', 60)
    UTA_STREAM_ACTIVITY_LOG_FILE = source_config_dict.get('UTA_STREAM_ACTIVITY_LOG_FILE', "stream_activity.bin")
    UTA_VIEWER_COUNT_LOGGING_ENABLED = source_config_dict.get('UTA_VIEWER_COUNT_LOGGING_ENABLED', False)
    UTA_VIEWER_COUNT_LOG_INTERVAL_SECONDS = source_config_dict.get('UTA_VIEWER_COUNT_LOG_INTERVAL_SECONDS', 300)
    UTA_VIEWER_COUNT_LOG_FILE = source_config_dict.get('UTA_VIEWER_COUNT_LOG_FILE', "viewer_counts.bin")
    BOT_SESSION_LOG_FILE_PATH = source_config_dict.get(BOT_SESSION_LOG_FILE_KEY, "bot_sessions.bin")
    UTA_YOUTUBE_API_ENABLED = source_config_dict.get('UTA_YOUTUBE_API_ENABLED', False)
    UTA_YOUTUBE_CLIENT_SECRET_FILE = source_config_dict.get('UTA_YOUTUBE_CLIENT_SECRET_FILE', "client_secret.json")
    UTA_YOUTUBE_TOKEN_FILE = source_config_dict.get('UTA_YOUTUBE_TOKEN_FILE', "youtube_token.json")
    UTA_YOUTUBE_PLAYLIST_ID = source_config_dict.get('UTA_YOUTUBE_PLAYLIST_ID')
    UTA_YOUTUBE_DEFAULT_PRIVACY = source_config_dict.get('UTA_YOUTUBE_DEFAULT_PRIVACY', "unlisted").lower()
    UTA_YOUTUBE_MAKE_PUBLIC_AFTER_STREAM = source_config_dict.get('UTA_YOUTUBE_MAKE_PUBLIC_AFTER_STREAM', False)
    UTA_YOUTUBE_SCHEDULED_ROLLOVER_HOURS = source_config_dict.get('UTA_YOUTUBE_SCHEDULED_ROLLOVER_HOURS', 0.0)
    UTA_YOUTUBE_DYNAMIC_TITLE_TEMPLATE = source_config_dict.get('UTA_YOUTUBE_DYNAMIC_TITLE_TEMPLATE', "{twitch_username} - {twitch_title} ({game_name}) - Part {part_num} [{date}]")
    UTA_YOUTUBE_DYNAMIC_DESCRIPTION_TEMPLATE = source_config_dict.get('UTA_YOUTUBE_DYNAMIC_DESCRIPTION_TEMPLATE', "Originally streamed by {twitch_username} on Twitch: https://twitch.tv/{twitch_username}\nGame: {game_name}\nTitle: {twitch_title}\n\nArchived by UTA.")
    UTA_RESTREAM_MAX_CONSECUTIVE_FAILURES = source_config_dict.get('UTA_RESTREAM_MAX_CONSECUTIVE_FAILURES', 3)
    UTA_RESTREAM_LONG_COOLDOWN_SECONDS = source_config_dict.get('UTA_RESTREAM_LONG_COOLDOWN_SECONDS', 300)
    UTA_YOUTUBE_PLAYABILITY_CHECK_ENABLED = source_config_dict.get('UTA_YOUTUBE_PLAYABILITY_CHECK_ENABLED', True)
    UTA_YOUTUBE_PLAYABILITY_CHECK_RETRIES = source_config_dict.get('UTA_YOUTUBE_PLAYABILITY_CHECK_RETRIES', 2)
    UTA_YOUTUBE_PLAYABILITY_CHECK_DELAY_SECONDS = source_config_dict.get('UTA_YOUTUBE_PLAYABILITY_CHECK_DELAY_SECONDS', 15)
    UTA_FFMPEG_STARTUP_WAIT_SECONDS = source_config_dict.get('UTA_FFMPEG_STARTUP_WAIT_SECONDS', 10)
    UTA_YOUTUBE_AUTO_CHAPTERS_ENABLED = source_config_dict.get('UTA_YOUTUBE_AUTO_CHAPTERS_ENABLED', True)
    UTA_YOUTUBE_MIN_CHAPTER_DURATION_SECONDS = source_config_dict.get('UTA_YOUTUBE_MIN_CHAPTER_DURATION_SECONDS', 60)
    UTA_YOUTUBE_DESCRIPTION_CHAPTER_MARKER = source_config_dict.get('UTA_YOUTUBE_DESCRIPTION_CHAPTER_MARKER', "## UTA Auto Chapters ##")
    UTA_YOUTUBE_CHAPTER_TITLE_TEMPLATE = source_config_dict.get('UTA_YOUTUBE_CHAPTER_TITLE_TEMPLATE', "{game_name} - {twitch_title}")

    # Apply Twitch Chat Monitor Configs
    TWITCH_CHAT_ENABLED = source_config_dict.get('TWITCH_CHAT_ENABLED', False)
    TWITCH_CHAT_NICKNAME = source_config_dict.get('TWITCH_CHAT_NICKNAME', "YourBotTwitchNickname")
    TWITCH_CHAT_OAUTH_TOKEN = source_config_dict.get('TWITCH_CHAT_OAUTH_TOKEN', "oauth:yourtwitchtoken")
    TWITCH_CHAT_LOG_INTERVAL_SECONDS = source_config_dict.get('TWITCH_CHAT_LOG_INTERVAL_SECONDS', 60)
    TWITCH_CHAT_ACTIVITY_LOG_FILE = source_config_dict.get('TWITCH_CHAT_ACTIVITY_LOG_FILE', "chat_activity.bin")
    DISCORD_TWITCH_CHAT_MIRROR_ENABLED = source_config_dict.get('DISCORD_TWITCH_CHAT_MIRROR_ENABLED', False)
    DISCORD_TWITCH_CHAT_MIRROR_CHANNEL_ID = int(source_config_dict.get('DISCORD_TWITCH_CHAT_MIRROR_CHANNEL_ID')) if source_config_dict.get('DISCORD_TWITCH_CHAT_MIRROR_CHANNEL_ID') else None


    from uta_bot.services.twitch_api_handler import TwitchAPIHelper
    if TWITCH_CLIENT_ID and TWITCH_CLIENT_SECRET:
        if fctd_twitch_api is None or \
           fctd_twitch_api.client_id != TWITCH_CLIENT_ID or \
           fctd_twitch_api.client_secret != TWITCH_CLIENT_SECRET:
            logger.info("Applying Config: Initializing/Re-initializing fctd_twitch_api.")
            fctd_twitch_api = TwitchAPIHelper(TWITCH_CLIENT_ID, TWITCH_CLIENT_SECRET)
    elif fctd_twitch_api is not None:
        logger.warning("Applying Config: Twitch Client ID/Secret missing or removed. Clearing fctd_twitch_api.")
        fctd_twitch_api = None

    logger.info("Configuration applied globally from source dictionary.")


def effective_youtube_api_enabled():
    return UTA_YOUTUBE_API_ENABLED and GOOGLE_API_AVAILABLE

_initial_success, _initial_config_data_dict = load_config(initial_load=True)
if not _initial_success:
    sys.exit(1)

apply_config_globally(config_data)

from uta_bot.utils.constants import *
from uta_bot.utils.formatters import *
from uta_bot.utils.data_logging import *