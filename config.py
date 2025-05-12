# --- Twitch API Configuration ---
TWITCH_CLIENT_ID = "HERE"  # Replace with your actual Client ID
TWITCH_CLIENT_SECRET = "HERE"  # Replace with your actual Client Secret
TWITCH_CHANNEL_NAME = "HERE"  # Twitch channel to monitor for clips and restream

# --- Clip Monitor Configuration ---
DISCORD_WEBHOOK_URL_CLIPS = "HEREi"  # Webhook for clip notifications
CHECK_INTERVAL_SECONDS_CLIPS = 30  # 5 minutes, How often to check for new clips
CLIP_LOOKBACK_MINUTES = 2  # How far back to look for clips to avoid missing any due to timing

# --- Restreamer Configuration ---
DISCORD_WEBHOOK_URL_RESTREAMER = "HERE"  # Webhook for restreamer status
YOUTUBE_RTMP_URL_BASE = "rtmp://a.rtmp.youtube.com/live2"  # YouTube RTMP base URL
YOUTUBE_STREAM_KEY = "HERE"  # Your YouTube stream key
CHECK_INTERVAL_SECONDS_RESTREAMER = 15 # How often to check if the streamer is live (when not restreaming)
RESTREAM_CHECK_INTERVAL_WHEN_LIVE = 60 # How often to re-verify live status while already restreaming (to detect potential issues)
POST_RESTREAM_COOLDOWN_SECONDS = 60 # How long to wait after a restream session ends before checking again

STREAMLINK_PATH = "streamlink" # Or full path e.g., "C:\\Program Files (x86)\\Streamlink\\bin\\streamlink.exe"
FFMPEG_PATH = "ffmpeg"       # Or full path e.g., "C:\\FFmpeg\\bin\\ffmpeg.exe"
