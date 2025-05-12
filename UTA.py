import requests
import time
import datetime
import logging
import subprocess
import os
import json
import shutil
import signal
import threading

try:
    import config
except ImportError:
    print("CRITICAL: config.py not found. Please create it and fill in your details.")
    exit(1)

# --- Setup Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s')

# --- Global Constants ---
TWITCH_API_BASE_URL = "https://api.twitch.tv/helix"
TWITCH_AUTH_URL = "https://id.twitch.tv/oauth2/token"

# --- Global State Variables ---
# Twitch Auth
shared_access_token = None
token_expiry_time = 0
token_refresh_lock = threading.Lock()

# Clip Monitor
broadcaster_id_cache = None
sent_clip_ids = set()

# Restreamer
streamlink_process = None
ffmpeg_process = None
is_restreaming_active = False # Tracks if WE are actively trying to restream

# Control
shutdown_event = threading.Event()

# --- Twitch API Helper Functions ---
def get_twitch_access_token():
    """Obtains or refreshes a Twitch API access token. Thread-safe."""
    global shared_access_token, token_expiry_time
    
    with token_refresh_lock: # Ensure only one thread tries to refresh at a time
        current_time = time.time()
        # Check if token exists and is valid (with a 60-second buffer)
        if shared_access_token and current_time < token_expiry_time - 60:
            return shared_access_token

        logging.info("Attempting to fetch/refresh Twitch API access token...")
        params = {
            "client_id": config.TWITCH_CLIENT_ID,
            "client_secret": config.TWITCH_CLIENT_SECRET,
            "grant_type": "client_credentials"
        }
        try:
            response = requests.post(TWITCH_AUTH_URL, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            shared_access_token = data["access_token"]
            token_expiry_time = current_time + data["expires_in"]
            logging.info("Successfully obtained/refreshed Twitch access token.")
            return shared_access_token
        except requests.exceptions.RequestException as e:
            logging.error(f"Error getting Twitch access token: {e}")
            if hasattr(response, 'text'): logging.error(f"Response content: {response.text}")
            shared_access_token = None # Invalidate on failure
            token_expiry_time = 0
            return None
        except KeyError:
            logging.error(f"Error parsing access token response: {response.text if hasattr(response, 'text') else 'No response text'}")
            shared_access_token = None
            token_expiry_time = 0
            return None

def _make_twitch_api_request(endpoint, params=None, method='GET', max_retries=1):
    """Makes a request to Twitch API, handles token refresh on 401."""
    url = f"{TWITCH_API_BASE_URL}/{endpoint.lstrip('/')}"
    
    for attempt in range(max_retries + 1):
        access_token = get_twitch_access_token()
        if not access_token:
            logging.error(f"Cannot make Twitch API request to {url}: No access token.")
            return None

        headers = {
            "Client-ID": config.TWITCH_CLIENT_ID,
            "Authorization": f"Bearer {access_token}"
        }

        try:
            if method.upper() == 'GET':
                response = requests.get(url, headers=headers, params=params, timeout=10)
            elif method.upper() == 'POST': # Add if needed later
                response = requests.post(url, headers=headers, json=params, timeout=10)
            else:
                logging.error(f"Unsupported HTTP method: {method}")
                return None

            if response.status_code == 401 and attempt < max_retries:
                logging.warning(f"Twitch API returned 401 for {url}. Invalidating token and retrying (attempt {attempt + 1}/{max_retries + 1}).")
                global shared_access_token, token_expiry_time
                with token_refresh_lock: # Lock to safely invalidate
                    shared_access_token = None
                    token_expiry_time = 0
                # Next iteration will call get_twitch_access_token() which will fetch a new one
                continue
            
            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            logging.error(f"Error during Twitch API request to {url} (attempt {attempt+1}): {e}")
            if hasattr(response, 'text'): logging.error(f"Response content: {response.text}")
            if attempt >= max_retries: # If it's the last attempt
                return None
            time.sleep(2**attempt) # Exponential backoff for retries on general errors
        except (KeyError, IndexError, json.JSONDecodeError) as e:
            logging.error(f"Error parsing Twitch API response from {url}: {e}")
            if hasattr(response, 'text'): logging.error(f"Response content: {response.text}")
            return None # Don't retry on parsing errors

    logging.error(f"Failed to complete request to {url} after {max_retries + 1} attempts.")
    return None


def get_broadcaster_id(channel_name):
    """Gets the Twitch User ID for a given channel name."""
    global broadcaster_id_cache
    if broadcaster_id_cache:
        return broadcaster_id_cache

    data = _make_twitch_api_request("/users", params={"login": channel_name})
    if data and data.get("data"):
        broadcaster_id_cache = data["data"][0]["id"]
        logging.info(f"Found broadcaster ID for {channel_name}: {broadcaster_id_cache}")
        return broadcaster_id_cache
    else:
        logging.error(f"Could not find broadcaster ID for channel: {channel_name}")
        return None

def get_recent_clips(broadcaster_id, lookback_minutes):
    """Fetches recent clips for a broadcaster."""
    if not broadcaster_id:
        logging.error("Cannot get clips without broadcaster ID.")
        return []

    end_time = datetime.datetime.utcnow()
    start_time = end_time - datetime.timedelta(minutes=lookback_minutes)
    started_at_str = start_time.isoformat("T") + "Z"

    params = {
        "broadcaster_id": broadcaster_id,
        "started_at": started_at_str,
        "first": 20
    }
    data = _make_twitch_api_request("/clips", params=params)
    return data.get("data", []) if data else []

def is_streamer_live(channel_name):
    """Checks if the streamer is live and returns stream data if they are."""
    params = {"user_login": channel_name}
    data = _make_twitch_api_request("/streams", params=params)
    if data and data.get("data") and data["data"][0].get("type") == "live":
        return True, data["data"][0]
    return False, None

# --- Discord Webhook Functions ---
def send_discord_clip_notification(clip_url, clip_title, channel_name):
    """Sends a new clip notification to Discord."""
    if not config.DISCORD_WEBHOOK_URL_CLIPS or "YOUR_DISCORD_WEBHOOK_URL" in config.DISCORD_WEBHOOK_URL_CLIPS:
        logging.warning("Discord webhook URL for clips is not configured or is a placeholder. Skipping notification.")
        return

    message = f"🎬 New clip from **{channel_name}**!\n**{clip_title}**\n{clip_url}"
    payload = {"content": message}
    try:
        response = requests.post(config.DISCORD_WEBHOOK_URL_CLIPS, json=payload, timeout=10)
        response.raise_for_status()
        logging.info(f"Successfully sent clip to Discord: {clip_url}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Error sending clip notification to Discord: {e}")
        if hasattr(response, 'text'): logging.error(f"Response content: {response.text}")

def send_discord_restream_status(status_type, username, stream_data=None):
    """Sends restream status (start/stop) to Discord."""
    if not config.DISCORD_WEBHOOK_URL_RESTREAMER or "YOUR_DISCORD_WEBHOOK_URL" in config.DISCORD_WEBHOOK_URL_RESTREAMER:
        logging.warning("Discord webhook URL for restreamer is not configured or is a placeholder. Skipping notification.")
        return

    color = 15158332  # Red for stop
    title_prefix = ":stop_button: Restream STOPPED"
    description = f"Restreaming of **{username}**'s Twitch stream to YouTube has stopped."

    if status_type == "start":
        color = 3066993  # Green for start
        title_prefix = ":satellite: Restream STARTED"
        stream_title = stream_data.get("title", "No Title") if stream_data else "N/A"
        game_name = stream_data.get("game_name", "N/A") if stream_data else "N/A"
        description = (f"Now restreaming **{username}** to YouTube.\n"
                       f"Twitch Title: **{stream_title}**\n"
                       f"Game: **{game_name}**\n"
                       f"[Watch on Twitch](https://twitch.tv/{username})")
    
    payload = {
        "content": f"{title_prefix} for **{username}**",
        "embeds": [{
            "title": title_prefix,
            "description": description,
            "color": color,
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "author": {"name": username, "url": f"https://twitch.tv/{username}"},
            "footer": {"text": "Twitch Monitor & Resteramer"}
        }]
    }
    try:
        response = requests.post(config.DISCORD_WEBHOOK_URL_RESTREAMER, json=payload, timeout=10)
        response.raise_for_status()
        logging.info(f"Sent Discord notification for restream {status_type} for {username}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Error sending restream status to Discord: {e}")
        if hasattr(response, 'text'): logging.error(f"Response content: {response.text}")

# --- Restreamer Core Functions ---
def terminate_process(process, name):
    if process and process.poll() is None:
        logging.info(f"Terminating {name} process (PID: {process.pid})...")
        try:
            process.terminate() # Ask nicely first
            process.wait(timeout=10)
            logging.info(f"{name} process terminated (Exit Code: {process.poll()}).")
        except subprocess.TimeoutExpired:
            logging.warning(f"{name} (PID: {process.pid}) did not terminate gracefully, killing...")
            process.kill() # Force kill
            process.wait()
            logging.info(f"{name} process killed (Exit Code: {process.poll()}).")
        except Exception as e:
            logging.error(f"Error during {name} process termination (PID: {process.pid}): {e}")

def cleanup_restream_processes():
    """Cleans up Streamlink and FFmpeg processes."""
    global streamlink_process, ffmpeg_process, is_restreaming_active
    logging.info("Cleaning up restream processes...")
    terminate_process(ffmpeg_process, "FFmpeg")
    ffmpeg_process = None
    terminate_process(streamlink_process, "Streamlink")
    streamlink_process = None
    is_restreaming_active = False # Ensure state reflects that processes are stopped
    logging.info("Restream process cleanup finished.")

def start_restream(username):
    """Starts the Streamlink to FFmpeg restreaming pipeline."""
    global streamlink_process, ffmpeg_process, is_restreaming_active

    if not config.YOUTUBE_STREAM_KEY or "YOUR_YOUTUBE_STREAM_KEY" in config.YOUTUBE_STREAM_KEY:
        logging.error("YouTube Stream Key is missing or placeholder in config.py. Cannot start restream.")
        return False # Indicate failure

    stream_url_twitch = f"twitch.tv/{username}"
    # Ensure no double slashes if YOUTUBE_RTMP_URL_BASE ends with /
    youtube_rtmp_full_url = f"{config.YOUTUBE_RTMP_URL_BASE.rstrip('/')}/{config.YOUTUBE_STREAM_KEY}"

    logging.info(f"Attempting to start restream for {username}...")
    logging.info(f"  Twitch Source: {stream_url_twitch}")
    logging.info(f"  YouTube Target: {config.YOUTUBE_RTMP_URL_BASE.rstrip('/')}/<YOUR_STREAM_KEY>")

    sl_command = [
        config.STREAMLINK_PATH,
        "--stdout",
        stream_url_twitch,
        "best", # You might want to specify resolutions like 720p,1080p if "best" is too much
        "--twitch-disable-hosting",
        "--hls-live-restart", # Try to restart if stream ends and comes back
        "--retry-streams", "5", # Retry fetching the stream 5 times
        "--retry-open", "3",   # Retry opening the stream 3 times
        # "--loglevel", "debug" # For streamlink debugging
    ]

    ffmpeg_command = [
        config.FFMPEG_PATH,
        "-hide_banner",
        "-i", "pipe:0",          # Input from stdin (Streamlink's stdout)

        # Video Settings: Still copy video to save CPU
        "-c:v", "copy",

        # Audio Settings: Transcode to AAC (standard for RTMP)
        "-c:a", "aac",           # Explicitly encode audio to AAC
        "-b:a", "160k",          # Set audio bitrate (e.g., 160kbps - adjust if needed 128k/192k)
        "-ar", "44100",          # Optional: Set audio sample rate (44.1kHz is common) - remove if unsure
        # "-ac", "2",            # Optional: Force stereo audio - remove if unsure

        # Mapping: Be slightly more explicit, map first video and first audio stream if they exist
        "-map", "0:v:0?",        # Map the first video stream (if present)
        "-map", "0:a:0?",        # Map the first audio stream (if present)

        # Output Settings
        "-f", "flv",             # Output format for RTMP
        "-bufsize", "4000k",     # Output buffer
        "-flvflags", "no_duration_filesize", # Recommended for live FLV
        # "-async", "1",         # Optional: Uncomment this if audio is out of sync after transcoding
                                 # (-async 1 synchronizes audio to timestamps, might drop/dup samples)
        "-loglevel", "warning",  # Changed to warning to see slightly more info if issues persist
        youtube_rtmp_full_url
    ]

    try:
        logging.info("Starting Streamlink process...")
        # Capture Streamlink's stderr to see its errors/info if needed
        streamlink_process = subprocess.Popen(sl_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        logging.info(f"Streamlink PID: {streamlink_process.pid}")

        logging.info("Starting FFmpeg process...")
        # FFmpeg's stderr will contain its operational logs/errors
        ffmpeg_process = subprocess.Popen(ffmpeg_command, stdin=streamlink_process.stdout, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        logging.info(f"FFmpeg PID: {ffmpeg_process.pid}")

        # Allow Streamlink to pass its stdout to FFmpeg, then close our reference to it.
        if streamlink_process.stdout:
            streamlink_process.stdout.close()
        
        is_restreaming_active = True
        logging.info(f"Restreaming started for {username}. Monitoring FFmpeg process...")

        # Monitor FFmpeg's stderr for errors while it's running
        ffmpeg_stderr_output = ""
        if ffmpeg_process.stderr:
            for line in iter(ffmpeg_process.stderr.readline, b''):
                if shutdown_event.is_set(): # Check for shutdown signal while reading
                    logging.info("Shutdown signal received, stopping FFmpeg log reading.")
                    break
                decoded_line = line.decode('utf-8', errors='ignore').strip()
                logging.debug(f"FFMPEG_LOG: {decoded_line}") # Log all ffmpeg output at debug level
                ffmpeg_stderr_output += decoded_line + "\n"
                # Check if Streamlink died unexpectedly
                if streamlink_process and streamlink_process.poll() is not None:
                    logging.warning(f"Streamlink process (PID: {streamlink_process.pid}) ended unexpectedly (Exit Code: {streamlink_process.returncode}) while FFmpeg was running.")
                    break # Stop reading FFmpeg logs as input is gone
            ffmpeg_process.stderr.close()

        logging.info("Waiting for FFmpeg process to exit...")
        ffmpeg_process.wait() # Wait for FFmpeg to finish
        ff_exit_code = ffmpeg_process.poll()
        logging.info(f"FFmpeg process (PID: {ffmpeg_process.pid if ffmpeg_process else 'N/A'}) exited with code: {ff_exit_code}")

        if ff_exit_code != 0 and ff_exit_code is not None: # None if terminated by signal and not by itself
             logging.error("--- FFmpeg Error Log ---")
             for line in ffmpeg_stderr_output.splitlines():
                 if line.strip(): logging.error(line)
             logging.error("--- End FFmpeg Error Log ---")
        
        # Check Streamlink's status after FFmpeg exits (or if it exited early)
        sl_exit_code = None
        if streamlink_process:
            sl_exit_code = streamlink_process.poll()
            if sl_exit_code is None: # If Streamlink is still running after FFmpeg exited (unlikely)
                 logging.warning("FFmpeg exited, but Streamlink is still running. Terminating Streamlink...")
                 terminate_process(streamlink_process, "Streamlink")
            else:
                 logging.info(f"Streamlink process had already exited with code: {sl_exit_code}")
                 if streamlink_process.stderr: # Log Streamlink's stderr if it hasn't been read
                      sl_stderr_output = ""
                      try:
                          # Non-blocking read for any remaining stderr from Streamlink
                          sl_stderr_bytes = streamlink_process.stderr.read()
                          if sl_stderr_bytes:
                              sl_stderr_output = sl_stderr_bytes.decode('utf-8', errors='ignore').strip()
                          if sl_stderr_output:
                              logging.info("--- Streamlink Stderr Log ---")
                              for line in sl_stderr_output.splitlines():
                                 if line.strip(): logging.info(line)
                              logging.info("--- End Streamlink Stderr Log ---")
                      except Exception as e:
                          logging.debug(f"Could not read streamlink stderr: {e}")
                      finally:
                          streamlink_process.stderr.close()
        return True # Indicates restream session ran (even if it failed)

    except FileNotFoundError as e:
        logging.critical(f"ERROR: Command not found (Streamlink or FFmpeg). Ensure they are installed and in PATH, or configure paths in config.py. Details: {e}")
        # Check which one specifically
        if str(e.filename) == config.STREAMLINK_PATH:
            logging.critical(f"'{config.STREAMLINK_PATH}' not found.")
        elif str(e.filename) == config.FFMPEG_PATH:
            logging.critical(f"'{config.FFMPEG_PATH}' not found.")
        return False # Indicate failure
    except Exception as e:
        logging.error(f"An unexpected critical error occurred during restreaming setup or execution: {e}", exc_info=True)
        return False # Indicate failure
    finally:
        # This finally block ensures cleanup happens if start_restream exits for any reason
        # Note: cleanup_restream_processes also sets is_restreaming_active to False
        # However, if we are shutting down globally, the main signal handler will call it.
        # This specific call is more for when start_restream itself exits.
        if not shutdown_event.is_set(): # Only do this if not part of global shutdown
            current_slp = streamlink_process
            current_ffp = ffmpeg_process
            streamlink_process = None # Avoid cleanup_restream_processes in signal handler re-terminating
            ffmpeg_process = None
            terminate_process(current_ffp, "FFmpeg (finally)")
            terminate_process(current_slp, "Streamlink (finally)")
        is_restreaming_active = False # Explicitly set here to reflect end of attempt

# --- Clip Monitor Thread Function ---
def clip_monitor_loop():
    logging.info("Clip Monitor thread started.")
    
    # Initial fetch of broadcaster_id for clips
    b_id = get_broadcaster_id(config.TWITCH_CHANNEL_NAME)
    if not b_id:
        logging.error(f"Clip Monitor: Failed to get broadcaster ID for {config.TWITCH_CHANNEL_NAME}. Will retry later.")
    
    # Prime sent_clip_ids on first run
    if b_id:
        logging.info(f"Clip Monitor: Performing initial clip scan for the last {config.CLIP_LOOKBACK_MINUTES} minutes to prime known clips...")
        initial_clips = get_recent_clips(b_id, config.CLIP_LOOKBACK_MINUTES)
        for clip_data in initial_clips:
            sent_clip_ids.add(clip_data['id'])
        logging.info(f"Clip Monitor: Primed {len(sent_clip_ids)} clips. Monitoring for new ones.")

    while not shutdown_event.is_set():
        try:
            current_b_id = broadcaster_id_cache or get_broadcaster_id(config.TWITCH_CHANNEL_NAME)
            if not current_b_id:
                logging.warning(f"Clip Monitor: No broadcaster ID for {config.TWITCH_CHANNEL_NAME}. Skipping clip check cycle.")
                if shutdown_event.wait(timeout=config.CHECK_INTERVAL_SECONDS_CLIPS): break
                continue

            logging.info(f"Clip Monitor: Checking for new clips for {config.TWITCH_CHANNEL_NAME}...")
            clips = get_recent_clips(current_b_id, config.CLIP_LOOKBACK_MINUTES)
            
            if not clips:
                logging.info("Clip Monitor: No clips found in the lookback window.")
            else:
                new_clips_found = 0
                # Process in chronological order (Twitch API usually returns newest first)
                for clip_data in reversed(clips):
                    if shutdown_event.is_set(): break
                    if clip_data['id'] not in sent_clip_ids:
                        clip_url = clip_data['url']
                        clip_title = clip_data['title']
                        logging.info(f"Clip Monitor: New clip found: {clip_title} - {clip_url}")
                        send_discord_clip_notification(clip_url, clip_title, config.TWITCH_CHANNEL_NAME)
                        sent_clip_ids.add(clip_data['id'])
                        new_clips_found += 1
                        if shutdown_event.wait(timeout=1): break # Small delay, interruptible
                if new_clips_found == 0 and clips: # Clips were found, but all were old
                    logging.info("Clip Monitor: No *new* clips found (all fetched clips already sent).")
            
            if shutdown_event.is_set(): break
            logging.info(f"Clip Monitor: Waiting for {config.CHECK_INTERVAL_SECONDS_CLIPS // 60} minutes ({config.CHECK_INTERVAL_SECONDS_CLIPS}s) before next clip check...")
            if shutdown_event.wait(timeout=config.CHECK_INTERVAL_SECONDS_CLIPS): break
        
        except Exception as e:
            logging.error(f"Clip Monitor: An unexpected error occurred: {e}", exc_info=True)
            if shutdown_event.wait(timeout=60): break # Wait a bit before retrying on error

    logging.info("Clip Monitor thread finished.")

# --- Restreamer Monitor Thread Function ---
def restreamer_monitor_loop():
    global is_restreaming_active # We modify this based on subprocess state
    logging.info("Restreamer Monitor thread started.")

    while not shutdown_event.is_set():
        try:
            live, stream_data = is_streamer_live(config.TWITCH_CHANNEL_NAME)

            if live and not is_restreaming_active:
                logging.info(f"Restreamer: {config.TWITCH_CHANNEL_NAME} is LIVE! Starting restream...")
                send_discord_restream_status("start", config.TWITCH_CHANNEL_NAME, stream_data)
                
                # start_restream is blocking and handles its own process management
                restream_successful_run = start_restream(config.TWITCH_CHANNEL_NAME)
                # is_restreaming_active should be False after start_restream finishes or fails
                
                logging.info(f"Restreamer: Restream attempt for {config.TWITCH_CHANNEL_NAME} concluded.")
                send_discord_restream_status("stop", config.TWITCH_CHANNEL_NAME) # Always send stop after attempt
                
                logging.info(f"Restreamer: Waiting {config.POST_RESTREAM_COOLDOWN_SECONDS}s after restream ended before next check...")
                if shutdown_event.wait(timeout=config.POST_RESTREAM_COOLDOWN_SECONDS): break
            
            elif live and is_restreaming_active:
                # This state means we think we are restreaming, but the outer loop is checking again.
                # This could happen if the CHECK_INTERVAL_SECONDS_RESTREAMER is very short,
                # or if start_restream exited but is_restreaming_active wasn't reset (should be handled by start_restream's finally).
                # For now, just log and wait for the configured "while live" check interval.
                logging.info(f"Restreamer: {config.TWITCH_CHANNEL_NAME} is still live. Current restream process should be active. Will re-verify stream status in {config.RESTREAM_CHECK_INTERVAL_WHEN_LIVE}s.")
                if shutdown_event.wait(timeout=config.RESTREAM_CHECK_INTERVAL_WHEN_LIVE): break
            
            elif not live and is_restreaming_active:
                # Streamer went offline, but we thought we were restreaming.
                # This implies FFmpeg/Streamlink might still be running if they didn't detect stream end.
                logging.warning(f"Restreamer: {config.TWITCH_CHANNEL_NAME} appears offline, but restream was marked active. Cleaning up processes.")
                cleanup_restream_processes() # This will set is_restreaming_active to False
                send_discord_restream_status("stop", config.TWITCH_CHANNEL_NAME) # Ensure stop is sent
                logging.info(f"Restreamer: Waiting {config.CHECK_INTERVAL_SECONDS_RESTREAMER}s...")
                if shutdown_event.wait(timeout=config.CHECK_INTERVAL_SECONDS_RESTREAMER): break
            
            elif not live and not is_restreaming_active:
                logging.info(f"Restreamer: {config.TWITCH_CHANNEL_NAME} is offline. Waiting {config.CHECK_INTERVAL_SECONDS_RESTREAMER}s...")
                if shutdown_event.wait(timeout=config.CHECK_INTERVAL_SECONDS_RESTREAMER): break
        
        except Exception as e:
            logging.error(f"Restreamer Monitor: An unexpected error occurred: {e}", exc_info=True)
            logging.info("Restreamer Monitor: Cleaning up processes due to error...")
            cleanup_restream_processes() # Ensure cleanup on error
            if shutdown_event.wait(timeout=60): break # Wait a bit before retrying loop

    logging.info("Restreamer Monitor thread finished.")

# --- Signal Handler ---
def signal_handler(sig, frame):
    logging.warning(f"Signal {signal.Signals(sig).name} received. Initiating graceful shutdown...")
    shutdown_event.set() # Signal all threads to stop
    # Crucially, try to stop external processes immediately as threads might be blocked
    cleanup_restream_processes()

# --- Main Execution ---
if __name__ == "__main__":
    # Config Validation
    critical_configs = {
        "TWITCH_CLIENT_ID": config.TWITCH_CLIENT_ID,
        "TWITCH_CLIENT_SECRET": config.TWITCH_CLIENT_SECRET,
        "TWITCH_CHANNEL_NAME": config.TWITCH_CHANNEL_NAME,
    }
    missing_configs = [k for k, v in critical_configs.items() if not v or "YOUR_" in v or v == "target_twitch_channel_name"]
    if missing_configs:
        logging.critical(f"Essential configuration variables are missing or have placeholder values in config.py: {', '.join(missing_configs)}")
        logging.critical("Please update config.py with your actual details.")
        exit(1)
    
    if not config.YOUTUBE_STREAM_KEY or "YOUR_YOUTUBE_STREAM_KEY" in config.YOUTUBE_STREAM_KEY:
        logging.warning("YOUTUBE_STREAM_KEY is not set or is a placeholder. Restreaming functionality will be disabled.")
    
    if not shutil.which(config.STREAMLINK_PATH):
        logging.critical(f"CRITICAL ERROR: '{config.STREAMLINK_PATH}' command not found. Check installation and PATH, or set STREAMLINK_PATH in config.py.")
        exit(1)
    if not shutil.which(config.FFMPEG_PATH):
        logging.critical(f"CRITICAL ERROR: '{config.FFMPEG_PATH}' command not found. Check installation and PATH, or set FFMPEG_PATH in config.py.")
        exit(1)

    logging.info("--- Twitch Monitor & Restreamer Starting ---")

    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Initial Twitch token fetch
    if not get_twitch_access_token():
        logging.critical("Failed to get initial Twitch access token. Exiting.")
        exit(1)

    # Create and start threads
    clip_thread = threading.Thread(target=clip_monitor_loop, name="ClipMonitor")
    restreamer_thread = threading.Thread(target=restreamer_monitor_loop, name="RestreamerMonitor")

    clip_thread.start()
    restreamer_thread.start()

    # Keep main thread alive until shutdown event or threads finish
    try:
        while not shutdown_event.is_set() and clip_thread.is_alive() and restreamer_thread.is_alive():
            # This loop primarily exists to keep the main thread responsive to signals
            # if the worker threads themselves don't handle shutdown_event.wait() frequently.
            # With shutdown_event.wait(timeout=...) in worker loops, this main loop
            # becomes less critical for responsiveness but good for clean exit.
            time.sleep(1) # Check every second
    except KeyboardInterrupt: # Should be caught by signal handler, but as a fallback
        logging.info("KeyboardInterrupt in main thread. Shutting down...")
        shutdown_event.set()
        cleanup_restream_processes()

    logging.info("Waiting for threads to complete...")
    clip_thread.join(timeout=15) # Give some time for threads to finish
    restreamer_thread.join(timeout=15)

    if clip_thread.is_alive():
        logging.warning("Clip monitor thread did not shut down cleanly.")
    if restreamer_thread.is_alive():
        logging.warning("Restreamer monitor thread did not shut down cleanly.")
        # Final attempt to ensure processes are gone if thread is stuck
        cleanup_restream_processes()

    logging.info("--- Twitch Monitor & Restreamer Finished ---")
