import logging
import time
import shutil
import subprocess
import os
import asyncio
from datetime import datetime, timedelta, timezone
import requests # For Discord webhook status

from uta_bot import config_manager
from uta_bot.services.twitch_api_handler import make_uta_twitch_api_request
from uta_bot.services.youtube_api_handler import (
    get_youtube_service, create_youtube_live_stream_resource, create_youtube_broadcast,
    transition_youtube_broadcast, update_youtube_broadcast_metadata,
    add_video_to_youtube_playlist, set_youtube_video_privacy,
    get_youtube_video_details
)
from .threading_manager import shutdown_event
from uta_bot.utils.data_logging import log_stream_duration_binary, parse_stream_activity_for_game_segments, get_viewer_stats_for_period
from uta_bot.utils.formatters import format_duration_human, format_seconds_to_hhmmss # Added format_seconds_to_hhmmss
from uta_bot.utils.chapter_utils import generate_chapter_text
import threading

logger = logging.getLogger(__name__)

_streamlink_process = None
_ffmpeg_process = None

def _send_discord_restream_status(status_type: str, username: str, stream_data=None, stream_duration_seconds=None):
    if not config_manager.UTA_DISCORD_WEBHOOK_URL_RESTREAMER or \
       "YOUR_DISCORD_WEBHOOK_URL" in config_manager.UTA_DISCORD_WEBHOOK_URL_RESTREAMER:
        logger.warning("UTA Restream Service: Discord webhook URL for restreamer status is not configured or is a placeholder.")
        return

    color = 15158332 if status_type == "stop" else 3066993
    title_text = f":{'stop_button' if status_type == 'stop' else 'satellite'}: Restream {'STOPPED' if status_type == 'stop' else 'STARTED'}"
    description = f"Restream of **{username}** to YouTube has {'stopped' if status_type == 'stop' else 'started'}."

    if status_type == "start" and stream_data:
        s_title = stream_data.get("title", "N/A")
        game = stream_data.get("game_name", "N/A")
        description = (f"Now restreaming **{username}** to YouTube.\n"
                       f"Twitch Title: **{s_title}**\n"
                       f"Game: **{game}**\n"
                       f"[Watch on Twitch](https://twitch.tv/{username})")
    elif status_type == "stop" and stream_duration_seconds is not None and stream_duration_seconds > 0:
        description += f"\nStream lasted for: **{format_duration_human(int(stream_duration_seconds))}**."

    current_time_utc_iso = datetime.now(timezone.utc).isoformat()
    payload = {
        "content": f"{title_text} for **{username}**",
        "embeds": [{
            "title": title_text,
            "description": description,
            "color": color,
            "timestamp": current_time_utc_iso,
            "author": {
                "name": username,
                "url": f"https://twitch.tv/{username}",
            },
            "footer": {"text": "UTA Bot - Twitch Monitor & Restreamer"}
        }]
    }

    try:
        response = requests.post(config_manager.UTA_DISCORD_WEBHOOK_URL_RESTREAMER, json=payload, timeout=10)
        response.raise_for_status()
        logger.info(f"UTA Restream Service: Sent Discord restream {status_type} notification for {username}.")
    except requests.exceptions.RequestException as e:
        logger.error(f"UTA Restream Service: Error sending restream status to Discord: {e}")


def _terminate_process(process, name: str):
    if process and process.poll() is None:
        logger.info(f"UTA Restream Service: Terminating {name} (PID: {process.pid})...")
        try:
            process.terminate()
            process.wait(timeout=10)
            logger.info(f"UTA Restream Service: {name} (PID: {process.pid}) terminated gracefully (Code: {process.poll()}).")
        except subprocess.TimeoutExpired:
            logger.warning(f"UTA Restream Service: {name} (PID: {process.pid}) did not terminate gracefully after 10s, killing...")
            process.kill()
            process.wait()
            logger.info(f"UTA Restream Service: {name} (PID: {process.pid}) process killed (Code: {process.poll()}).")
        except Exception as e:
            logger.error(f"UTA Restream Service: Error during termination of {name} (PID: {process.pid}): {e}")

def cleanup_restream_processes():
    global _streamlink_process, _ffmpeg_process
    logger.info("UTA Restream Service: Cleaning up active restream processes...")
    _terminate_process(_ffmpeg_process, "FFmpeg")
    _ffmpeg_process = None
    config_manager.UTA_FFMPEG_PID = None

    _terminate_process(_streamlink_process, "Streamlink")
    _streamlink_process = None
    config_manager.UTA_STREAMLINK_PID = None

    config_manager.uta_is_restreaming_active = False
    logger.info("UTA Restream Service: Restream process cleanup finished.")

async def _check_youtube_playability(video_id: str, bot_loop) -> bool:
    if not video_id or not config_manager.STREAMLINK_LIB_AVAILABLE or not config_manager.UTA_YOUTUBE_PLAYABILITY_CHECK_ENABLED:
        if config_manager.UTA_YOUTUBE_PLAYABILITY_CHECK_ENABLED and not config_manager.STREAMLINK_LIB_AVAILABLE:
            logger.warning("UTA YouTube Health Check: Streamlink library not available, skipping playability check.")
        config_manager.UTA_LAST_PLAYABILITY_CHECK_STATUS = "Skipped"
        config_manager.logger.info(f"UTA_GUI_LOG: PlayabilityCheckStatus={config_manager.UTA_LAST_PLAYABILITY_CHECK_STATUS}")
        return True

    youtube_watch_url = f"https://www.youtube.com/watch?v={video_id}"
    logger.info(f"UTA YouTube Health Check: Verifying playability of {youtube_watch_url}...")
    config_manager.UTA_LAST_PLAYABILITY_CHECK_STATUS = f"Pending for {video_id}"
    config_manager.logger.info(f"UTA_GUI_LOG: PlayabilityCheckStatus={config_manager.UTA_LAST_PLAYABILITY_CHECK_STATUS}")

    for attempt in range(config_manager.UTA_YOUTUBE_PLAYABILITY_CHECK_RETRIES):
        if shutdown_event.is_set():
            config_manager.UTA_LAST_PLAYABILITY_CHECK_STATUS = "Cancelled (Shutdown)"
            config_manager.logger.info(f"UTA_GUI_LOG: PlayabilityCheckStatus={config_manager.UTA_LAST_PLAYABILITY_CHECK_STATUS}")
            return False
        try:
            streams = await asyncio.to_thread(config_manager.streamlink.streams, youtube_watch_url)
            if streams and ("best" in streams or "worst" in streams or "live" in streams or "audio_only" in streams or "audio" in streams):
                logger.info(f"UTA YouTube Health Check: Stream {video_id} confirmed playable via streamlink (Attempt {attempt+1}).")
                config_manager.UTA_LAST_PLAYABILITY_CHECK_STATUS = f"Passed for {video_id}"
                config_manager.logger.info(f"UTA_GUI_LOG: PlayabilityCheckStatus={config_manager.UTA_LAST_PLAYABILITY_CHECK_STATUS}")
                return True
            else:
                logger.warning(f"UTA YouTube Health Check: No playable streams found for {video_id} via streamlink (Attempt {attempt + 1}/{config_manager.UTA_YOUTUBE_PLAYABILITY_CHECK_RETRIES}). Streams: {streams.keys() if streams else 'None'}")
        except config_manager.streamlink.exceptions.NoPluginError:
            logger.error(f"UTA YouTube Health Check: Streamlink has no plugin for YouTube (unexpected). Cannot verify {video_id}.")
            config_manager.UTA_LAST_PLAYABILITY_CHECK_STATUS = f"Error (NoPlugin) for {video_id}"
            config_manager.logger.info(f"UTA_GUI_LOG: PlayabilityCheckStatus={config_manager.UTA_LAST_PLAYABILITY_CHECK_STATUS}")
            return False
        except config_manager.streamlink.exceptions.PluginError as e:
            logger.warning(f"UTA YouTube Health Check: Streamlink PluginError for {video_id} (Attempt {attempt + 1}/{config_manager.UTA_YOUTUBE_PLAYABILITY_CHECK_RETRIES}): {e}")
        except Exception as e:
            logger.error(f"UTA YouTube Health Check: Unexpected error verifying {video_id} (Attempt {attempt + 1}/{config_manager.UTA_YOUTUBE_PLAYABILITY_CHECK_RETRIES}): {e}", exc_info=True)

        if attempt < config_manager.UTA_YOUTUBE_PLAYABILITY_CHECK_RETRIES - 1:
            logger.info(f"UTA YouTube Health Check: Retrying playability check in {config_manager.UTA_YOUTUBE_PLAYABILITY_CHECK_DELAY_SECONDS}s...")
            if shutdown_event.wait(timeout=config_manager.UTA_YOUTUBE_PLAYABILITY_CHECK_DELAY_SECONDS):
                config_manager.UTA_LAST_PLAYABILITY_CHECK_STATUS = "Cancelled (Shutdown during retry)"
                config_manager.logger.info(f"UTA_GUI_LOG: PlayabilityCheckStatus={config_manager.UTA_LAST_PLAYABILITY_CHECK_STATUS}")
                logger.info("UTA YouTube Health Check: Shutdown during retry sleep.")
                return False

    logger.error(f"UTA YouTube Health Check: Failed to confirm playability of {video_id} after {config_manager.UTA_YOUTUBE_PLAYABILITY_CHECK_RETRIES} attempts.")
    config_manager.UTA_LAST_PLAYABILITY_CHECK_STATUS = f"Failed for {video_id}"
    config_manager.logger.info(f"UTA_GUI_LOG: PlayabilityCheckStatus={config_manager.UTA_LAST_PLAYABILITY_CHECK_STATUS}")
    return False

def _generate_enhanced_youtube_description(
        twitch_username: str,
        twitch_title: str,
        current_game_name: str,
        part_num: int,
        vod_part_start_utc: datetime,
        vod_part_end_utc: datetime = None, # Optional: if None, means current part is ongoing
        existing_description_base: str = None # For appending, e.g., after chapters
    ) -> str:
    """Generates an enhanced YouTube description."""

    base_desc_template = config_manager.UTA_YOUTUBE_DYNAMIC_DESCRIPTION_TEMPLATE
    description_parts = [
        base_desc_template.format(
            twitch_username=twitch_username,
            twitch_title=twitch_title,
            game_name=current_game_name # Game at the start of this part or current game
        )
    ]

    # Add list of games played in this VOD part
    if config_manager.UTA_STREAM_ACTIVITY_LOG_FILE and os.path.exists(config_manager.UTA_STREAM_ACTIVITY_LOG_FILE):
        part_start_unix = int(vod_part_start_utc.timestamp())
        # If part is ongoing, use current time as temp end for parsing segments *within this part so far*
        # If part has ended, use actual end time.
        part_end_unix_for_segment_parsing = int((vod_part_end_utc or datetime.now(timezone.utc)).timestamp())

        game_segments_this_part = parse_stream_activity_for_game_segments(
            config_manager.UTA_STREAM_ACTIVITY_LOG_FILE,
            part_start_unix,
            part_end_unix_for_segment_parsing
        )

        if game_segments_this_part:
            description_parts.append("\n\nGames played in this part:")
            unique_games_in_part = []
            for seg in sorted(game_segments_this_part, key=lambda x: x['start_ts']):
                game = seg.get('game', "Unknown Game")
                # Create simple timestamped game changes if desired (like chapters but in description)
                # relative_start_seconds = seg['start_ts'] - part_start_unix
                # time_marker = format_seconds_to_hhmmss(relative_start_seconds) # Use imported helper
                # description_parts.append(f"- {time_marker} {game}")
                if game not in unique_games_in_part and game != "N/A": # Filter out "N/A" games for list
                    unique_games_in_part.append(game)
            if unique_games_in_part:
                 description_parts.append("- " + "\n- ".join(unique_games_in_part))
            elif not any(seg.get('game') and seg.get('game') != "N/A" for seg in game_segments_this_part): # If all games were N/A
                 description_parts.append("- Game details not available for this part")
            else: # Should not happen if game_segments_this_part is true and filtering logic above is complete
                 description_parts.append("- Various games (see chapters if available)")


    # Placeholder for future: Top chatters or emotes (requires significant chat logging changes)
    # description_parts.append("\n\nTop Chatters this session: ...")

    if existing_description_base: # e.g., if chapters were already added
        return existing_description_base + "\n\n" + "\n".join(description_parts)

    return "\n".join(description_parts)[:5000] # YouTube description limit

def _start_restream_pipe(username: str, youtube_rtmp_url: str, youtube_stream_key: str, bot_loop):
    global _streamlink_process, _ffmpeg_process

    config_manager.UTA_FFMPEG_PID, config_manager.UTA_STREAMLINK_PID = None, None
    config_manager.UTA_PIPE_START_TIME_UTC = datetime.now(timezone.utc) # Set pipe start time here
    config_manager.logger.info("UTA_GUI_LOG: RestreamPipeStatus=Starting")

    if not youtube_stream_key or "YOUR_YOUTUBE_STREAM_KEY" in youtube_stream_key or not youtube_rtmp_url:
        logger.error(f"UTA Restream Service: YouTube RTMP URL ('{youtube_rtmp_url}') or Stream Key is not valid or is a placeholder. Cannot start restream pipe.")
        return False

    full_youtube_destination_url = f"{youtube_rtmp_url.rstrip('/')}/{youtube_stream_key}"
    logger.info(f"UTA Restream Service: Attempting to start restream pipe for {username} to {youtube_rtmp_url.rstrip('/')}/<STREAM_KEY>")

    streamlink_command = [
        config_manager.UTA_STREAMLINK_PATH,
        "--stdout",
        f"twitch.tv/{username}",
        "best",
        "--twitch-disable-hosting",
        "--hls-live-restart",
        "--retry-streams", "5",
        "--retry-open", "3"
    ]
    ffmpeg_command = [
        config_manager.UTA_FFMPEG_PATH,
        "-hide_banner",
        "-i", "pipe:0",
        "-c:v", "copy",
        "-c:a", "aac",
        "-b:a", "160k",
        "-map", "0:v:0?",
        "-map", "0:a:0?",
        "-f", "flv",
        "-bufsize", "4000k",
        "-flvflags", "no_duration_filesize",
        "-loglevel", "warning",
        full_youtube_destination_url
    ]

    current_streamlink_process, current_ffmpeg_process = None, None
    stream_pipe_successfully_started = False

    try:
        logger.info("UTA Restream Service: Starting Streamlink process...")
        startupinfo = None
        if os.name == 'nt':
            startupinfo = subprocess.STARTUPINFO()
            startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
            startupinfo.wShowWindow = subprocess.SW_HIDE

        current_streamlink_process = subprocess.Popen(
            streamlink_command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            startupinfo=startupinfo
        )
        config_manager.UTA_STREAMLINK_PID = current_streamlink_process.pid
        _streamlink_process = current_streamlink_process
        logger.info(f"UTA Restream Service: Streamlink process started (PID: {config_manager.UTA_STREAMLINK_PID})")

        logger.info("UTA Restream Service: Starting FFmpeg process...")
        current_ffmpeg_process = subprocess.Popen(
            ffmpeg_command,
            stdin=current_streamlink_process.stdout,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            startupinfo=startupinfo
        )
        config_manager.UTA_FFMPEG_PID = current_ffmpeg_process.pid
        _ffmpeg_process = current_ffmpeg_process
        logger.info(f"UTA Restream Service: FFmpeg process started (PID: {config_manager.UTA_FFMPEG_PID})")

        if current_streamlink_process.stdout:
            current_streamlink_process.stdout.close()

        config_manager.uta_is_restreaming_active = True
        logger.info(f"UTA Restream Service: Restreaming for {username} is now active. Monitoring FFmpeg/Streamlink processes...")
        config_manager.logger.info("UTA_GUI_LOG: RestreamPipeStatus=Active_Connecting")


        if config_manager.UTA_YOUTUBE_PLAYABILITY_CHECK_ENABLED and \
           config_manager.effective_youtube_api_enabled() and config_manager.UTA_CURRENT_YT_VIDEO_ID_FOR_LOGGING:

            if shutdown_event.wait(timeout=config_manager.UTA_FFMPEG_STARTUP_WAIT_SECONDS):
                logger.info("UTA Restream Service: Shutdown detected during FFmpeg startup wait (before playability check).")
                return False

            playability_future = asyncio.run_coroutine_threadsafe(
                _check_youtube_playability(config_manager.UTA_CURRENT_YT_VIDEO_ID_FOR_LOGGING, bot_loop), bot_loop
            )
            try:
                overall_timeout = (config_manager.UTA_YOUTUBE_PLAYABILITY_CHECK_RETRIES *
                                   (config_manager.UTA_YOUTUBE_PLAYABILITY_CHECK_DELAY_SECONDS + 5)) + 10
                is_playable = playability_future.result(timeout=overall_timeout)
                if not is_playable:
                    logger.error(f"UTA Restream Service: YouTube stream {config_manager.UTA_CURRENT_YT_VIDEO_ID_FOR_LOGGING} reported as not playable after FFmpeg start. Terminating current pipe attempt.")
                    return False
            except asyncio.TimeoutError:
                logger.error(f"UTA Restream Service: YouTube playability check for {config_manager.UTA_CURRENT_YT_VIDEO_ID_FOR_LOGGING} timed out overall.")
                config_manager.UTA_LAST_PLAYABILITY_CHECK_STATUS = f"Timeout for {config_manager.UTA_CURRENT_YT_VIDEO_ID_FOR_LOGGING}"
                config_manager.logger.info(f"UTA_GUI_LOG: PlayabilityCheckStatus={config_manager.UTA_LAST_PLAYABILITY_CHECK_STATUS}")
                return False
            except Exception as e_play_check:
                logger.error(f"UTA Restream Service: Error during YouTube playability check orchestration: {e_play_check}", exc_info=True)
                config_manager.UTA_LAST_PLAYABILITY_CHECK_STATUS = f"Error for {config_manager.UTA_CURRENT_YT_VIDEO_ID_FOR_LOGGING}"
                config_manager.logger.info(f"UTA_GUI_LOG: PlayabilityCheckStatus={config_manager.UTA_LAST_PLAYABILITY_CHECK_STATUS}")
                return False

        stream_pipe_successfully_started = True
        config_manager.logger.info("UTA_GUI_LOG: RestreamPipeStatus=Active_Streaming")

        ffmpeg_stderr_output_full = ""
        if current_ffmpeg_process.stderr:
            for line_bytes in iter(current_ffmpeg_process.stderr.readline, b''):
                if shutdown_event.is_set():
                    logger.info("UTA Restream Service: Shutdown signal received, stopping FFmpeg log reading.")
                    break
                decoded_line = line_bytes.decode('utf-8', errors='ignore').strip()
                logger.debug(f"UTA_FFMPEG_LOG: {decoded_line}")
                ffmpeg_stderr_output_full += decoded_line + "\n"

                if current_streamlink_process and current_streamlink_process.poll() is not None:
                    logger.warning(f"UTA Restream Service: Streamlink (PID: {config_manager.UTA_STREAMLINK_PID}) exited unexpectedly (Code: {current_streamlink_process.returncode}) while FFmpeg was running.")
                    stream_pipe_successfully_started = False
                    break
                if current_ffmpeg_process.poll() is not None:
                    break
            if not current_ffmpeg_process.stderr.closed:
                current_ffmpeg_process.stderr.close()

        current_ffmpeg_process.wait()
        ffmpeg_exit_code = current_ffmpeg_process.poll()
        logger.info(f"UTA Restream Service: FFmpeg (PID: {config_manager.UTA_FFMPEG_PID if config_manager.UTA_FFMPEG_PID else 'N/A'}) exited with code: {ffmpeg_exit_code}")
        if ffmpeg_exit_code != 0 and ffmpeg_exit_code is not None:
            stream_pipe_successfully_started = False
            logger.error("UTA Restream Service: --- FFmpeg Error Log (Last 20 lines) ---")
            for err_line in ffmpeg_stderr_output_full.strip().splitlines()[-20:]:
                if err_line.strip(): logger.error(err_line)
            logger.error("UTA Restream Service: --- End FFmpeg Error Log ---")

        if current_streamlink_process:
            streamlink_exit_code = current_streamlink_process.poll()
            if streamlink_exit_code is None:
                logger.warning("UTA Restream Service: FFmpeg exited, but Streamlink is still running. Terminating Streamlink...")
                _terminate_process(current_streamlink_process, "Streamlink (post-ffmpeg cleanup)")
                streamlink_exit_code = current_streamlink_process.poll()

            logger.info(f"UTA Restream Service: Streamlink (PID: {config_manager.UTA_STREAMLINK_PID if config_manager.UTA_STREAMLINK_PID else 'N/A'}) exited with code: {streamlink_exit_code}")
            if streamlink_exit_code != 0 and streamlink_exit_code is not None :
                stream_pipe_successfully_started = False

            if current_streamlink_process.stderr:
                try:
                    sl_stderr_output_bytes = current_streamlink_process.stderr.read()
                finally:
                    if not current_streamlink_process.stderr.closed:
                        current_streamlink_process.stderr.close()
                if sl_stderr_output_bytes:
                    logger.info(f"UTA Restream Service: --- Streamlink Stderr Log ---\n{sl_stderr_output_bytes.decode('utf-8', errors='ignore').strip()}\n--- End Streamlink Stderr Log ---")

        return stream_pipe_successfully_started

    except FileNotFoundError as e:
        logger.critical(f"UTA Restream Service: ERROR - Command not found (Streamlink or FFmpeg). Ensure paths are correct in config and executables are installed: {e}.")
        return False
    except Exception as e:
        logger.error(f"UTA Restream Service: Critical error during restreaming setup or monitoring: {e}", exc_info=True)
        return False
    finally:
        if not shutdown_event.is_set():
            temp_slp_to_clear, temp_ffp_to_clear = _streamlink_process, _ffmpeg_process
            if current_streamlink_process == temp_slp_to_clear: _streamlink_process = None
            if current_ffmpeg_process == temp_ffp_to_clear: _ffmpeg_process = None

            _terminate_process(current_ffmpeg_process, "FFmpeg (pipe_start finally)")
            _terminate_process(current_streamlink_process, "Streamlink (pipe_start finally)")

        config_manager.uta_is_restreaming_active = False
        config_manager.logger.info("UTA_GUI_LOG: RestreamPipeStatus=Inactive_EndedAttempt")


def restreamer_monitor_loop(bot_instance): # bot_instance is passed here for bot.loop
    logger.info(f"UTA Restreamer Service thread ({threading.current_thread().name}) started.")

    _twitch_session_active_local = False
    _youtube_api_session_active_local = False
    _twitch_session_start_time_utc = None
    _twitch_session_stream_data = None

    yt_service_instance = None
    if config_manager.effective_youtube_api_enabled():
        yt_service_instance = get_youtube_service()
        if not yt_service_instance:
             logger.warning("UTA Restreamer: Failed to initialize YouTube service on thread start. API mode features will be impaired until successful initialization.")

    while not shutdown_event.is_set():
        config_manager.twitch_session_active_global = _twitch_session_active_local
        config_manager.youtube_api_session_active_global = _youtube_api_session_active_local

        try:
            if not config_manager.UTA_TWITCH_CHANNEL_NAME:
                logger.warning("UTA Restreamer: UTA_TWITCH_CHANNEL_NAME not set in config. Skipping cycle.")
                if shutdown_event.wait(timeout=config_manager.UTA_CHECK_INTERVAL_SECONDS_RESTREAMER): break
                continue

            stream_api_data = make_uta_twitch_api_request("streams", params={"user_login": config_manager.UTA_TWITCH_CHANNEL_NAME})
            is_twitch_live_now = False
            current_twitch_stream_data_from_api = None
            if stream_api_data and stream_api_data.get("data") and len(stream_api_data["data"]) > 0 and stream_api_data["data"][0].get("type") == "live":
                is_twitch_live_now = True
                current_twitch_stream_data_from_api = stream_api_data["data"][0]

            now_utc = datetime.now(timezone.utc)

            manual_ffmpeg_restart_triggered_this_cycle = False
            if config_manager.UTA_MANUAL_FFMPEG_RESTART_REQUESTED and _twitch_session_active_local and is_twitch_live_now:
                logger.info("UTA Restreamer: Manual FFmpeg/Streamlink restart triggered by command.")
                if config_manager.uta_is_restreaming_active: cleanup_restream_processes()
                config_manager.UTA_MANUAL_FFMPEG_RESTART_REQUESTED = False
                manual_ffmpeg_restart_triggered_this_cycle = True
                config_manager.UTA_RESTREAM_CONSECUTIVE_FAILURES = 0
                config_manager.logger.info(f"UTA_GUI_LOG: ConsecutiveFailures={config_manager.UTA_RESTREAM_CONSECUTIVE_FAILURES}")


            manual_new_part_triggered_this_cycle = False
            if config_manager.UTA_MANUAL_NEW_PART_REQUESTED and _twitch_session_active_local and is_twitch_live_now and \
               config_manager.effective_youtube_api_enabled() and _youtube_api_session_active_local:
                logger.info("UTA Restreamer: Manual new YouTube part triggered by command.")
                config_manager.UTA_MANUAL_NEW_PART_REQUESTED = False
                manual_new_part_triggered_this_cycle = True

            if is_twitch_live_now and not _twitch_session_active_local:
                logger.info(f"UTA Restreamer: Twitch channel {config_manager.UTA_TWITCH_CHANNEL_NAME} is LIVE! Preparing restream session...")
                _twitch_session_active_local = True
                _twitch_session_start_time_utc = now_utc
                _twitch_session_stream_data = current_twitch_stream_data_from_api
                config_manager.last_known_title_for_ended_part = _twitch_session_stream_data.get("title","N/A") # For initial part
                config_manager.last_known_game_for_ended_part = _twitch_session_stream_data.get("game_name","N/A") # For initial part


                config_manager.uta_current_restream_part_number = 1
                config_manager.UTA_RESTREAM_CONSECUTIVE_FAILURES = 0
                config_manager.UTA_CURRENT_YT_VIDEO_ID_FOR_LOGGING = None
                _youtube_api_session_active_local = False
                config_manager.UTA_LAST_PLAYABILITY_CHECK_STATUS = "N/A"
                config_manager.logger.info(f"UTA_GUI_LOG: PlayabilityCheckStatus={config_manager.UTA_LAST_PLAYABILITY_CHECK_STATUS}")
                config_manager.UTA_FFMPEG_PID, config_manager.UTA_STREAMLINK_PID, config_manager.UTA_PIPE_START_TIME_UTC = None, None, None # Reset pipe start time
                config_manager.logger.info("UTA_GUI_LOG: RestreamPipeStatus=Inactive")
                config_manager.logger.info(f"UTA_GUI_LOG: ConsecutiveFailures=0")
                config_manager.logger.info("UTA_GUI_LOG: CooldownStatus=Inactive")

                if config_manager.effective_youtube_api_enabled():
                    if not yt_service_instance: yt_service_instance = get_youtube_service()

                    if yt_service_instance:
                        future_ls = asyncio.run_coroutine_threadsafe(
                            create_youtube_live_stream_resource(yt_service_instance, config_manager.UTA_TWITCH_CHANNEL_NAME),
                            bot_instance.loop
                        )
                        try:
                            new_ls_id, new_rtmp, new_key = future_ls.result(timeout=30)
                            if new_ls_id and new_rtmp and new_key:
                                config_manager.uta_current_youtube_live_stream_id = new_ls_id
                                config_manager.uta_current_youtube_rtmp_url = new_rtmp
                                config_manager.uta_current_youtube_stream_key = new_key

                                title = config_manager.UTA_YOUTUBE_DYNAMIC_TITLE_TEMPLATE.format(
                                    twitch_username=config_manager.UTA_TWITCH_CHANNEL_NAME,
                                    twitch_title=_twitch_session_stream_data.get("title","N/A"),
                                    game_name=_twitch_session_stream_data.get("game_name","N/A"),
                                    part_num=config_manager.uta_current_restream_part_number,
                                    date=now_utc.strftime("%Y-%m-%d"),
                                    time=now_utc.strftime("%H:%M:%S UTC")
                                )
                                description = _generate_enhanced_youtube_description(
                                    twitch_username=config_manager.UTA_TWITCH_CHANNEL_NAME,
                                    twitch_title=_twitch_session_stream_data.get("title","N/A"),
                                    current_game_name=_twitch_session_stream_data.get("game_name","N/A"),
                                    part_num=config_manager.uta_current_restream_part_number,
                                    vod_part_start_utc=now_utc # Start of this new part
                                )

                                privacy = config_manager.UTA_YOUTUBE_DEFAULT_PRIVACY
                                
                                start_iso = now_utc.isoformat()

                                future_bcast = asyncio.run_coroutine_threadsafe(
                                    create_youtube_broadcast(yt_service_instance, config_manager.uta_current_youtube_live_stream_id, title, description, privacy, start_iso),
                                    bot_instance.loop
                                )
                                new_bcast_id = future_bcast.result(timeout=30)

                                if new_bcast_id:
                                    config_manager.UTA_CURRENT_YT_BROADCAST_ID_FOR_LOGGING = new_bcast_id
                                    config_manager.UTA_CURRENT_YT_VIDEO_ID_FOR_LOGGING = new_bcast_id
                                    _youtube_api_session_active_local = True
                                    logger.info(f"UTA YouTube: Successfully created broadcast {config_manager.UTA_CURRENT_YT_BROADCAST_ID_FOR_LOGGING} (Video ID: {config_manager.UTA_CURRENT_YT_VIDEO_ID_FOR_LOGGING}) for Part {config_manager.uta_current_restream_part_number}. Watch: https://www.youtube.com/watch?v={config_manager.UTA_CURRENT_YT_VIDEO_ID_FOR_LOGGING}")
                                    if config_manager.UTA_CURRENT_YT_VIDEO_ID_FOR_LOGGING:
                                        config_manager.logger.info(f"UTA_GUI_LOG: YouTubeVideoID={config_manager.UTA_CURRENT_YT_VIDEO_ID_FOR_LOGGING}")
                                        config_manager.logger.info(f"UTA_GUI_LOG: YouTubePartNum={config_manager.uta_current_restream_part_number}")

                                    if config_manager.UTA_YOUTUBE_PLAYLIST_ID:
                                        asyncio.run_coroutine_threadsafe(
                                            add_video_to_youtube_playlist(yt_service_instance, config_manager.UTA_CURRENT_YT_VIDEO_ID_FOR_LOGGING, config_manager.UTA_YOUTUBE_PLAYLIST_ID),
                                            bot_instance.loop
                                        )

                                    if config_manager.UTA_YOUTUBE_SCHEDULED_ROLLOVER_HOURS > 0:
                                        config_manager.uta_youtube_next_rollover_time_utc = now_utc + timedelta(hours=config_manager.UTA_YOUTUBE_SCHEDULED_ROLLOVER_HOURS)
                                    else:
                                        config_manager.uta_youtube_next_rollover_time_utc = None
                                else:
                                    logger.error("UTA YouTube: Failed to create broadcast. Will fallback to legacy RTMP if configured, or fail.")
                                    config_manager.UTA_CURRENT_YT_BROADCAST_ID_FOR_LOGGING = None
                                    _youtube_api_session_active_local = False
                            else:
                                logger.error("UTA YouTube: Failed to create liveStream resource. Will fallback to legacy RTMP if configured, or fail.")
                                _youtube_api_session_active_local = False
                        except TimeoutError:
                            logger.error("UTA YouTube: Timeout creating liveStream resource or broadcast.")
                            _youtube_api_session_active_local = False
                        except Exception as e_yt_setup:
                            logger.error(f"UTA YouTube: Exception during initial YouTube setup: {e_yt_setup}", exc_info=True)
                            _youtube_api_session_active_local = False
                    else:
                         logger.error("UTA YouTube: YouTube service not available. Will fallback to legacy RTMP if configured, or fail.")
                         _youtube_api_session_active_local = False

                _send_discord_restream_status("start", config_manager.UTA_TWITCH_CHANNEL_NAME, _twitch_session_stream_data)

            if _twitch_session_active_local and is_twitch_live_now:
                time_for_youtube_rollover = manual_new_part_triggered_this_cycle
                if not time_for_youtube_rollover and \
                   config_manager.effective_youtube_api_enabled() and \
                   _youtube_api_session_active_local and \
                   config_manager.uta_youtube_next_rollover_time_utc and now_utc >= config_manager.uta_youtube_next_rollover_time_utc:
                    logger.info(f"UTA YouTube: Scheduled rollover time for broadcast {config_manager.UTA_CURRENT_YT_BROADCAST_ID_FOR_LOGGING}.")
                    time_for_youtube_rollover = True

                if time_for_youtube_rollover and yt_service_instance:
                    logger.info(f"UTA YouTube: Initiating stream rollover from Part {config_manager.uta_current_restream_part_number}.")
                    if config_manager.uta_is_restreaming_active: cleanup_restream_processes()
                    
                    # Store info about the part that's ending, for its final description update
                    config_manager.last_known_title_for_ended_part = current_twitch_stream_data_from_api.get("title","N/A") if current_twitch_stream_data_from_api else "N/A"
                    config_manager.last_known_game_for_ended_part = current_twitch_stream_data_from_api.get("game_name","N/A") if current_twitch_stream_data_from_api else "N/A"


                    # --- START Auto Chapter Generation for Rollover Part ---
                    if config_manager.UTA_YOUTUBE_AUTO_CHAPTERS_ENABLED and \
                       config_manager.UTA_CURRENT_YT_VIDEO_ID_FOR_LOGGING and \
                       config_manager.UTA_PIPE_START_TIME_UTC: # Use pipe start time of current (now ending) part

                        vod_part_start_unix_chapters_rollover = int(config_manager.UTA_PIPE_START_TIME_UTC.timestamp())
                        vod_part_end_unix_chapters_rollover = int(now_utc.timestamp()) # Current time is end of this part

                        logger.info(f"Chapter Gen (Rollover Part): VOD Part Time Window: {datetime.fromtimestamp(vod_part_start_unix_chapters_rollover, tz=timezone.utc)} to {datetime.fromtimestamp(vod_part_end_unix_chapters_rollover, tz=timezone.utc)}")

                        game_segments_for_chapters_rollover = parse_stream_activity_for_game_segments(
                            config_manager.UTA_STREAM_ACTIVITY_LOG_FILE,
                            vod_part_start_unix_chapters_rollover,
                            vod_part_end_unix_chapters_rollover
                        )
                        if game_segments_for_chapters_rollover:
                            chapter_str_rollover = generate_chapter_text(game_segments_for_chapters_rollover, vod_part_start_unix_chapters_rollover)
                            if chapter_str_rollover:
                                future_video_details_rollover = asyncio.run_coroutine_threadsafe(
                                    get_youtube_video_details(
                                        yt_service_instance,
                                        config_manager.UTA_CURRENT_YT_VIDEO_ID_FOR_LOGGING
                                    ),
                                    bot_instance.loop
                                )
                                try:
                                    video_details_resp_rollover = future_video_details_rollover.result(timeout=20)
                                    if video_details_resp_rollover:
                                        existing_desc_rollover = video_details_resp_rollover['snippet']['description'] if 'snippet' in video_details_resp_rollover and 'description' in video_details_resp_rollover['snippet'] else ""
                                        chapter_marker = config_manager.UTA_YOUTUBE_DESCRIPTION_CHAPTER_MARKER

                                        # Preserve content before the chapter marker if it exists
                                        if chapter_marker in existing_desc_rollover:
                                            base_description_content = existing_desc_rollover.split(chapter_marker, 1)[0].rstrip()
                                        else: # No marker, use the whole existing description as base
                                            base_description_content = existing_desc_rollover.rstrip()

                                        # Regenerate enhanced part with game list (for the part just ended)
                                        enhanced_desc_for_ended_part = _generate_enhanced_youtube_description(
                                            twitch_username=config_manager.UTA_TWITCH_CHANNEL_NAME,
                                            twitch_title=config_manager.last_known_title_for_ended_part,
                                            current_game_name=config_manager.last_known_game_for_ended_part,
                                            part_num=config_manager.uta_current_restream_part_number, # Part number that just ended
                                            vod_part_start_utc=datetime.fromtimestamp(vod_part_start_unix_chapters_rollover, tz=timezone.utc),
                                            vod_part_end_utc=datetime.fromtimestamp(vod_part_end_unix_chapters_rollover, tz=timezone.utc)
                                        )
                                        final_description_rollover = f"{enhanced_desc_for_ended_part}\n\n{chapter_marker}\n{chapter_str_rollover}"

                                        logger.info(f"Chapter Gen (Rollover Part): Updating YouTube video {config_manager.UTA_CURRENT_YT_VIDEO_ID_FOR_LOGGING} with description and chapters.")
                                        
                                        future_update_rollover = asyncio.run_coroutine_threadsafe(
                                            update_youtube_broadcast_metadata(
                                                yt_service_instance,
                                                config_manager.UTA_CURRENT_YT_VIDEO_ID_FOR_LOGGING,
                                                new_description=final_description_rollover
                                            ),
                                            bot_instance.loop
                                        )
                                        future_update_rollover.result(timeout=20)
                                    else:
                                        logger.warning(f"Chapter Gen (Rollover Part): Could not retrieve video details for {config_manager.UTA_CURRENT_YT_VIDEO_ID_FOR_LOGGING}.")
                                except TimeoutError:
                                    logger.error(f"Chapter Gen (Rollover Part): Timeout getting video details or updating metadata for {config_manager.UTA_CURRENT_YT_VIDEO_ID_FOR_LOGGING}.")
                                except Exception as e_chap_api_roll:
                                    logger.error(f"Chapter Gen (Rollover Part): Error during YouTube API call for chapters: {e_chap_api_roll}", exc_info=True)
                            else: logger.info("Chapter Gen (Rollover Part): No valid chapter string generated.")
                        else: logger.info("Chapter Gen (Rollover Part): No game segments for this part.")
                    # --- END Auto Chapter Generation for Rollover Part ---

                    asyncio.run_coroutine_threadsafe(transition_youtube_broadcast(yt_service_instance, config_manager.UTA_CURRENT_YT_BROADCAST_ID_FOR_LOGGING, "complete"), bot_instance.loop).result(timeout=30)
                    if config_manager.UTA_YOUTUBE_MAKE_PUBLIC_AFTER_STREAM and config_manager.UTA_YOUTUBE_DEFAULT_PRIVACY != "public":
                        asyncio.run_coroutine_threadsafe(set_youtube_video_privacy(yt_service_instance, config_manager.UTA_CURRENT_YT_VIDEO_ID_FOR_LOGGING, "public"), bot_instance.loop)

                    config_manager.logger.info(f"UTA_GUI_LOG: YouTubeVideoID=N/A")

                    config_manager.uta_current_restream_part_number += 1
                    config_manager.UTA_RESTREAM_CONSECUTIVE_FAILURES = 0
                    config_manager.logger.info(f"UTA_GUI_LOG: ConsecutiveFailures=0")
                    logger.info(f"UTA YouTube: Preparing for Part {config_manager.uta_current_restream_part_number}.")
                    config_manager.UTA_PIPE_START_TIME_UTC = None # Clear before new part pipe starts

                    future_ls_r = asyncio.run_coroutine_threadsafe(create_youtube_live_stream_resource(yt_service_instance, config_manager.UTA_TWITCH_CHANNEL_NAME), bot_instance.loop)
                    try:
                        new_ls_id_r, new_rtmp_r, new_key_r = future_ls_r.result(timeout=30)
                        if new_ls_id_r and new_rtmp_r and new_key_r:
                            config_manager.uta_current_youtube_live_stream_id, config_manager.uta_current_youtube_rtmp_url, config_manager.uta_current_youtube_stream_key = new_ls_id_r, new_rtmp_r, new_key_r
                            title_r = config_manager.UTA_YOUTUBE_DYNAMIC_TITLE_TEMPLATE.format(
                                twitch_username=config_manager.UTA_TWITCH_CHANNEL_NAME,
                                twitch_title=current_twitch_stream_data_from_api.get("title","N/A"),
                                game_name=current_twitch_stream_data_from_api.get("game_name","N/A"),
                                part_num=config_manager.uta_current_restream_part_number,
                                date=now_utc.strftime("%Y-%m-%d"),
                                time=now_utc.strftime("%H:%M:%S UTC")
                            )
                            desc_r = _generate_enhanced_youtube_description(
                                twitch_username=config_manager.UTA_TWITCH_CHANNEL_NAME,
                                twitch_title=current_twitch_stream_data_from_api.get("title","N/A"),
                                current_game_name=current_twitch_stream_data_from_api.get("game_name","N/A"),
                                part_num=config_manager.uta_current_restream_part_number,
                                vod_part_start_utc=now_utc # For this new part
                            )
                            future_bcast_r = asyncio.run_coroutine_threadsafe(create_youtube_broadcast(yt_service_instance, config_manager.uta_current_youtube_live_stream_id, title_r, desc_r, config_manager.UTA_YOUTUBE_DEFAULT_PRIVACY, now_utc.isoformat()), bot_instance.loop)
                            new_bcast_id_r = future_bcast_r.result(timeout=30)

                            if new_bcast_id_r:
                                config_manager.UTA_CURRENT_YT_BROADCAST_ID_FOR_LOGGING, config_manager.UTA_CURRENT_YT_VIDEO_ID_FOR_LOGGING = new_bcast_id_r, new_bcast_id_r
                                _youtube_api_session_active_local = True
                                logger.info(f"UTA YouTube: Rollover successful. New broadcast {config_manager.UTA_CURRENT_YT_BROADCAST_ID_FOR_LOGGING} (Video ID: {config_manager.UTA_CURRENT_YT_VIDEO_ID_FOR_LOGGING}) for Part {config_manager.uta_current_restream_part_number}. Watch: https://www.youtube.com/watch?v={config_manager.UTA_CURRENT_YT_VIDEO_ID_FOR_LOGGING}")
                                if config_manager.UTA_CURRENT_YT_VIDEO_ID_FOR_LOGGING:
                                    config_manager.logger.info(f"UTA_GUI_LOG: YouTubeVideoID={config_manager.UTA_CURRENT_YT_VIDEO_ID_FOR_LOGGING}")
                                    config_manager.logger.info(f"UTA_GUI_LOG: YouTubePartNum={config_manager.uta_current_restream_part_number}")

                                if config_manager.UTA_YOUTUBE_PLAYLIST_ID:
                                    asyncio.run_coroutine_threadsafe(add_video_to_youtube_playlist(yt_service_instance, config_manager.UTA_CURRENT_YT_VIDEO_ID_FOR_LOGGING, config_manager.UTA_YOUTUBE_PLAYLIST_ID), bot_instance.loop)
                                if config_manager.UTA_YOUTUBE_SCHEDULED_ROLLOVER_HOURS > 0:
                                    config_manager.uta_youtube_next_rollover_time_utc = now_utc + timedelta(hours=config_manager.UTA_YOUTUBE_SCHEDULED_ROLLOVER_HOURS)
                            else:
                                logger.error("UTA YouTube: Rollover failed to create new broadcast. Aborting restream session.")
                                _twitch_session_active_local=False; _youtube_api_session_active_local=False; config_manager.UTA_CURRENT_YT_BROADCAST_ID_FOR_LOGGING=None
                        else:
                            logger.error("UTA YouTube: Rollover failed to create new liveStream resource. Aborting restream session.")
                            _twitch_session_active_local=False; _youtube_api_session_active_local=False; config_manager.UTA_CURRENT_YT_BROADCAST_ID_FOR_LOGGING=None
                    except TimeoutError:
                        logger.error("UTA YouTube: Timeout during rollover YouTube setup.")
                        _twitch_session_active_local=False; _youtube_api_session_active_local=False; config_manager.UTA_CURRENT_YT_BROADCAST_ID_FOR_LOGGING=None
                    except Exception as e_yt_rollover:
                        logger.error(f"UTA YouTube: Exception during rollover YouTube setup: {e_yt_rollover}", exc_info=True)
                        _twitch_session_active_local=False; _youtube_api_session_active_local=False; config_manager.UTA_CURRENT_YT_BROADCAST_ID_FOR_LOGGING=None


                rtmp_url_to_use, stream_key_to_use = None, None
                can_start_pipe = False
                if config_manager.effective_youtube_api_enabled() and _youtube_api_session_active_local:
                    rtmp_url_to_use = config_manager.uta_current_youtube_rtmp_url
                    stream_key_to_use = config_manager.uta_current_youtube_stream_key
                    can_start_pipe = bool(rtmp_url_to_use and stream_key_to_use)
                elif not config_manager.UTA_YOUTUBE_API_ENABLED:
                    rtmp_url_to_use = config_manager.UTA_YOUTUBE_RTMP_URL_BASE
                    stream_key_to_use = config_manager.UTA_YOUTUBE_STREAM_KEY
                    can_start_pipe = bool(rtmp_url_to_use and stream_key_to_use and "YOUR_YOUTUBE_STREAM_KEY" not in stream_key_to_use)

                if not config_manager.uta_is_restreaming_active and can_start_pipe:
                    if not manual_ffmpeg_restart_triggered_this_cycle and config_manager.UTA_RESTREAM_CONSECUTIVE_FAILURES >= config_manager.UTA_RESTREAM_MAX_CONSECUTIVE_FAILURES :
                        logger.critical(f"UTA Restreamer: Max consecutive pipe failures ({config_manager.UTA_RESTREAM_CONSECUTIVE_FAILURES}) reached. Entering long cooldown: {config_manager.UTA_RESTREAM_LONG_COOLDOWN_SECONDS}s.")
                        _send_discord_restream_status("stop", config_manager.UTA_TWITCH_CHANNEL_NAME, stream_duration_seconds=0)
                        config_manager.logger.info(f"UTA_GUI_LOG: ConsecutiveFailures={config_manager.UTA_RESTREAM_CONSECUTIVE_FAILURES}")
                        config_manager.logger.info(f"UTA_GUI_LOG: CooldownStatus=LongCooldownActive_{config_manager.UTA_RESTREAM_LONG_COOLDOWN_SECONDS}s")
                        if shutdown_event.wait(timeout=config_manager.UTA_RESTREAM_LONG_COOLDOWN_SECONDS): break
                        config_manager.logger.info("UTA_GUI_LOG: CooldownStatus=Inactive")
                        config_manager.UTA_RESTREAM_CONSECUTIVE_FAILURES = 0
                        config_manager.logger.info(f"UTA_GUI_LOG: ConsecutiveFailures=0")
                        continue

                    if not (shutil.which(config_manager.UTA_STREAMLINK_PATH) and shutil.which(config_manager.UTA_FFMPEG_PATH)):
                        logger.error("UTA Restreamer: Streamlink or FFmpeg path is invalid or executables not found. Aborting restream session.")
                        _twitch_session_active_local = False
                        if config_manager.effective_youtube_api_enabled() and _youtube_api_session_active_local and yt_service_instance:
                            asyncio.run_coroutine_threadsafe(transition_youtube_broadcast(yt_service_instance, config_manager.UTA_CURRENT_YT_BROADCAST_ID_FOR_LOGGING, "complete"), bot_instance.loop)
                        _youtube_api_session_active_local = False; config_manager.UTA_CURRENT_YT_BROADCAST_ID_FOR_LOGGING = None
                        continue

                    part_num_log = config_manager.uta_current_restream_part_number if config_manager.effective_youtube_api_enabled() and _youtube_api_session_active_local else 'N/A (Legacy)'
                    logger.info(f"UTA Restreamer: Starting FFmpeg/Streamlink pipe for {config_manager.UTA_TWITCH_CHANNEL_NAME} (Part {part_num_log}). Attempt {config_manager.UTA_RESTREAM_CONSECUTIVE_FAILURES + 1}.")

                    pipe_success = _start_restream_pipe(config_manager.UTA_TWITCH_CHANNEL_NAME, rtmp_url_to_use, stream_key_to_use, bot_instance.loop)

                    logger.info(f"UTA Restreamer: FFmpeg/Streamlink pipe for Part {part_num_log} has ended.")
                    if pipe_success:
                        config_manager.UTA_RESTREAM_CONSECUTIVE_FAILURES = 0
                        config_manager.logger.info(f"UTA_GUI_LOG: ConsecutiveFailures=0")
                    else:
                        config_manager.UTA_RESTREAM_CONSECUTIVE_FAILURES += 1
                        logger.error(f"UTA Restreamer: Pipe attempt failed. Consecutive failures: {config_manager.UTA_RESTREAM_CONSECUTIVE_FAILURES}.")
                        config_manager.logger.info(f"UTA_GUI_LOG: ConsecutiveFailures={config_manager.UTA_RESTREAM_CONSECUTIVE_FAILURES}")
                        config_manager.logger.info("UTA_GUI_LOG: RestreamPipeStatus=FailedRetry")

                    if not manual_ffmpeg_restart_triggered_this_cycle and config_manager.UTA_RESTREAM_CONSECUTIVE_FAILURES < config_manager.UTA_RESTREAM_MAX_CONSECUTIVE_FAILURES:
                            config_manager.logger.info(f"UTA_GUI_LOG: CooldownStatus=ShortRetryCooldown_{config_manager.UTA_POST_RESTREAM_COOLDOWN_SECONDS}s")
                            if shutdown_event.wait(timeout=config_manager.UTA_POST_RESTREAM_COOLDOWN_SECONDS): break
                            config_manager.logger.info("UTA_GUI_LOG: CooldownStatus=Inactive")

                elif config_manager.uta_is_restreaming_active:
                    if not manual_ffmpeg_restart_triggered_this_cycle:
                        logger.info(f"UTA Restreamer: {config_manager.UTA_TWITCH_CHANNEL_NAME} is live. Restream pipe is active. Check interval: {config_manager.UTA_RESTREAM_CHECK_INTERVAL_WHEN_LIVE}s.")
                        config_manager.logger.info("UTA_GUI_LOG: RestreamPipeStatus=Active_Monitoring")
                        if shutdown_event.wait(timeout=config_manager.UTA_RESTREAM_CHECK_INTERVAL_WHEN_LIVE): break
                else:
                    logger.warning(f"UTA Restreamer: {config_manager.UTA_TWITCH_CHANNEL_NAME} is live, but not currently restreaming (conditions not met for pipe start). Check interval: {config_manager.UTA_CHECK_INTERVAL_SECONDS_RESTREAMER}s.")
                    config_manager.logger.info("UTA_GUI_LOG: RestreamPipeStatus=Inactive_WaitingCanStart")
                    if shutdown_event.wait(timeout=config_manager.UTA_CHECK_INTERVAL_SECONDS_RESTREAMER): break

            elif not is_twitch_live_now and _twitch_session_active_local:
                logger.info(f"UTA Restreamer: Twitch channel {config_manager.UTA_TWITCH_CHANNEL_NAME} is now OFFLINE. Ending restream session.")
                if config_manager.uta_is_restreaming_active: cleanup_restream_processes()

                current_vod_part_end_time_utc = now_utc # End time for this VOD part
                
                # Store title/game of the part that just ended for description generation
                config_manager.last_known_title_for_ended_part = _twitch_session_stream_data.get("title","N/A") if _twitch_session_stream_data else "N/A"
                config_manager.last_known_game_for_ended_part = _twitch_session_stream_data.get("game_name","N/A") if _twitch_session_stream_data else "N/A"

                if config_manager.effective_youtube_api_enabled() and _youtube_api_session_active_local and yt_service_instance:
                    # --- START Auto Chapter Generation for final part ---
                    if config_manager.UTA_YOUTUBE_AUTO_CHAPTERS_ENABLED and \
                       config_manager.UTA_CURRENT_YT_VIDEO_ID_FOR_LOGGING and \
                       config_manager.UTA_PIPE_START_TIME_UTC:

                        vod_part_start_unix_chapters = int(config_manager.UTA_PIPE_START_TIME_UTC.timestamp())
                        vod_part_end_unix_chapters = int(current_vod_part_end_time_utc.timestamp())

                        logger.info(f"Chapter Gen (Final Part): VOD Part Time Window: {datetime.fromtimestamp(vod_part_start_unix_chapters, tz=timezone.utc)} to {datetime.fromtimestamp(vod_part_end_unix_chapters, tz=timezone.utc)}")

                        # CORRECTED CALL: No await asyncio.to_thread
                        game_segments_for_chapters = parse_stream_activity_for_game_segments(
                            config_manager.UTA_STREAM_ACTIVITY_LOG_FILE,
                            vod_part_start_unix_chapters,
                            vod_part_end_unix_chapters
                        )
                        if game_segments_for_chapters:
                            chapter_str = generate_chapter_text(game_segments_for_chapters, vod_part_start_unix_chapters)
                            if chapter_str:
                                future_video_details = asyncio.run_coroutine_threadsafe(
                                    get_youtube_video_details(
                                        yt_service_instance,
                                        config_manager.UTA_CURRENT_YT_VIDEO_ID_FOR_LOGGING
                                    ),
                                    bot_instance.loop
                                )
                                try:
                                    video_details_resp = future_video_details.result(timeout=20)
                                    if video_details_resp:
                                        existing_desc = video_details_resp['snippet']['description'] if 'snippet' in video_details_resp and 'description' in video_details_resp['snippet'] else ""
                                        chapter_marker = config_manager.UTA_YOUTUBE_DESCRIPTION_CHAPTER_MARKER

                                        if chapter_marker in existing_desc:
                                            base_description_content = existing_desc.split(chapter_marker, 1)[0].rstrip()
                                        else:
                                            base_description_content = existing_desc.rstrip()
                                        
                                        enhanced_desc_for_final_part = _generate_enhanced_youtube_description(
                                            twitch_username=config_manager.UTA_TWITCH_CHANNEL_NAME,
                                            twitch_title=config_manager.last_known_title_for_ended_part,
                                            current_game_name=config_manager.last_known_game_for_ended_part,
                                            part_num=config_manager.uta_current_restream_part_number, # current part number is the final one
                                            vod_part_start_utc=datetime.fromtimestamp(vod_part_start_unix_chapters, tz=timezone.utc),
                                            vod_part_end_utc=current_vod_part_end_time_utc
                                        )
                                        
                                        # Combine: Original (if any before marker) + Enhanced Dynamic Part + Chapter Marker + Chapters
                                        final_description = f"{enhanced_desc_for_final_part}\n\n{chapter_marker}\n{chapter_str}"
                                        
                                        logger.info(f"Chapter Gen (Final Part): Updating YouTube video {config_manager.UTA_CURRENT_YT_VIDEO_ID_FOR_LOGGING} with description and chapters.")
                                        future_update = asyncio.run_coroutine_threadsafe(
                                            update_youtube_broadcast_metadata(
                                                yt_service_instance,
                                                config_manager.UTA_CURRENT_YT_VIDEO_ID_FOR_LOGGING,
                                                new_description=final_description
                                            ),
                                            bot_instance.loop
                                        )
                                        future_update.result(timeout=20)
                                    else:
                                        logger.warning(f"Chapter Gen (Final Part): Could not retrieve video details for {config_manager.UTA_CURRENT_YT_VIDEO_ID_FOR_LOGGING}.")
                                except TimeoutError:
                                    logger.error(f"Chapter Gen (Final Part): Timeout getting video details or updating metadata for {config_manager.UTA_CURRENT_YT_VIDEO_ID_FOR_LOGGING}.")
                                except Exception as e_chap_api:
                                    logger.error(f"Chapter Gen (Final Part): Error during YouTube API call for chapters: {e_chap_api}", exc_info=True)
                            else: logger.info("Chapter Gen (Final Part): No valid chapter string generated.")
                        else: logger.info("Chapter Gen (Final Part): No game segments for this part.")
                    # --- END Auto Chapter Generation ---

                    logger.info(f"UTA YouTube: Finalizing YouTube broadcast {config_manager.UTA_CURRENT_YT_BROADCAST_ID_FOR_LOGGING}.")
                    asyncio.run_coroutine_threadsafe(transition_youtube_broadcast(yt_service_instance, config_manager.UTA_CURRENT_YT_BROADCAST_ID_FOR_LOGGING, "complete"), bot_instance.loop).result(timeout=30)
                    if config_manager.UTA_YOUTUBE_MAKE_PUBLIC_AFTER_STREAM and config_manager.UTA_YOUTUBE_DEFAULT_PRIVACY!="public":
                        asyncio.run_coroutine_threadsafe(set_youtube_video_privacy(yt_service_instance, config_manager.UTA_CURRENT_YT_VIDEO_ID_FOR_LOGGING, "public"), bot_instance.loop)

                config_manager.UTA_PIPE_START_TIME_UTC = None # Clear pipe start time after VOD part processing

                config_manager.UTA_CURRENT_YT_BROADCAST_ID_FOR_LOGGING=None; config_manager.UTA_CURRENT_YT_VIDEO_ID_FOR_LOGGING=None;
                config_manager.uta_current_youtube_live_stream_id=None; config_manager.uta_current_youtube_rtmp_url=None; config_manager.uta_current_youtube_stream_key=None
                config_manager.uta_youtube_next_rollover_time_utc=None
                _youtube_api_session_active_local = False
                config_manager.logger.info(f"UTA_GUI_LOG: YouTubeVideoID=N/A")


                overall_session_duration_sec = (now_utc - _twitch_session_start_time_utc).total_seconds() if _twitch_session_start_time_utc else 0
                if overall_session_duration_sec > 15 and _twitch_session_start_time_utc and bot_instance.loop:
                    asyncio.run_coroutine_threadsafe(log_stream_duration_binary(int(_twitch_session_start_time_utc.timestamp()), int(now_utc.timestamp())), bot_instance.loop)

                _send_discord_restream_status("stop", config_manager.UTA_TWITCH_CHANNEL_NAME, stream_duration_seconds=overall_session_duration_sec)

                _twitch_session_active_local = False
                _twitch_session_start_time_utc = None
                _twitch_session_stream_data = None
                config_manager.UTA_RESTREAM_CONSECUTIVE_FAILURES = 0
                config_manager.last_known_title_for_ended_part = None # Reset
                config_manager.last_known_game_for_ended_part = None  # Reset

                config_manager.logger.info(f"UTA_GUI_LOG: ConsecutiveFailures=0")
                config_manager.logger.info("UTA_GUI_LOG: CooldownStatus=Inactive")
                config_manager.logger.info("UTA_GUI_LOG: PlayabilityCheckStatus=N/A")
                config_manager.logger.info("UTA_GUI_LOG: RestreamPipeStatus=Inactive_SessionEnded")

                logger.info(f"UTA Restreamer: Post-session cooldown for {config_manager.UTA_POST_RESTREAM_COOLDOWN_SECONDS}s.")
                if shutdown_event.wait(timeout=config_manager.UTA_POST_RESTREAM_COOLDOWN_SECONDS): break

            elif not is_twitch_live_now and not _twitch_session_active_local:
                config_manager.UTA_RESTREAM_CONSECUTIVE_FAILURES = 0
                config_manager.UTA_CURRENT_YT_VIDEO_ID_FOR_LOGGING = None
                _youtube_api_session_active_local = False
                config_manager.logger.info(f"UTA_GUI_LOG: ConsecutiveFailures=0")
                config_manager.logger.info("UTA_GUI_LOG: CooldownStatus=Inactive")
                config_manager.logger.info("UTA_GUI_LOG: PlayabilityCheckStatus=N/A")
                config_manager.logger.info("UTA_GUI_LOG: RestreamPipeStatus=Inactive_Offline")
                config_manager.logger.info(f"UTA_GUI_LOG: YouTubeVideoID=N/A")

                logger.debug(f"UTA Restreamer: {config_manager.UTA_TWITCH_CHANNEL_NAME} is offline. Waiting {config_manager.UTA_CHECK_INTERVAL_SECONDS_RESTREAMER}s for next check...")
                if shutdown_event.wait(timeout=config_manager.UTA_CHECK_INTERVAL_SECONDS_RESTREAMER): break

            if shutdown_event.is_set(): break

        except Exception as e:
            logger.error(f"UTA Restreamer Service: Unexpected error in monitor loop: {e}", exc_info=True)
            cleanup_restream_processes()

            if config_manager.effective_youtube_api_enabled() and _youtube_api_session_active_local and config_manager.UTA_CURRENT_YT_BROADCAST_ID_FOR_LOGGING and yt_service_instance:
                logger.error(f"UTA YouTube: Attempting to finalize YouTube broadcast {config_manager.UTA_CURRENT_YT_BROADCAST_ID_FOR_LOGGING} due to an error in restreamer loop.")
                try:
                    asyncio.run_coroutine_threadsafe(transition_youtube_broadcast(yt_service_instance, config_manager.UTA_CURRENT_YT_BROADCAST_ID_FOR_LOGGING, "complete"), bot_instance.loop).result(timeout=30)
                except Exception as yt_err_cleanup:
                    logger.error(f"UTA YouTube: Failed to finalize broadcast during error handling: {yt_err_cleanup}")

            _twitch_session_active_local = False; _twitch_session_start_time_utc = None; _twitch_session_stream_data = None
            _youtube_api_session_active_local=False; config_manager.UTA_CURRENT_YT_BROADCAST_ID_FOR_LOGGING=None; config_manager.UTA_CURRENT_YT_VIDEO_ID_FOR_LOGGING=None
            config_manager.uta_current_youtube_live_stream_id=None; config_manager.uta_current_youtube_rtmp_url=None; config_manager.uta_current_youtube_stream_key=None; config_manager.uta_youtube_next_rollover_time_utc=None
            config_manager.UTA_RESTREAM_CONSECUTIVE_FAILURES=0
            config_manager.last_known_title_for_ended_part = None # Reset
            config_manager.last_known_game_for_ended_part = None  # Reset
            config_manager.UTA_PIPE_START_TIME_UTC = None


            config_manager.logger.info(f"UTA_GUI_LOG: ConsecutiveFailures=0")
            config_manager.logger.info("UTA_GUI_LOG: CooldownStatus=Inactive")
            config_manager.logger.info("UTA_GUI_LOG: PlayabilityCheckStatus=N/A")
            config_manager.logger.info("UTA_GUI_LOG: RestreamPipeStatus=ErrorState")
            config_manager.logger.info(f"UTA_GUI_LOG: YouTubeVideoID=N/A")

            _send_discord_restream_status("stop", config_manager.UTA_TWITCH_CHANNEL_NAME, stream_duration_seconds=0)

            if shutdown_event.wait(timeout=60): break

    config_manager.twitch_session_active_global = False
    config_manager.youtube_api_session_active_global = False
    if bot_instance.loop and not bot_instance.loop.is_closed():
        config_manager.logger.info("UTA_GUI_LOG: RestreamPipeStatus=Stopped")

    logger.info(f"UTA Restreamer Service thread ({threading.current_thread().name}) has finished.")