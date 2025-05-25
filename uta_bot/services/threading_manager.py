import logging
import threading
import shutil
import os
import asyncio

from uta_bot import config_manager # This top-level import should be fine

# --- Remove problematic top-level imports that depend on twitch_api_handler ---
# from .twitch_api_handler import get_uta_twitch_access_token, get_uta_broadcaster_id # Keep this commented out
# from .clip_service import _uta_sent_clip_ids as clip_service_sent_ids # Defer this too
# from .youtube_api_handler import get_youtube_service # Defer if it causes issues, but often ok

logger = logging.getLogger(__name__)

shutdown_event = threading.Event()

_uta_clip_thread: threading.Thread = None
_uta_restreamer_thread: threading.Thread = None
_uta_stream_status_thread: threading.Thread = None

_are_uta_threads_active = False # Internal state for this manager

def start_all_services(bot_instance):
    global _uta_clip_thread, _uta_restreamer_thread, _uta_stream_status_thread, _are_uta_threads_active
    
    # --- Deferred imports ---
    from .twitch_api_handler import get_uta_twitch_access_token, get_uta_broadcaster_id
    from .youtube_api_handler import get_youtube_service # Import get_youtube_service here
    from .clip_service import clip_monitor_loop, _uta_sent_clip_ids as clip_service_sent_ids # Import specific loop and sent_ids
    from .status_service import stream_status_monitor_loop
    from .restream_service import restreamer_monitor_loop

    if _are_uta_threads_active:
        logger.warning("UTA ThreadingManager: Attempted to start services, but they appear to be active already. Call stop_all_services first.")
        return

    shutdown_event.clear()
    logger.info("UTA ThreadingManager: Cleared shutdown event, preparing to start service threads.")

    config_manager.uta_broadcaster_id_cache = None
    
    # Access and clear _uta_sent_clip_ids from the imported clip_service module
    clip_service_sent_ids.clear()
    logger.info("UTA ThreadingManager: Cleared sent clip IDs cache in clip_service.")

    if config_manager.UTA_ENABLED and config_manager.UTA_TWITCH_CHANNEL_NAME:
        if not get_uta_twitch_access_token():
            logger.critical("UTA ThreadingManager: Failed to get/refresh Twitch token for UTA services. Functionality will be impaired.")
        
        get_uta_broadcaster_id(config_manager.UTA_TWITCH_CHANNEL_NAME)

    if config_manager.UTA_ENABLED and config_manager.UTA_RESTREAMER_ENABLED and \
       config_manager.effective_youtube_api_enabled():
        logger.info("UTA ThreadingManager: Attempting to initialize YouTube API service for UTA...")
        # Use the get_youtube_service imported inside this function
        if not get_youtube_service(force_reinitialize=True):
             logger.warning("UTA ThreadingManager: YouTube API service failed to initialize during service startup. Restreamer (API mode) may try again or fall back.")
    elif config_manager.UTA_ENABLED and config_manager.UTA_RESTREAMER_ENABLED and \
         config_manager.UTA_YOUTUBE_API_ENABLED and not config_manager.GOOGLE_API_AVAILABLE:
        logger.error("UTA ThreadingManager: YouTube API is enabled in config, but Google libraries are not installed. YouTube API features for restreamer will be disabled.")

    if config_manager.UTA_ENABLED and config_manager.UTA_CLIP_MONITOR_ENABLED and config_manager.UTA_TWITCH_CHANNEL_NAME:
        logger.info("UTA ThreadingManager: Starting Clip Monitor service thread...")
        _uta_clip_thread = threading.Thread(target=clip_monitor_loop, args=(bot_instance,), name="UTAClipMonitorThread", daemon=True)
        _uta_clip_thread.start()
    else:
        logger.info("UTA ThreadingManager: Clip Monitor service disabled or prerequisites not met.")

    restreamer_prereqs_ok = False
    if config_manager.UTA_ENABLED and config_manager.UTA_RESTREAMER_ENABLED and config_manager.UTA_TWITCH_CHANNEL_NAME:
        if not shutil.which(config_manager.UTA_STREAMLINK_PATH):
            logger.critical(f"UTA ThreadingManager: Streamlink executable ('{config_manager.UTA_STREAMLINK_PATH}') not found. Restreamer service cannot start.")
        elif not shutil.which(config_manager.UTA_FFMPEG_PATH):
            logger.critical(f"UTA ThreadingManager: FFmpeg executable ('{config_manager.UTA_FFMPEG_PATH}') not found. Restreamer service cannot start.")
        else:
            if config_manager.effective_youtube_api_enabled():
                if not os.path.exists(config_manager.UTA_YOUTUBE_CLIENT_SECRET_FILE):
                     logger.critical(f"UTA ThreadingManager: YouTube API client secret file ('{config_manager.UTA_YOUTUBE_CLIENT_SECRET_FILE}') not found. Restreamer (API mode) cannot start.")
                else:
                    restreamer_prereqs_ok = True
                    logger.info("UTA ThreadingManager: Restreamer (YouTube API Mode) prerequisites met.")
            elif not config_manager.UTA_YOUTUBE_API_ENABLED:
                if config_manager.UTA_YOUTUBE_RTMP_URL_BASE and config_manager.UTA_YOUTUBE_STREAM_KEY and \
                   "YOUR_YOUTUBE_STREAM_KEY" not in config_manager.UTA_YOUTUBE_STREAM_KEY:
                    restreamer_prereqs_ok = True
                    logger.info("UTA ThreadingManager: Restreamer (Legacy RTMP Mode) prerequisites met.")
                else:
                    logger.warning("UTA ThreadingManager: Restreamer (Legacy RTMP Mode) selected, but YouTube RTMP URL or Stream Key is incomplete or placeholder. Restreamer cannot start.")
            else:
                 logger.warning("UTA ThreadingManager: Restreamer (YouTube API Mode) selected, but Google API libraries are missing. Restreamer cannot start in API mode.")

    if restreamer_prereqs_ok:
        logger.info("UTA ThreadingManager: Starting Restreamer Monitor service thread...")
        _uta_restreamer_thread = threading.Thread(target=restreamer_monitor_loop, args=(bot_instance,), name="UTARestreamerThread", daemon=True)
        _uta_restreamer_thread.start()
    elif config_manager.UTA_ENABLED and config_manager.UTA_RESTREAMER_ENABLED:
        logger.error("UTA ThreadingManager: Restreamer service is enabled in config, but prerequisites were not met. Service not started.")

    if config_manager.UTA_ENABLED and config_manager.UTA_STREAM_STATUS_NOTIFICATIONS_ENABLED and config_manager.UTA_TWITCH_CHANNEL_NAME:
        logger.info("UTA ThreadingManager: Starting Stream Status Monitor service thread...")
        _uta_stream_status_thread = threading.Thread(target=stream_status_monitor_loop, args=(bot_instance,), name="UTAStatusMonitorThread", daemon=True)
        _uta_stream_status_thread.start()
    else:
        logger.info("UTA ThreadingManager: Stream Status Monitor service disabled or prerequisites not met.")

    _are_uta_threads_active = True
    config_manager._are_uta_threads_active = True # Update global status in config_manager

async def stop_all_services():
    global _uta_clip_thread, _uta_restreamer_thread, _uta_stream_status_thread, _are_uta_threads_active
    # Deferred import for cleanup
    from .restream_service import cleanup_restream_processes as cleanup_restream_processes_ext

    active_threads_exist = (
        (_uta_clip_thread and _uta_clip_thread.is_alive()) or
        (_uta_restreamer_thread and _uta_restreamer_thread.is_alive()) or
        (_uta_stream_status_thread and _uta_stream_status_thread.is_alive())
    )

    if not _are_uta_threads_active and not active_threads_exist:
        logger.info("UTA ThreadingManager: No active UTA service threads to stop.")
        _are_uta_threads_active = False
        config_manager._are_uta_threads_active = False
        return

    logger.info("UTA ThreadingManager: Initiating shutdown of UTA service threads...")
    shutdown_event.set()

    threads_to_join = []
    if _uta_clip_thread and _uta_clip_thread.is_alive():
        threads_to_join.append(_uta_clip_thread)
    if _uta_restreamer_thread and _uta_restreamer_thread.is_alive():
        threads_to_join.append(_uta_restreamer_thread)
    if _uta_stream_status_thread and _uta_stream_status_thread.is_alive():
        threads_to_join.append(_uta_stream_status_thread)

    for t in threads_to_join:
        logger.info(f"UTA ThreadingManager: Attempting to join thread {t.name}...")
        try:
            await asyncio.to_thread(t.join, timeout=10)
            if t.is_alive():
                logger.warning(f"UTA ThreadingManager: Thread {t.name} did not join cleanly after 10 seconds.")
            else:
                logger.info(f"UTA ThreadingManager: Thread {t.name} joined successfully.")
        except Exception as e:
            logger.error(f"UTA ThreadingManager: Exception while joining thread {t.name}: {e}")

    if config_manager.UTA_ENABLED and config_manager.UTA_RESTREAMER_ENABLED:
        logger.info("UTA ThreadingManager: Performing final cleanup of restreamer processes (FFmpeg/Streamlink)...")
        cleanup_restream_processes_ext()

    _uta_clip_thread = None
    _uta_restreamer_thread = None
    _uta_stream_status_thread = None
    _are_uta_threads_active = False
    config_manager._are_uta_threads_active = False
    logger.info("UTA ThreadingManager: All service threads processed for stopping.")