import logging
import time
import asyncio
import threading # Added threading for current_thread access
from datetime import datetime, timedelta, timezone
import requests 

from uta_bot import config_manager
from uta_bot.services.twitch_api_handler import make_uta_twitch_api_request, get_uta_broadcaster_id 
from .threading_manager import shutdown_event 

logger = logging.getLogger(__name__)

_uta_sent_clip_ids = set() 

def _send_discord_clip_notification(clip_url: str, clip_title: str, channel_name: str):
    if not config_manager.UTA_DISCORD_WEBHOOK_URL_CLIPS or \
       "YOUR_DISCORD_WEBHOOK_URL" in config_manager.UTA_DISCORD_WEBHOOK_URL_CLIPS:
        logger.warning("UTA Clip Service: Discord webhook URL for clips is not configured or is a placeholder. Skipping notification.")
        return

    payload = {
        "content": f"🎬 New clip from **{channel_name}**!\n**{clip_title}**\n{clip_url}"
    }
    response_obj = None
    try:
        response_obj = requests.post(config_manager.UTA_DISCORD_WEBHOOK_URL_CLIPS, json=payload, timeout=10)
        response_obj.raise_for_status()
        logger.info(f"UTA Clip Service: Sent clip notification to Discord: {clip_url}")
    except requests.exceptions.RequestException as e:
        logger.error(f"UTA Clip Service: Error sending clip notification to Discord: {e}")
        if response_obj is not None and hasattr(response_obj, 'text'):
            logger.error(f"UTA Clip Service: Response content: {response_obj.text}")

def _get_recent_clips(broadcaster_id: str, lookback_minutes: int):
    if not broadcaster_id:
        logger.error("UTA Clip Service: No broadcaster ID provided for fetching clips.")
        return []

    end_time_utc = datetime.now(timezone.utc)
    start_time_utc = end_time_utc - timedelta(minutes=lookback_minutes)
    formatted_start_time = start_time_utc.isoformat().replace('+00:00', 'Z')
    
    params = {
        "broadcaster_id": broadcaster_id,
        "started_at": formatted_start_time,
        "first": 20 
    }
    
    data = make_uta_twitch_api_request("clips", params=params) 
    
    return data.get("data", []) if data else []


def clip_monitor_loop(bot_instance): 
    logger.info(f"UTA Clip Monitor Service thread ({threading.current_thread().name}) started.")
    
    if config_manager.UTA_TWITCH_CHANNEL_NAME:
        current_broadcaster_id = get_uta_broadcaster_id(config_manager.UTA_TWITCH_CHANNEL_NAME)
        
        if current_broadcaster_id:
            logger.info(f"UTA Clip Service: Initial scan for clips from the last {config_manager.UTA_CLIP_LOOKBACK_MINUTES} minutes...")
            try:
                initial_clips = _get_recent_clips(current_broadcaster_id, config_manager.UTA_CLIP_LOOKBACK_MINUTES)
                for clip in initial_clips:
                    _uta_sent_clip_ids.add(clip['id'])
                logger.info(f"UTA Clip Service: Primed {len(_uta_sent_clip_ids)} clips. Monitoring for new ones.")
            except Exception as e_init:
                logger.error(f"UTA Clip Service: Error during initial clip priming: {e_init}", exc_info=True)
        else:
            logger.error(f"UTA Clip Service: Could not fetch broadcaster ID for {config_manager.UTA_TWITCH_CHANNEL_NAME} on startup. Clip monitoring may be impaired.")
    else:
        logger.warning("UTA Clip Service: UTA_TWITCH_CHANNEL_NAME not set. Clip monitoring will not function.")

    while not shutdown_event.is_set():
        try:
            if not config_manager.UTA_TWITCH_CHANNEL_NAME:
                logger.debug("UTA Clip Service: UTA_TWITCH_CHANNEL_NAME not configured. Skipping clip check cycle.")
                if shutdown_event.wait(timeout=config_manager.UTA_CHECK_INTERVAL_SECONDS_CLIPS): break
                continue

            broadcaster_id = get_uta_broadcaster_id(config_manager.UTA_TWITCH_CHANNEL_NAME)
            if not broadcaster_id:
                logger.warning(f"UTA Clip Service: Still unable to fetch broadcaster ID for {config_manager.UTA_TWITCH_CHANNEL_NAME}. Skipping cycle.")
                if shutdown_event.wait(timeout=config_manager.UTA_CHECK_INTERVAL_SECONDS_CLIPS): break
                continue
            
            logger.debug(f"UTA Clip Service: Checking for new clips for {config_manager.UTA_TWITCH_CHANNEL_NAME} (ID: {broadcaster_id}).")
            recent_clips = _get_recent_clips(broadcaster_id, config_manager.UTA_CLIP_LOOKBACK_MINUTES)

            if not recent_clips:
                logger.debug("UTA Clip Service: No clips found in the lookback window.")
            else:
                new_clips_found_count = 0
                for clip in reversed(recent_clips): 
                    if shutdown_event.is_set(): break 
                    if clip['id'] not in _uta_sent_clip_ids:
                        logger.info(f"UTA Clip Service: New clip found: '{clip['title']}' - {clip['url']}")
                        asyncio.run_coroutine_threadsafe(
                            asyncio.to_thread(
                                _send_discord_clip_notification,
                                clip['url'], clip['title'], config_manager.UTA_TWITCH_CHANNEL_NAME
                            ),
                            bot_instance.loop
                        )
                        _uta_sent_clip_ids.add(clip['id'])
                        new_clips_found_count += 1
                        if shutdown_event.wait(timeout=1): break 
                
                if new_clips_found_count == 0 and recent_clips:
                    logger.debug("UTA Clip Service: No *new* clips found (all fetched clips were already known/sent).")
            
            if shutdown_event.is_set(): break 

            wait_interval = config_manager.UTA_CHECK_INTERVAL_SECONDS_CLIPS
            logger.debug(f"UTA Clip Service: Waiting {wait_interval // 60} min ({wait_interval}s) for the next clip check.")
            if shutdown_event.wait(timeout=wait_interval): 
                break 

        except Exception as e:
            logger.error(f"UTA Clip Service: An unexpected error occurred in the monitor loop: {e}", exc_info=True)
            if shutdown_event.wait(timeout=60): break 
            
    logger.info(f"UTA Clip Monitor Service thread ({threading.current_thread().name}) has finished.")
    _uta_sent_clip_ids.clear() 