import logging
import time
import asyncio 
import threading # For current_thread access
from datetime import datetime, timezone
import discord 
import requests 
import json # For webhook file sending with payload_json
import os # For activity log checks (though should use utils)

from uta_bot import config_manager
from uta_bot.services.twitch_api_handler import make_uta_twitch_api_request
from .threading_manager import shutdown_event 
from uta_bot.utils.data_logging import (
    log_stream_activity_binary, log_viewer_data_binary, 
    get_viewer_stats_for_period, parse_stream_activity_for_game_segments, 
    read_and_find_records_for_period
)
from uta_bot.utils.formatters import format_duration_human
from uta_bot.utils.constants import (
    EVENT_TYPE_STREAM_START, EVENT_TYPE_STREAM_END, 
    EVENT_TYPE_GAME_CHANGE, EVENT_TYPE_TITLE_CHANGE, EVENT_TYPE_TAGS_CHANGE
)
# Import YouTube API handler for metadata updates
from .youtube_api_handler import update_youtube_broadcast_metadata

logger = logging.getLogger(__name__)

async def _send_status_notification_to_discord(bot_instance, message_content: str = None, embed: discord.Embed = None, file: discord.File = None):
    sent_to_webhook, sent_to_channel = False, False

    if config_manager.UTA_STREAM_STATUS_WEBHOOK_URL and \
       "YOUR_DISCORD_WEBHOOK_URL" not in config_manager.UTA_STREAM_STATUS_WEBHOOK_URL:
        payload = {}
        if message_content: payload["content"] = message_content
        if embed: payload["embeds"] = [embed.to_dict()] 

        files_for_webhook = {}
        if file: 
            if hasattr(file.fp, 'seekable') and file.fp.seekable():
                 file.fp.seek(0)
            files_for_webhook = {'file': (file.filename, file.fp, 'image/png')} 
            
            # Use await asyncio.to_thread for the blocking requests.post call
            response = await asyncio.to_thread(
                requests.post, 
                config_manager.UTA_STREAM_STATUS_WEBHOOK_URL, 
                data={'payload_json': json.dumps(payload)}, 
                files=files_for_webhook, 
                timeout=15
            )
            if hasattr(file.fp, 'seekable') and file.fp.seekable(): 
                 file.fp.seek(0) # Reset pointer again if needed for channel send
        else:
            response = await asyncio.to_thread(
                requests.post, 
                config_manager.UTA_STREAM_STATUS_WEBHOOK_URL, 
                json=payload, 
                timeout=10
            )
        try:
            response.raise_for_status()
            logger.info(f"UTA Status Service: Sent webhook notification for {config_manager.UTA_TWITCH_CHANNEL_NAME}.")
            sent_to_webhook = True
        except requests.exceptions.RequestException as e:
            logger.error(f"UTA Status Service: Error sending webhook notification: {e}. Response: {response.text if hasattr(response, 'text') else 'N/A'}")

    if config_manager.UTA_STREAM_STATUS_CHANNEL_ID and bot_instance and not sent_to_webhook: # Fallback or primary if webhook fails/not set
        try:
            channel = bot_instance.get_channel(config_manager.UTA_STREAM_STATUS_CHANNEL_ID)
            if channel:
                await channel.send(content=message_content, embed=embed, file=file)
                logger.info(f"UTA Status Service: Sent channel message notification for {config_manager.UTA_TWITCH_CHANNEL_NAME}.")
                sent_to_channel = True
            else:
                logger.warning(f"UTA Status Service: Notification channel ID {config_manager.UTA_STREAM_STATUS_CHANNEL_ID} not found by the bot.")
        except discord.Forbidden:
            logger.error(f"UTA Status Service: Bot lacks permission to send messages in channel ID {config_manager.UTA_STREAM_STATUS_CHANNEL_ID}.")
        except discord.HTTPException as e:
            logger.error(f"UTA Status Service: Failed to send channel message due to an HTTP error: {e}")
        except Exception as e:
            logger.error(f"UTA Status Service: An unexpected error occurred sending channel message: {e}", exc_info=True)

    if not sent_to_webhook and not sent_to_channel:
        logger.debug("UTA Status Service: No webhook or channel ID configured for notifications, or both methods failed.")


def stream_status_monitor_loop(bot_instance): 
    logger.info(f"UTA Stream Status Monitor Service thread ({threading.current_thread().name}) started.")
    
    is_currently_live = False 
    last_known_game_name = None
    last_known_title = None
    last_known_tags = None 
    current_session_start_time_utc = None 
    current_session_peak_viewers = 0
    last_viewer_log_timestamp = 0 

    while not shutdown_event.is_set():
        try:
            if not config_manager.UTA_TWITCH_CHANNEL_NAME:
                logger.debug("UTA Status Service: UTA_TWITCH_CHANNEL_NAME not configured. Skipping status check.")
                if shutdown_event.wait(timeout=config_manager.UTA_STREAM_STATUS_POLL_INTERVAL_SECONDS): break
                continue

            logger.debug(f"UTA Status Service: Checking stream status for {config_manager.UTA_TWITCH_CHANNEL_NAME}...")
            
            stream_api_data = make_uta_twitch_api_request("streams", params={"user_login": config_manager.UTA_TWITCH_CHANNEL_NAME})
            
            is_twitch_live_now = False
            live_stream_data_from_api = None
            if stream_api_data and stream_api_data.get("data") and stream_api_data["data"][0].get("type") == "live":
                is_twitch_live_now = True
                live_stream_data_from_api = stream_api_data["data"][0]
            
            current_utc_time = datetime.now(timezone.utc)
            config_manager.twitch_session_active_global = is_twitch_live_now # Update global state

            if is_twitch_live_now:
                current_viewers = live_stream_data_from_api.get("viewer_count", 0)
                current_game_name = live_stream_data_from_api.get("game_name", "N/A")
                current_title = live_stream_data_from_api.get("title", "N/A")
                current_tags_from_api = live_stream_data_from_api.get("tags", []) 
                stream_started_at_str_api = live_stream_data_from_api.get("started_at") 

                if not is_currently_live: 
                    is_currently_live = True
                    if stream_started_at_str_api:
                        try:
                            current_session_start_time_utc = datetime.fromisoformat(stream_started_at_str_api.replace('Z', '+00:00'))
                            config_manager.current_twitch_session_start_ts_global = int(current_session_start_time_utc.timestamp())
                        except ValueError:
                            logger.warning(f"UTA Status Service: Could not parse Twitch's started_at time '{stream_started_at_str_api}'. Using current time for session start.")
                            current_session_start_time_utc = current_utc_time
                            config_manager.current_twitch_session_start_ts_global = int(current_session_start_time_utc.timestamp())
                    else:
                        current_session_start_time_utc = current_utc_time
                        config_manager.current_twitch_session_start_ts_global = int(current_session_start_time_utc.timestamp())
                    
                    last_known_game_name = current_game_name
                    last_known_title = current_title
                    last_known_tags = list(current_tags_from_api or []) 

                    current_session_peak_viewers = current_viewers
                    logger.info(f"UTA Status Service: {config_manager.UTA_TWITCH_CHANNEL_NAME} is LIVE. Game: {current_game_name}, Title: {current_title}, Tags: {last_known_tags}")

                    if bot_instance.loop and bot_instance.is_ready():
                        yt_video_id_for_log = config_manager.UTA_CURRENT_YT_VIDEO_ID_FOR_LOGGING \
                                              if config_manager.effective_youtube_api_enabled() and \
                                                 config_manager.youtube_api_session_active_global else None

                        asyncio.run_coroutine_threadsafe(
                            log_stream_activity_binary(
                                EVENT_TYPE_STREAM_START, current_session_start_time_utc, # Use session start for log
                                title=current_title, game=current_game_name, tags=last_known_tags,
                                youtube_video_id=yt_video_id_for_log 
                            ), bot_instance.loop
                        )
                        
                        embed = discord.Embed(
                            title=f"🔴 {config_manager.UTA_TWITCH_CHANNEL_NAME} is LIVE!",
                            description=f"**{current_title}**\nPlaying: **{current_game_name}**\n[Watch Stream](https://twitch.tv/{config_manager.UTA_TWITCH_CHANNEL_NAME})",
                            color=discord.Color.red(),
                            timestamp=current_session_start_time_utc 
                        )
                        if last_known_tags:
                            embed.add_field(name="Tags", value=", ".join(last_known_tags[:8]) + ("..." if len(last_known_tags) > 8 else ""), inline=False)
                        
                        thumbnail_url = live_stream_data_from_api.get("thumbnail_url", "").replace("{width}", "1280").replace("{height}", "720")
                        if thumbnail_url:
                            embed.set_image(url=thumbnail_url + f"?t={int(time.time())}") 

                        asyncio.run_coroutine_threadsafe(
                            _send_status_notification_to_discord(bot_instance, None, embed=embed),
                            bot_instance.loop
                        )
                    last_viewer_log_timestamp = 0 
                
                else: 
                    should_trigger_youtube_metadata_update = False
                    
                    if current_game_name != last_known_game_name:
                        logger.info(f"UTA Status Service: Game changed for {config_manager.UTA_TWITCH_CHANNEL_NAME} from '{last_known_game_name}' to '{current_game_name}'.")
                        if bot_instance.loop and bot_instance.is_ready():
                            asyncio.run_coroutine_threadsafe(log_stream_activity_binary(EVENT_TYPE_GAME_CHANGE, current_utc_time, old_game=last_known_game_name, new_game=current_game_name), bot_instance.loop)
                            embed_gc = discord.Embed(title=f"🔄 Game Change for {config_manager.UTA_TWITCH_CHANNEL_NAME}", description=f"Now playing: **{current_game_name}**\nWas: {last_known_game_name}\n[Watch Stream](https://twitch.tv/{config_manager.UTA_TWITCH_CHANNEL_NAME})", color=discord.Color.blue(), timestamp=current_utc_time)
                            asyncio.run_coroutine_threadsafe(_send_status_notification_to_discord(bot_instance, None, embed=embed_gc), bot_instance.loop)
                        last_known_game_name = current_game_name
                        should_trigger_youtube_metadata_update = True

                    if current_title != last_known_title:
                        logger.info(f"UTA Status Service: Title changed for {config_manager.UTA_TWITCH_CHANNEL_NAME} from '{last_known_title}' to '{current_title}'.")
                        if bot_instance.loop and bot_instance.is_ready():
                            asyncio.run_coroutine_threadsafe(log_stream_activity_binary(EVENT_TYPE_TITLE_CHANGE, current_utc_time, old_title=last_known_title, new_title=current_title), bot_instance.loop)
                            embed_tc = discord.Embed(title=f"✍️ Title Change for {config_manager.UTA_TWITCH_CHANNEL_NAME}", description=f"New title: **{current_title}**\n[Watch Stream](https://twitch.tv/{config_manager.UTA_TWITCH_CHANNEL_NAME})", color=discord.Color.green(), timestamp=current_utc_time)
                            asyncio.run_coroutine_threadsafe(_send_status_notification_to_discord(bot_instance, None, embed=embed_tc), bot_instance.loop)
                        last_known_title = current_title
                        should_trigger_youtube_metadata_update = True
                    
                    if set(current_tags_from_api or []) != set(last_known_tags or []): 
                        logger.info(f"UTA Status Service: Tags changed for {config_manager.UTA_TWITCH_CHANNEL_NAME} from '{last_known_tags}' to '{current_tags_from_api}'.")
                        if bot_instance.loop and bot_instance.is_ready():
                            asyncio.run_coroutine_threadsafe(log_stream_activity_binary(EVENT_TYPE_TAGS_CHANGE, current_utc_time, old_tags=last_known_tags, new_tags=(current_tags_from_api or [])), bot_instance.loop)
                            embed_tag_c = discord.Embed(title=f"🏷️ Tags Change for {config_manager.UTA_TWITCH_CHANNEL_NAME}", color=discord.Color.orange(), timestamp=current_utc_time)
                            embed_tag_c.add_field(name="Old Tags", value=", ".join(last_known_tags[:8]) + ("..." if len(last_known_tags) > 8 else "") or "None", inline=False)
                            embed_tag_c.add_field(name="New Tags", value=", ".join((current_tags_from_api or [])[:8]) + ("..." if len(current_tags_from_api or []) > 8 else "") or "None", inline=False)
                            embed_tag_c.add_field(name="Stream Link", value=f"[Watch Stream](https://twitch.tv/{config_manager.UTA_TWITCH_CHANNEL_NAME})", inline=False)
                            asyncio.run_coroutine_threadsafe(_send_status_notification_to_discord(bot_instance, None, embed=embed_tag_c), bot_instance.loop)
                        last_known_tags = list(current_tags_from_api or [])

                    if should_trigger_youtube_metadata_update and \
                       config_manager.effective_youtube_api_enabled() and \
                       config_manager.youtube_api_session_active_global and \
                       config_manager.UTA_CURRENT_YT_BROADCAST_ID_FOR_LOGGING and \
                       config_manager.uta_yt_service: 
                        
                        current_yt_broadcast_id = config_manager.UTA_CURRENT_YT_BROADCAST_ID_FOR_LOGGING
                        yt_service_to_use = config_manager.uta_yt_service 
                        current_yt_part_num = config_manager.uta_current_restream_part_number 

                        new_yt_title = config_manager.UTA_YOUTUBE_DYNAMIC_TITLE_TEMPLATE.format(
                            twitch_username=config_manager.UTA_TWITCH_CHANNEL_NAME,
                            twitch_title=current_title, 
                            game_name=current_game_name, 
                            part_num=current_yt_part_num, 
                            date=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                            time=datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
                        )
                        # Description update is handled by restream_service when part ends/rolls over
                        # to include the full game list for that specific part.
                        # Here, we only update title if it changes on Twitch.
                        logger.info(f"UTA YouTube: Attempting to update title for broadcast {current_yt_broadcast_id} due to Twitch title/game change.")
                        logger.info(f"UTA YouTube: New proposed title: {new_yt_title}")
                        
                        update_future = asyncio.run_coroutine_threadsafe(
                            update_youtube_broadcast_metadata(yt_service_to_use, current_yt_broadcast_id, new_title=new_yt_title), # Only title
                            bot_instance.loop
                        )
                        try:
                            if update_future.result(timeout=20): 
                                logger.info(f"UTA YouTube: Successfully updated title for broadcast {current_yt_broadcast_id}.")
                        except TimeoutError:
                            logger.error(f"UTA YouTube: Timeout updating title for {current_yt_broadcast_id}.")
                        except Exception as e_yt_meta_update:
                            logger.error(f"UTA YouTube: Exception updating title for {current_yt_broadcast_id}: {e_yt_meta_update}", exc_info=True)

                current_session_peak_viewers = max(current_session_peak_viewers, current_viewers)
                if config_manager.UTA_VIEWER_COUNT_LOGGING_ENABLED and bot_instance.loop and bot_instance.is_ready() and \
                   (time.time() - last_viewer_log_timestamp >= config_manager.UTA_VIEWER_COUNT_LOG_INTERVAL_SECONDS):
                    asyncio.run_coroutine_threadsafe(log_viewer_data_binary(current_utc_time, current_viewers), bot_instance.loop)
                    last_viewer_log_timestamp = time.time()

            else: 
                if is_currently_live: 
                    is_currently_live = False 
                    config_manager.current_twitch_session_start_ts_global = None # Clear global session start
                    
                    duration_seconds = 0
                    session_start_unix, session_end_unix = 0, 0
                    if current_session_start_time_utc: 
                        duration_seconds = (current_utc_time - current_session_start_time_utc).total_seconds()
                        session_start_unix = int(current_session_start_time_utc.timestamp())
                        session_end_unix = int(current_utc_time.timestamp())

                    logger.info(f"UTA Status Service: {config_manager.UTA_TWITCH_CHANNEL_NAME} is OFFLINE. Stream lasted: {format_duration_human(int(duration_seconds))}. Peak Viewers this session: {current_session_peak_viewers}")
                    
                    # Store final state for potential description update by restreamer
                    config_manager.last_known_title_for_ended_part = last_known_title
                    config_manager.last_known_game_for_ended_part = last_known_game_name

                    avg_viewers_summary, _, num_viewer_datapoints_summary = (None, 0, 0)
                    if config_manager.UTA_VIEWER_COUNT_LOGGING_ENABLED and config_manager.UTA_VIEWER_COUNT_LOG_FILE and session_start_unix and session_end_unix :
                        avg_viewers_summary, _, num_viewer_datapoints_summary = get_viewer_stats_for_period( 
                            config_manager.UTA_VIEWER_COUNT_LOG_FILE, session_start_unix, session_end_unix
                        )

                    games_played_summary_list_str = "N/A (Activity log N/A or no games)"
                    if config_manager.UTA_STREAM_ACTIVITY_LOG_FILE and os.path.exists(config_manager.UTA_STREAM_ACTIVITY_LOG_FILE) and session_start_unix and session_end_unix:
                        game_segments_from_log = parse_stream_activity_for_game_segments( 
                            config_manager.UTA_STREAM_ACTIVITY_LOG_FILE, session_start_unix, session_end_unix
                        )
                        if game_segments_from_log:
                            games_summary_dict = {}
                            for seg_item in game_segments_from_log:
                                games_summary_dict[seg_item['game']] = games_summary_dict.get(seg_item['game'], 0) + (seg_item['end_ts'] - seg_item['start_ts'])
                            sorted_games_list = sorted(games_summary_dict.items(), key=lambda item: item[1], reverse=True)
                            games_played_parts_temp = [f"{game_name} ({format_duration_human(int(dur_sec))})" for game_name, dur_sec in sorted_games_list if game_name and game_name != "N/A"] # Filter N/A games for summary
                            if games_played_parts_temp: games_played_summary_list_str = ", ".join(games_played_parts_temp)
                            elif not games_played_parts_temp and game_segments_from_log : games_played_summary_list_str = "Game details not available for this session" # All games were N/A
                            if len(games_played_summary_list_str) > 1000: games_played_summary_list_str = games_played_summary_list_str[:997] + "..."

                    follower_gain_summary_str = "N/A (Follower log N/A)"
                    if config_manager.FCTD_FOLLOWER_DATA_FILE and os.path.exists(config_manager.FCTD_FOLLOWER_DATA_FILE) and \
                       config_manager.FCTD_TWITCH_USERNAME and \
                       config_manager.FCTD_TWITCH_USERNAME.lower() == (config_manager.UTA_TWITCH_CHANNEL_NAME or "").lower() and \
                       session_start_unix and session_end_unix:
                        s_foll, e_foll, _, _, _ = read_and_find_records_for_period( 
                            config_manager.FCTD_FOLLOWER_DATA_FILE, session_start_unix, session_end_unix
                        )
                        if s_foll is not None and e_foll is not None:
                            gain = e_foll - s_foll
                            follower_gain_summary_str = f"{gain:+,} followers"
                        else:
                            follower_gain_summary_str = "No follower data for this session's timeframe"

                    if bot_instance.loop and bot_instance.is_ready():
                        asyncio.run_coroutine_threadsafe(log_stream_activity_binary(EVENT_TYPE_STREAM_END, current_utc_time, duration_seconds=int(duration_seconds), peak_viewers=current_session_peak_viewers), bot_instance.loop)
                        
                        embed_summary = discord.Embed(title=f"📊 Stream Session Summary for {config_manager.UTA_TWITCH_CHANNEL_NAME}", color=discord.Color.dark_grey(), timestamp=current_utc_time)
                        embed_summary.set_author(name=config_manager.UTA_TWITCH_CHANNEL_NAME, url=f"https://twitch.tv/{config_manager.UTA_TWITCH_CHANNEL_NAME}")
                        embed_summary.add_field(name="Status", value="⚫ OFFLINE", inline=False)
                        embed_summary.add_field(name="Duration", value=format_duration_human(int(duration_seconds)), inline=True)
                        embed_summary.add_field(name="Peak Viewers (Session)", value=f"{current_session_peak_viewers:,}", inline=True)
                        if avg_viewers_summary is not None:
                            embed_summary.add_field(name="Avg. Viewers (Session)", value=f"{avg_viewers_summary:,.1f} (from {num_viewer_datapoints_summary} points)", inline=True)
                        else:
                            embed_summary.add_field(name="Avg. Viewers (Session)", value="N/A", inline=True)
                        
                        embed_summary.add_field(name="Games Played This Session", value=games_played_summary_list_str, inline=False)
                        if config_manager.FCTD_TWITCH_USERNAME == config_manager.UTA_TWITCH_CHANNEL_NAME: 
                             embed_summary.add_field(name="Follower Change This Session", value=follower_gain_summary_str, inline=False)
                        
                        asyncio.run_coroutine_threadsafe(_send_status_notification_to_discord(bot_instance, None, embed=embed_summary), bot_instance.loop)

                    current_session_start_time_utc = None; current_session_peak_viewers = 0; 
                    last_known_game_name = None; last_known_title = None; last_known_tags = None
            
            if shutdown_event.wait(timeout=config_manager.UTA_STREAM_STATUS_POLL_INTERVAL_SECONDS):
                # If shutting down while live, capture final state for potential description update
                if is_currently_live:
                    config_manager.last_known_title_for_ended_part = last_known_title
                    config_manager.last_known_game_for_ended_part = last_known_game_name
                break 
        
        except Exception as e:
            logger.error(f"UTA Stream Status Monitor: An unexpected error occurred in the monitor loop: {e}", exc_info=True)
            is_currently_live = False; current_session_start_time_utc = None; current_session_peak_viewers = 0;
            last_known_game_name = None; last_known_title = None; last_known_tags = None;
            config_manager.current_twitch_session_start_ts_global = None
            if shutdown_event.wait(timeout=60): break 
    
    if is_currently_live: # If loop exited (shutdown) while live
        config_manager.last_known_title_for_ended_part = last_known_title
        config_manager.last_known_game_for_ended_part = last_known_game_name
            
    logger.info(f"UTA Stream Status Monitor Service thread ({threading.current_thread().name}) has finished.")