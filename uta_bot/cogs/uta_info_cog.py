import discord
from discord.ext import commands
from datetime import datetime, timezone, timedelta
import os
import struct
import io 
import asyncio # For to_thread
import json # For twitchinfo response parsing (if needed, handled by requests.json())
# requests is now handled by twitch_api_handler

from uta_bot import config_manager
from uta_bot.core.bot_instance import bot
from uta_bot.utils.formatters import format_duration_human, parse_duration_to_timedelta
from uta_bot.utils.data_logging import (
    read_stream_durations_for_period, 
    parse_stream_activity_for_game_segments,
    get_viewer_stats_for_period, 
    read_and_find_records_for_period
)
from uta_bot.utils.constants import (
    BINARY_RECORD_SIZE, STREAM_DURATION_RECORD_SIZE, SA_BASE_HEADER_SIZE,
    EVENT_TYPE_STREAM_START, EVENT_TYPE_STREAM_END, EVENT_TYPE_GAME_CHANGE, EVENT_TYPE_TITLE_CHANGE
)
# Import the centralized API request function
from uta_bot.services.twitch_api_handler import make_uta_twitch_api_request # For UTA features
# For twitchinfo, we can use the fctd_twitch_api for general public data if suitable,
# or use make_uta_twitch_api_request if UTA specific token/handling is desired.
# The original used a local _uta_make_twitch_api_request_local based on fctd_twitch_api.
# Let's keep that pattern for twitchinfo for now, simplifying its internal call.

class UTAInfoCog(commands.Cog, name="UTA Informational Commands"):
    def __init__(self, bot_instance):
        self.bot = bot_instance

    async def cog_check(self, ctx):
        if ctx.guild and config_manager.FCTD_COMMAND_CHANNEL_ID is not None and \
           ctx.channel.id != config_manager.FCTD_COMMAND_CHANNEL_ID:
            return False
        return True

    @commands.command(name="streamtime", help="Total stream time for UTA_TWITCH_CHANNEL_NAME over a period (from restream or activity logs). Usage: !streamtime <period>")
    async def stream_time_command(self, ctx: commands.Context, *, duration_input: str = None):
        if not config_manager.UTA_ENABLED or not config_manager.UTA_TWITCH_CHANNEL_NAME:
            await ctx.send("UTA module or UTA_TWITCH_CHANNEL_NAME is not configured/enabled.")
            return

        log_file_to_use = None
        source_description = ""
        is_activity_log_source = False

        if config_manager.UTA_STREAM_ACTIVITY_LOG_FILE and os.path.exists(config_manager.UTA_STREAM_ACTIVITY_LOG_FILE) and config_manager.UTA_STREAM_STATUS_NOTIFICATIONS_ENABLED:
            log_file_to_use = config_manager.UTA_STREAM_ACTIVITY_LOG_FILE
            source_description = "Twitch live sessions (from activity log)"
            is_activity_log_source = True
        elif config_manager.UTA_STREAM_DURATION_LOG_FILE and os.path.exists(config_manager.UTA_STREAM_DURATION_LOG_FILE) and config_manager.UTA_RESTREAMER_ENABLED:
            log_file_to_use = config_manager.UTA_STREAM_DURATION_LOG_FILE
            source_description = "YouTube restream durations (from restream log)"
        else:
            await ctx.send("No suitable stream data log (activity or restream duration) found or configured for this command.")
            return

        if duration_input is None:
            await ctx.send(f"Please provide a duration (e.g., `7d`, `1mo`, `all`). Example: `{config_manager.FCTD_COMMAND_PREFIX}streamtime 7d`")
            return

        now_utc = datetime.now(timezone.utc)
        query_start_unix, query_end_unix = 0, int(now_utc.timestamp()) 
        period_name_display = "all time"

        if duration_input.lower() != "all":
            time_delta, parsed_period_name = parse_duration_to_timedelta(duration_input)
            if not time_delta:
                await ctx.send(parsed_period_name) 
                return
            query_start_unix = int((now_utc - time_delta).timestamp())
            period_name_display = parsed_period_name
        
        total_duration_seconds = 0
        num_sessions = 0

        async with ctx.typing():
            if is_activity_log_source:
                game_segments = await asyncio.to_thread(
                    parse_stream_activity_for_game_segments, 
                    log_file_to_use, query_start_unix, query_end_unix
                )
                total_duration_seconds = sum(seg['end_ts'] - seg['start_ts'] for seg in game_segments)
                if game_segments:
                    game_segments.sort(key=lambda s: s['start_ts'])
                    num_sessions = 1
                    for i in range(1, len(game_segments)):
                        if game_segments[i]['start_ts'] - game_segments[i-1]['end_ts'] > 600:
                            num_sessions += 1
            else: 
                total_duration_seconds, num_sessions = await asyncio.to_thread(
                    read_stream_durations_for_period,
                    log_file_to_use, query_start_unix, query_end_unix
                )

        human_readable_duration = format_duration_human(total_duration_seconds)
        embed_title = f"Stream Time for {config_manager.UTA_TWITCH_CHANNEL_NAME} ({period_name_display})"
        embed_desc = (f"{config_manager.UTA_TWITCH_CHANNEL_NAME} was live for **{human_readable_duration}** "
                      f"across {num_sessions} session(s) in the {period_name_display}.\n"
                      f"*Data sourced from: {source_description}*")
        
        embed = discord.Embed(
            title=embed_title,
            description=embed_desc,
            color=discord.Color.purple() if num_sessions > 0 else discord.Color.light_grey()
        )
        period_start_dt = datetime.fromtimestamp(query_start_unix, tz=timezone.utc) if duration_input.lower() != "all" else None
        period_end_dt = datetime.fromtimestamp(query_end_unix, tz=timezone.utc)
        
        footer_text = f"Query period end: {discord.utils.format_dt(period_end_dt)}"
        if period_start_dt:
            footer_text = f"Query period: {discord.utils.format_dt(period_start_dt)} to {discord.utils.format_dt(period_end_dt)}"
        embed.set_footer(text=footer_text)
        
        await ctx.send(embed=embed)

    @commands.command(name="twitchinfo", aliases=['tinfo'], help="Shows public info for a Twitch channel. Usage: !twitchinfo [username]")
    async def twitch_info_command(self, ctx: commands.Context, twitch_username_to_check: str = None):
        if not twitch_username_to_check:
            if config_manager.UTA_ENABLED and config_manager.UTA_TWITCH_CHANNEL_NAME:
                twitch_username_to_check = config_manager.UTA_TWITCH_CHANNEL_NAME
            else:
                await ctx.send(f"Please specify a Twitch username or configure `UTA_TWITCH_CHANNEL_NAME` in the bot's settings. Usage: `{config_manager.FCTD_COMMAND_PREFIX}twitchinfo <username>`")
                return
        
        # Use the fctd_twitch_api for general public data requests if available
        if not config_manager.fctd_twitch_api:
            await ctx.send("Twitch API client (fctd) not initialized. Cannot fetch Twitch info.")
            return

        async with ctx.typing():
            # Fetch user data (ID, profile pic, description, views, created_at)
            # Note: fctd_twitch_api.get_user_id only returns ID. Need more general request.
            # We will directly use its _get_app_access_token and make requests, similar to the original cog.
            token = await config_manager.fctd_twitch_api._get_app_access_token()
            if not token:
                await ctx.send("Failed to get Twitch API token. Cannot fetch info.")
                return

            headers = {"Client-ID": config_manager.TWITCH_CLIENT_ID, "Authorization": f"Bearer {token}"}

            async def _make_request(endpoint, params=None):
                url = f"https://api.twitch.tv/helix/{endpoint.lstrip('/')}"
                response = await asyncio.to_thread(requests.get, url, headers=headers, params=params, timeout=10)
                response.raise_for_status() # Will raise for 4xx/5xx
                return response.json()

            try:
                user_data_response = await _make_request("users", params={"login": twitch_username_to_check})
                if not user_data_response or not user_data_response.get("data"):
                    await ctx.send(f"Could not find Twitch user: `{twitch_username_to_check}`. Please check the username.")
                    return
                
                user_info = user_data_response["data"][0]
                broadcaster_id = user_info["id"]

                channel_task = _make_request("channels", params={"broadcaster_id": broadcaster_id})
                stream_task = _make_request("streams", params={"user_id": broadcaster_id}) 
                followers_task = _make_request("channels/followers", params={"broadcaster_id": broadcaster_id})
                
                channel_data_response, stream_data_response, followers_data_response = await asyncio.gather(
                    channel_task, stream_task, followers_task, return_exceptions=True
                )
                # Handle potential errors from gather
                if isinstance(channel_data_response, Exception):
                    config_manager.logger.error(f"TwitchInfo: Error fetching channel data: {channel_data_response}")
                    channel_data_response = {} # Default to empty
                if isinstance(stream_data_response, Exception):
                    config_manager.logger.error(f"TwitchInfo: Error fetching stream data: {stream_data_response}")
                    stream_data_response = {}
                if isinstance(followers_data_response, Exception):
                    config_manager.logger.error(f"TwitchInfo: Error fetching followers data: {followers_data_response}")
                    followers_data_response = {}


            except requests.exceptions.HTTPError as http_err:
                config_manager.logger.error(f"TwitchInfo: HTTP error for API request: {http_err}")
                if hasattr(http_err, 'response') and http_err.response is not None: 
                    config_manager.logger.error(f"TwitchInfo: Response content: {http_err.response.text}")
                await ctx.send(f"An error occurred while fetching Twitch data: {http_err.response.status_code}")
                return
            except Exception as e:
                config_manager.logger.error(f"TwitchInfo: Unexpected error: {e}", exc_info=True)
                await ctx.send("An unexpected error occurred while fetching Twitch info.")
                return


        channel_info = channel_data_response.get("data", [{}])[0] if channel_data_response and channel_data_response.get("data") else {}
        stream_info = stream_data_response.get("data", [{}])[0] if stream_data_response and stream_data_response.get("data") else {}
        follower_count = followers_data_response.get("total", 0) if followers_data_response else 0

        embed = discord.Embed(
            title=f"Twitch Info: {user_info.get('display_name', twitch_username_to_check)}",
            color=discord.Color.purple()
        )
        if user_info.get("profile_image_url"):
            embed.set_thumbnail(url=user_info.get("profile_image_url"))
        
        description = user_info.get("description")
        if description:
            embed.description = description[:250] + "..." if len(description) > 250 else description
        
        embed.add_field(name="Followers", value=f"{follower_count:,}", inline=True)
        embed.add_field(name="Total Views", value=f"{user_info.get('view_count', 0):,}", inline=True)
        if user_info.get("created_at"):
            created_dt = datetime.fromisoformat(user_info.get("created_at").replace('Z', '+00:00'))
            embed.add_field(name="Account Created", value=discord.utils.format_dt(created_dt, 'D'), inline=True) 

        is_live = stream_info and stream_info.get("type") == "live"
        if is_live:
            live_title = stream_info.get("title", "N/A")
            live_game = stream_info.get("game_name", "N/A")
            viewers = stream_info.get("viewer_count", 0)
            started_at_str = stream_info.get("started_at")
            uptime_str = "N/A"
            if started_at_str:
                started_dt_utc = datetime.fromisoformat(started_at_str.replace('Z', '+00:00'))
                uptime_delta_seconds = (datetime.now(timezone.utc) - started_dt_utc).total_seconds()
                uptime_str = format_duration_human(int(uptime_delta_seconds))
            
            live_details = (f"**Title:** {live_title}\n"
                            f"**Game:** {live_game}\n"
                            f"**Viewers:** {viewers:,}\n"
                            f"**Uptime:** {uptime_str}")
            embed.add_field(name="🔴 LIVE NOW", value=live_details, inline=False)
        else:
            embed.add_field(name="Status", value="Offline", inline=False)
            if channel_info.get("title"):
                embed.add_field(name="Last Title", value=channel_info.get("title"), inline=True)
            if channel_info.get("game_name"):
                embed.add_field(name="Last Game", value=channel_info.get("game_name"), inline=True)

        if channel_info.get("broadcaster_language"):
            embed.add_field(name="Language", value=channel_info.get("broadcaster_language").upper(), inline=True)

        current_tags_to_display = stream_info.get("tags", []) if is_live else (channel_info.get("tags", []) if channel_info else [])
        if current_tags_to_display:
            tags_str = ", ".join(current_tags_to_display[:8]) 
            if len(current_tags_to_display) > 8:
                tags_str += "..."
            embed.add_field(name="Tags", value=tags_str if tags_str else "None", inline=False)
        
        embed.url = f"https://twitch.tv/{twitch_username_to_check}"
        embed.set_footer(text=f"User ID: {broadcaster_id} | Data fetched at")
        embed.timestamp = datetime.now(timezone.utc)
        
        await ctx.send(embed=embed)

    @commands.command(name="gamestats", help="Game stats with optional viewer histogram. Usage: !gamestats \"<Game Name>\" [period|all]")
    async def game_stats_command(self, ctx: commands.Context, game_name_input: str, *, duration_input: str = "all"):
        if not (config_manager.UTA_ENABLED and config_manager.UTA_TWITCH_CHANNEL_NAME and config_manager.UTA_STREAM_ACTIVITY_LOG_FILE):
            await ctx.send("UTA module, target Twitch channel for UTA, or stream activity log is not configured/enabled. This command relies on activity logs.")
            return

        target_game_name = game_name_input.strip()
        if not target_game_name:
            await ctx.send("Please provide a game name. Usage: `!gamestats \"Exact Game Name From Twitch\" [period|all]`")
            return
        
        now_utc = datetime.now(timezone.utc)
        query_start_unix, query_end_unix = 0, int(now_utc.timestamp()) 
        period_name_display = "all time"

        if duration_input.lower() != "all":
            time_delta, parsed_period_name = parse_duration_to_timedelta(duration_input)
            if not time_delta:
                await ctx.send(parsed_period_name) 
                return
            query_start_unix = int((now_utc - time_delta).timestamp())
            period_name_display = parsed_period_name
        
        # Plotting is now handled by PlotCog if this command requests it.
        # For now, this command just shows text stats.
        async with ctx.typing():
            game_segments_all = await asyncio.to_thread(
                parse_stream_activity_for_game_segments, 
                config_manager.UTA_STREAM_ACTIVITY_LOG_FILE, 
                query_start_unix, 
                query_end_unix
            )
            
            target_game_segments_found = [
                seg for seg in game_segments_all if seg.get('game', '').lower() == target_game_name.lower()
            ]

            if not target_game_segments_found:
                await ctx.send(f"No streaming data found for game '{target_game_name}' in the period '{period_name_display}'. Please ensure the game name is exactly as it appears on Twitch.")
                return

            total_time_streamed_for_game_sec = sum(seg['end_ts'] - seg['start_ts'] for seg in target_game_segments_found)
            
            avg_viewers_for_game_stat, total_follower_gain_for_game_stat = None, None
            sessions_with_follower_data_count = 0
            viewer_counts_for_game = [] # For calculating avg viewers
            total_viewer_datapoints_for_game_stat = 0

            if config_manager.UTA_VIEWER_COUNT_LOGGING_ENABLED and config_manager.UTA_VIEWER_COUNT_LOG_FILE and os.path.exists(config_manager.UTA_VIEWER_COUNT_LOG_FILE):
                min_segment_start_ts = min(s['start_ts'] for s in target_game_segments_found) if target_game_segments_found else 0
                max_segment_end_ts = max(s['end_ts'] for s in target_game_segments_found) if target_game_segments_found else 0
                
                all_viewer_records_in_game_period = []
                if min_segment_start_ts < max_segment_end_ts :
                    # This is less efficient than the original direct file read in the copied func.
                    # get_viewer_stats_for_period is for a single period.
                    # We need to iterate over segments and call it, or read once and filter.
                    # Reverting to a more direct read similar to the original:
                    try:
                        with open(config_manager.UTA_VIEWER_COUNT_LOG_FILE, 'rb') as vf:
                            while True:
                                chunk = vf.read(BINARY_RECORD_SIZE)
                                if not chunk: break
                                if len(chunk) < BINARY_RECORD_SIZE: break
                                ts, count = struct.unpack(config_manager.BINARY_RECORD_FORMAT, chunk)
                                if min_segment_start_ts <= ts < max_segment_end_ts:
                                    all_viewer_records_in_game_period.append({'ts': ts, 'count': count})
                    except Exception as e_viewer_read:
                        config_manager.logger.error(f"Error reading viewer log for gamestats: {e_viewer_read}")
                
                if all_viewer_records_in_game_period:
                    for seg in target_game_segments_found: 
                        for vr_rec in all_viewer_records_in_game_period:
                            if seg['start_ts'] <= vr_rec['ts'] < seg['end_ts']:
                                viewer_counts_for_game.append(vr_rec['count'])
                    
                    if viewer_counts_for_game:
                        avg_viewers_for_game_stat = sum(viewer_counts_for_game) / len(viewer_counts_for_game)
                        total_viewer_datapoints_for_game_stat = len(viewer_counts_for_game)

            if config_manager.FCTD_FOLLOWER_DATA_FILE and os.path.exists(config_manager.FCTD_FOLLOWER_DATA_FILE) and \
               config_manager.FCTD_TWITCH_USERNAME and \
               config_manager.FCTD_TWITCH_USERNAME.lower() == (config_manager.UTA_TWITCH_CHANNEL_NAME or "").lower():
                
                current_total_follower_gain = 0
                for seg in target_game_segments_found:
                    s_foll, e_foll, _, _, _ = await asyncio.to_thread(
                        read_and_find_records_for_period,
                        config_manager.FCTD_FOLLOWER_DATA_FILE, 
                        seg['start_ts'], 
                        seg['end_ts']    
                    )
                    if s_foll is not None and e_foll is not None:
                        current_total_follower_gain += (e_foll - s_foll)
                        sessions_with_follower_data_count +=1
                total_follower_gain_for_game_stat = current_total_follower_gain

            embed = discord.Embed(
                title=f"Game Stats for: {target_game_name}",
                description=f"Channel: {config_manager.UTA_TWITCH_CHANNEL_NAME}\nPeriod: {period_name_display}",
                color=discord.Color.blue()
            )
            embed.add_field(name="Total Time Streamed", value=format_duration_human(total_time_streamed_for_game_sec), inline=False)

            if avg_viewers_for_game_stat is not None:
                avg_view_val = f"{avg_viewers_for_game_stat:,.0f} (from {total_viewer_datapoints_for_game_stat} data points)" if total_viewer_datapoints_for_game_stat > 0 else "No viewer data during these game sessions."
                embed.add_field(name="Average Viewers", value=avg_view_val, inline=True)
            else:
                embed.add_field(name="Average Viewers", value="Viewer count logging not enabled or no relevant data.", inline=True)

            if total_follower_gain_for_game_stat is not None:
                gain_str = f"{total_follower_gain_for_game_stat:+,}" if total_follower_gain_for_game_stat != 0 else "0"
                foll_gain_val = f"{gain_str} followers (across {sessions_with_follower_data_count} sessions with data)"
                embed.add_field(name="Follower Change During Game", value=foll_gain_val, inline=True)
            elif config_manager.FCTD_TWITCH_USERNAME == config_manager.UTA_TWITCH_CHANNEL_NAME: 
                 embed.add_field(name="Follower Change During Game", value="Follower logging not enabled or no relevant data.", inline=True)

            embed.set_footer(text=f"{len(target_game_segments_found)} play session(s) found for '{target_game_name}'.")
            # Plotting is responsibility of plot_cog. gamestats just gives text for now.
            
        await ctx.send(embed=embed) # No file sent from here

async def setup(bot_instance):
    await bot_instance.add_cog(UTAInfoCog(bot_instance))