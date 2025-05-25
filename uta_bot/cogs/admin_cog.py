import discord
from discord.ext import commands
import os
import json
import time
from datetime import datetime, timezone, timedelta
import sys
import shutil
import struct
import asyncio
import random
import io # For mocking ctx.send output for command tests

from uta_bot import config_manager
from uta_bot.core.bot_instance import bot
from uta_bot.utils.data_logging import (
    read_string_from_file_handle, read_tag_list_from_file_handle, consume_activity_event_body,
    log_bot_session_event, calculate_bot_runtime_in_period,
    read_and_find_records_for_period,
    parse_stream_activity_for_game_segments,
    read_stream_durations_for_period,
    get_viewer_stats_for_period,
)
from uta_bot.utils.constants import (
    BINARY_RECORD_SIZE, BINARY_RECORD_FORMAT,
    STREAM_DURATION_RECORD_SIZE, STREAM_DURATION_RECORD_FORMAT,
    CHAT_ACTIVITY_RECORD_SIZE, CHAT_ACTIVITY_RECORD_FORMAT,
    SA_BASE_HEADER_SIZE, SA_BASE_HEADER_FORMAT,
    SA_STRING_LEN_SIZE, SA_STRING_LEN_FORMAT, SA_LIST_HEADER_SIZE, SA_LIST_HEADER_FORMAT, SA_INT_SIZE, SA_INT_FORMAT,
    EVENT_TYPE_STREAM_START, EVENT_TYPE_STREAM_END, EVENT_TYPE_GAME_CHANGE,
    EVENT_TYPE_TITLE_CHANGE, EVENT_TYPE_TAGS_CHANGE,
    BOT_SESSION_RECORD_SIZE, BOT_SESSION_RECORD_FORMAT, BOT_EVENT_START, BOT_EVENT_STOP
)
from uta_bot.utils.formatters import format_duration_human
from uta_bot.services.threading_manager import start_all_services, stop_all_services
from uta_bot.services.twitch_api_handler import get_uta_twitch_access_token, get_uta_broadcaster_id
from uta_bot.services.youtube_api_handler import get_youtube_service
from uta_bot.core.background_tasks import update_channel_name_and_log_followers

# Import the TWITCHIO_AVAILABLE flag from the cogs package's __init__.py
try:
    from ..cogs import TWITCHIO_AVAILABLE as COGS_TWITCHIO_MODULE_AVAILABLE
except ImportError:
    config_manager.logger.warning("AdminCog: Could not import TWITCHIO_AVAILABLE from cogs package. Assuming False for Twitch Chat Cog status.")
    COGS_TWITCHIO_MODULE_AVAILABLE = False


def get_config_diff(old_dict, new_dict):
    diff = {}
    all_keys = set(old_dict.keys()) | set(new_dict.keys())
    for k in all_keys:
        old_v = old_dict.get(k)
        new_v = new_dict.get(k)
        if old_v != new_v:
            if any(s in k.lower() for s in ["token", "secret", "key", "webhook_url"]):
                diff[k] = {"old": "**** (Sensitive)", "new": "**** (Sensitive)"}
            else:
                diff[k] = {"old": old_v, "new": new_v}
    return diff

# Minimal Mock Context for command testing
class MockAuthor:
    def __init__(self, id, name="TestUser"):
        self.id = id
        self.name = name
        self.mention = f"<@{id}>"
        self.display_name = name

class MockChannel:
    def __init__(self, id):
        self.id = id
        self.guild = MockGuild(123) # Dummy guild ID

    async def send(self, *args, **kwargs): # Mock send
        output_content = args[0] if args else kwargs.get("content", "")
        output_embed = kwargs.get("embed")
        # print(f"MockChannel.send: Content='{output_content}', Embed={'Yes' if output_embed else 'No'}")
        return MockMessage()

class MockGuild:
    def __init__(self, id):
        self.id = id

class MockMessage:
    async def edit(self, *args, **kwargs):
        pass

class MockContext:
    def __init__(self, bot_instance, author_id, channel_id=None):
        self.bot = bot_instance
        self.author = MockAuthor(author_id)
        self.channel = MockChannel(channel_id if channel_id else 987)
        self.guild = self.channel.guild
        self.message = MockMessage()
        self.command_prefix = bot_instance.command_prefix if hasattr(bot_instance, 'command_prefix') else config_manager.FCTD_COMMAND_PREFIX # Ensure prefix exists
        self.invoked_with = ""
        self.command = None

    async def typing(self):
        class MockTyping:
            async def __aenter__(self): pass
            async def __aexit__(self, exc_type, exc, tb): pass
        return MockTyping()
    
    async def send(self, *args, **kwargs):
        return await self.channel.send(*args, **kwargs)


class AdminCog(commands.Cog, name="Admin Commands"):
    def __init__(self, bot_instance):
        self.bot = bot_instance

    def cog_check(self, ctx):
        if ctx.guild and config_manager.FCTD_COMMAND_CHANNEL_ID is not None and \
           ctx.channel.id != config_manager.FCTD_COMMAND_CHANNEL_ID:
            return False
        return True

    @commands.command(name="uptime", help="Shows how long the bot has been running in the current session.")
    @commands.is_owner()
    async def uptime_command(self, ctx: commands.Context):
        if config_manager.bot_start_time is None:
            await ctx.send("Bot start time not recorded yet.")
            return

        uptime_delta = datetime.now(timezone.utc) - config_manager.bot_start_time
        human_uptime = format_duration_human(int(uptime_delta.total_seconds()))

        embed = discord.Embed(
            title="Bot Uptime (Current Session)",
            description=f"I have been running for **{human_uptime}** in this session.",
            color=discord.Color.green()
        )
        embed.add_field(name="Current Session Started At", value=discord.utils.format_dt(config_manager.bot_start_time, 'F'), inline=False)
        await ctx.send(embed=embed)

    @commands.command(name="runtime", help="Shows bot's total runtime in a past period. Usage: !runtime <period> (e.g., 24h, 7d)")
    async def runtime_command(self, ctx: commands.Context, *, duration_input: str = None):
        if not config_manager.BOT_SESSION_LOG_FILE_PATH:
            await ctx.send("Bot session logging is not configured."); return
        if not os.path.exists(config_manager.BOT_SESSION_LOG_FILE_PATH):
            await ctx.send(f"Bot session log file (`{os.path.basename(config_manager.BOT_SESSION_LOG_FILE_PATH)}`) not found."); return
        if duration_input is None:
            await ctx.send(f"Please specify a period. Usage: `{config_manager.FCTD_COMMAND_PREFIX}runtime <duration>` (e.g., `24h`, `7d`, `1mo`)."); return

        from uta_bot.utils.formatters import parse_duration_to_timedelta # Local import for clarity
        time_delta, period_name_display = parse_duration_to_timedelta(duration_input)
        if not time_delta:
            await ctx.send(period_name_display); return

        now_utc = datetime.now(timezone.utc)
        query_end_unix = int(now_utc.timestamp())
        query_start_unix = int((now_utc - time_delta).timestamp())

        async with ctx.typing():
            total_uptime_sec, num_sessions = await asyncio.to_thread(
                calculate_bot_runtime_in_period,
                config_manager.BOT_SESSION_LOG_FILE_PATH,
                query_start_unix,
                query_end_unix
            )

        human_uptime_in_period = format_duration_human(total_uptime_sec)
        embed = discord.Embed(
            title=f"Bot Runtime History ({period_name_display})",
            description=f"The bot was active for a total of **{human_uptime_in_period}** during the {period_name_display}.",
            color=discord.Color.blue()
        )
        embed.add_field(name="Query Period Start", value=discord.utils.format_dt(datetime.fromtimestamp(query_start_unix, tz=timezone.utc), 'F'), inline=True)
        embed.add_field(name="Query Period End", value=discord.utils.format_dt(datetime.fromtimestamp(query_end_unix, tz=timezone.utc), 'F'), inline=True)
        embed.add_field(name="Contributing Sessions", value=str(num_sessions), inline=True)

        period_duration_seconds = query_end_unix - query_start_unix
        if period_duration_seconds > 0 :
            percentage_uptime = (total_uptime_sec / period_duration_seconds) * 100
            embed.add_field(name="Uptime Percentage", value=f"{percentage_uptime:.2f}% of the period", inline=False)
        else:
            embed.add_field(name="Uptime Percentage", value="N/A (invalid period)", inline=False)
        await ctx.send(embed=embed)

    @commands.command(name="reloadconfig", aliases=['reloadcfg', 'cfgrel'], help="Reloads config.json. (Bot owner only)")
    @commands.is_owner()
    async def reload_config_command(self, ctx: commands.Context):
        await ctx.send("Attempting to reload configuration...")
        config_manager.logger.info(f"Configuration reload initiated by {ctx.author} (ID: {ctx.author.id}).")

        old_config_data_copy = config_manager.config_data.copy()
        old_uta_enabled_overall = old_config_data_copy.get('UTA_ENABLED', False)
        old_twitch_chat_enabled = old_config_data_copy.get('TWITCH_CHAT_ENABLED', False) 

        success, new_loaded_data_dict = config_manager.load_config(initial_load=False)

        if not success:
            await ctx.send(f"Configuration reload failed: {new_loaded_data_dict}")
            config_manager.logger.error(f"Configuration reload failed: {new_loaded_data_dict}")
            return

        new_uta_enabled_overall = new_loaded_data_dict.get('UTA_ENABLED', False)
        new_twitch_chat_enabled = new_loaded_data_dict.get('TWITCH_CHAT_ENABLED', False)
        uta_config_changed_structurally = False
        if new_uta_enabled_overall:
            uta_structural_keys = [
                "UTA_TWITCH_CHANNEL_NAME", "UTA_STREAMLINK_PATH", "UTA_FFMPEG_PATH",
                "UTA_YOUTUBE_API_ENABLED", "UTA_YOUTUBE_CLIENT_SECRET_FILE", "UTA_YOUTUBE_TOKEN_FILE",
                "UTA_YOUTUBE_RTMP_URL_BASE", "UTA_YOUTUBE_STREAM_KEY",
                "UTA_CLIP_MONITOR_ENABLED", "UTA_RESTREAMER_ENABLED", "UTA_STREAM_STATUS_NOTIFICATIONS_ENABLED",
                "UTA_CHECK_INTERVAL_SECONDS_CLIPS", "UTA_CHECK_INTERVAL_SECONDS_RESTREAMER",
                "UTA_STREAM_STATUS_POLL_INTERVAL_SECONDS",
                "UTA_YOUTUBE_SCHEDULED_ROLLOVER_HOURS",
                "UTA_RESTREAM_MAX_CONSECUTIVE_FAILURES", "UTA_RESTREAM_LONG_COOLDOWN_SECONDS",
                "UTA_YOUTUBE_PLAYABILITY_CHECK_ENABLED", "UTA_YOUTUBE_PLAYABILITY_CHECK_RETRIES",
                "UTA_YOUTUBE_PLAYABILITY_CHECK_DELAY_SECONDS", "UTA_FFMPEG_STARTUP_WAIT_SECONDS",
                "UTA_YOUTUBE_AUTO_CHAPTERS_ENABLED", "UTA_YOUTUBE_MIN_CHAPTER_DURATION_SECONDS",
                "UTA_YOUTUBE_DESCRIPTION_CHAPTER_MARKER", "UTA_YOUTUBE_CHAPTER_TITLE_TEMPLATE"
            ]
            for key in uta_structural_keys:
                if old_config_data_copy.get(key) != new_loaded_data_dict.get(key):
                    uta_config_changed_structurally = True
                    config_manager.logger.info(f"Reload: UTA structural key '{key}' changed.")
                    break
        
        twitch_chat_config_changed_structurally = False
        if new_twitch_chat_enabled: # Only check if the feature is enabled in the new config
            chat_structural_keys = [
                "TWITCH_CHAT_NICKNAME", "TWITCH_CHAT_OAUTH_TOKEN", 
                "UTA_TWITCH_CHANNEL_NAME", # This is used as the target channel for chat
                "TWITCH_CHAT_LOG_INTERVAL_SECONDS", "DISCORD_TWITCH_CHAT_MIRROR_ENABLED",
                "DISCORD_TWITCH_CHAT_MIRROR_CHANNEL_ID"
            ]
            for key in chat_structural_keys:
                if old_config_data_copy.get(key) != new_loaded_data_dict.get(key):
                    twitch_chat_config_changed_structurally = True
                    config_manager.logger.info(f"Reload: Twitch Chat structural key '{key}' changed.")
                    break


        needs_uta_thread_restart = (old_uta_enabled_overall != new_uta_enabled_overall) or \
                                   (new_uta_enabled_overall and uta_config_changed_structurally)
        
        needs_twitch_chat_cog_restart = (old_twitch_chat_enabled != new_twitch_chat_enabled) or \
                                        (new_twitch_chat_enabled and twitch_chat_config_changed_structurally)


        if needs_uta_thread_restart:
            config_manager.logger.info("Reload: UTA related configuration or master enable status changed. Managing UTA threads.")
            await stop_all_services() 

        if needs_twitch_chat_cog_restart:
            config_manager.logger.info("Reload: Twitch Chat Monitor configuration or enable status changed. Attempting cog reload/load/unload.")
            cog_name_chat = "uta_bot.cogs.twitch_chat_cog"
            try:
                if new_twitch_chat_enabled:
                    if cog_name_chat in self.bot.extensions: # If already loaded, reload
                        await self.bot.reload_extension(cog_name_chat)
                        config_manager.logger.info(f"Reload: {cog_name_chat} reloaded successfully.")
                    else: # Not loaded, try to load
                        await self.bot.load_extension(cog_name_chat)
                        config_manager.logger.info(f"Reload: {cog_name_chat} loaded successfully.")
                elif old_twitch_chat_enabled and not new_twitch_chat_enabled: # Was enabled, now disabled
                    if cog_name_chat in self.bot.extensions:
                        await self.bot.unload_extension(cog_name_chat)
                        config_manager.logger.info(f"Reload: {cog_name_chat} unloaded as it's now disabled.")
            except commands.ExtensionError as e_ext: # Covers NotLoaded, AlreadyLoaded, NoEntryPoint, etc.
                config_manager.logger.error(f"Reload: Error managing {cog_name_chat}: {e_ext}")
            except Exception as e_reload_chat_generic:
                 config_manager.logger.error(f"Reload: Unexpected error managing {cog_name_chat}: {e_reload_chat_generic}", exc_info=True)


        config_manager.config_data = new_loaded_data_dict
        config_manager.apply_config_globally(config_manager.config_data)

        diff = get_config_diff(old_config_data_copy, config_manager.config_data)
        diff_summary = "\n".join([f"**'{k}'**: `{v['old']}` -> `{v['new']}`" for k,v in diff.items()]) if diff else "No changes detected."
        config_manager.logger.info(f"Configuration reload diff:\n{diff_summary.replace('**', '')}")

        if 'DISCORD_TOKEN' in diff:
            config_manager.logger.warning("DISCORD_TOKEN changed. Full bot restart by user is required for this to take effect.")
            await ctx.send("⚠️ **DISCORD_TOKEN changed!** A full manual bot restart (stop and start the script) is required for this to take effect.")

        if 'DISCORD_BOT_OWNER_ID' in diff and self.bot:
            new_owner_id_str = config_manager.owner_id_from_config
            if new_owner_id_str:
                try:
                    self.bot.owner_id = int(new_owner_id_str)
                    config_manager.logger.info(f"Bot owner ID updated to: {self.bot.owner_id}")
                except ValueError:
                    config_manager.logger.error(f"Invalid DISCORD_BOT_OWNER_ID in new config: {new_owner_id_str}")
                    self.bot.owner_id = None
            else:
                self.bot.owner_id = None
                config_manager.logger.info("Bot owner ID removed from config.")

        if 'FCTD_COMMAND_PREFIX' in diff and self.bot:
            self.bot.command_prefix = config_manager.FCTD_COMMAND_PREFIX
            config_manager.logger.info(f"Bot command prefix updated to: {config_manager.FCTD_COMMAND_PREFIX}")

        if 'TWITCH_CLIENT_ID' in diff or 'TWITCH_CLIENT_SECRET' in diff:
            config_manager.logger.info("Twitch client ID/secret changed. Re-initializing fctd.TwitchAPI and clearing UTA Twitch token.")
            config_manager.uta_shared_access_token = None
            config_manager.uta_token_expiry_time = 0
            if config_manager.FCTD_TWITCH_USERNAME and config_manager.fctd_twitch_api:
                config_manager.fctd_current_twitch_user_id = await config_manager.fctd_twitch_api.get_user_id(config_manager.FCTD_TWITCH_USERNAME)
                config_manager.logger.info(f"Reload: Re-fetched fctd_current_twitch_user_id: {config_manager.fctd_current_twitch_user_id}")

        if 'FCTD_TWITCH_USERNAME' in diff:
            config_manager.logger.info(f"FCTD_TWITCH_USERNAME changed. Updating fctd_current_twitch_user_id.")
            if config_manager.FCTD_TWITCH_USERNAME and config_manager.fctd_twitch_api:
                config_manager.fctd_current_twitch_user_id = await config_manager.fctd_twitch_api.get_user_id(config_manager.FCTD_TWITCH_USERNAME)
                config_manager.logger.info(f"New fctd_current_twitch_user_id: {config_manager.fctd_current_twitch_user_id}")
            else:
                config_manager.fctd_current_twitch_user_id = None
                config_manager.logger.info("FCTD_TWITCH_USERNAME removed. User ID set to None.")

        if update_channel_name_and_log_followers.is_running():
            if update_channel_name_and_log_followers.minutes != config_manager.FCTD_UPDATE_INTERVAL_MINUTES:
                try:
                    update_channel_name_and_log_followers.change_interval(minutes=config_manager.FCTD_UPDATE_INTERVAL_MINUTES)
                    config_manager.logger.info(f"Follower update task interval changed to {config_manager.FCTD_UPDATE_INTERVAL_MINUTES} minutes.")
                except Exception as e:
                    config_manager.logger.error(f"Error changing follower task interval post-reload: {e}")

        should_run_fctd_task = bool(
            config_manager.FCTD_TWITCH_USERNAME and \
            config_manager.fctd_current_twitch_user_id and \
            (config_manager.FCTD_TARGET_CHANNEL_ID or config_manager.FCTD_FOLLOWER_DATA_FILE)
        )
        if should_run_fctd_task and not update_channel_name_and_log_followers.is_running():
            config_manager.logger.info("Reload: Starting follower update task due to config changes.")
            update_channel_name_and_log_followers.start()
        elif not should_run_fctd_task and update_channel_name_and_log_followers.is_running():
            config_manager.logger.info("Reload: Stopping follower update task due to config changes.")
            update_channel_name_and_log_followers.cancel()

        new_yt_api_enabled_check = config_manager.effective_youtube_api_enabled() 
        old_yt_api_enabled_check = old_config_data_copy.get('UTA_YOUTUBE_API_ENABLED', False) and config_manager.GOOGLE_API_AVAILABLE

        if new_yt_api_enabled_check and \
           (old_yt_api_enabled_check != new_yt_api_enabled_check or \
            old_config_data_copy.get('UTA_YOUTUBE_CLIENT_SECRET_FILE') != config_manager.UTA_YOUTUBE_CLIENT_SECRET_FILE or \
            old_config_data_copy.get('UTA_YOUTUBE_TOKEN_FILE') != config_manager.UTA_YOUTUBE_TOKEN_FILE):
            config_manager.logger.info("Reload: YouTube API configuration changed. Service re-initialization may be needed by UTA threads.")
            config_manager.uta_yt_service = None

        if needs_uta_thread_restart and new_uta_enabled_overall:
            config_manager.logger.info("Reload: UTA is active and its config necessitates a thread (re)start for UTA services (Clip/Restream/Status).")
            start_all_services(self.bot)
        elif not new_uta_enabled_overall and old_uta_enabled_overall:
            config_manager.logger.info("Reload: UTA is now disabled. UTA service threads were already stopped (or stop_all_services handled it if running).")

        final_message = f"Configuration reloaded successfully.\n**Changes:**\n{diff_summary if len(diff_summary) < 1800 else 'Too many changes to display, see logs.'}"
        await ctx.send(final_message)


    @commands.command(name="readdata", help="Dumps raw data. Keys: followers, viewers, durations, activity, sessions, chat. Owner only.")
    @commands.is_owner()
    async def read_data_command(self, ctx: commands.Context, filename_key: str = "followers", max_records_str: str = "50"):
        filepath_to_read = None
        record_format_expected = BINARY_RECORD_FORMAT
        record_size_expected = BINARY_RECORD_SIZE
        is_duration_file, is_activity_file, is_bot_session_file, is_chat_activity_file = False, False, False, False
        data_type_name = "Unknown"

        filename_key_lower = filename_key.lower()
        if filename_key_lower in ["followers", "foll"]:
            filepath_to_read = config_manager.FCTD_FOLLOWER_DATA_FILE
            record_format_expected = BINARY_RECORD_FORMAT
            record_size_expected = BINARY_RECORD_SIZE
            data_type_name = "Follower"
        elif filename_key_lower in ["viewers", "views"]:
            filepath_to_read = config_manager.UTA_VIEWER_COUNT_LOG_FILE
            record_format_expected = BINARY_RECORD_FORMAT
            record_size_expected = BINARY_RECORD_SIZE
            data_type_name = "Viewer Count"
        elif filename_key_lower in ["durations", "streamdurations"]:
            filepath_to_read = config_manager.UTA_STREAM_DURATION_LOG_FILE
            record_format_expected = STREAM_DURATION_RECORD_FORMAT
            record_size_expected = STREAM_DURATION_RECORD_SIZE
            is_duration_file = True
            data_type_name = "Stream Duration"
        elif filename_key_lower in ["activity", "streamactivity"]:
            filepath_to_read = config_manager.UTA_STREAM_ACTIVITY_LOG_FILE
            is_activity_file = True
            record_size_expected = SA_BASE_HEADER_SIZE 
            data_type_name = "Stream Activity"
        elif filename_key_lower in ["sessions", "botsessions"]:
            filepath_to_read = config_manager.BOT_SESSION_LOG_FILE_PATH
            record_format_expected = BOT_SESSION_RECORD_FORMAT
            record_size_expected = BOT_SESSION_RECORD_SIZE
            is_bot_session_file = True
            data_type_name = "Bot Session"
        elif filename_key_lower in ["chat", "chatactivity"]:
            filepath_to_read = config_manager.TWITCH_CHAT_ACTIVITY_LOG_FILE
            record_format_expected = CHAT_ACTIVITY_RECORD_FORMAT
            record_size_expected = CHAT_ACTIVITY_RECORD_SIZE
            is_chat_activity_file = True
            data_type_name = "Chat Activity"
        else:
            await ctx.send(f"Unknown data file key '{filename_key}'. Use 'followers', 'viewers', 'durations', 'activity', 'sessions', or 'chat'.")
            return

        if not filepath_to_read:
            await ctx.send(f"{data_type_name} data file not configured in config.json.")
            return

        try:
            max_r = min(max(1, int(max_records_str)), 200) 
        except ValueError:
            max_r = 50
            await ctx.send("Invalid number for max_records, using default of 50.")

        async with ctx.typing():
            lines_to_send = []
            basename_of_file = os.path.basename(filepath_to_read)
            
            if not os.path.exists(filepath_to_read):
                await ctx.send(f"```Error: File '{filepath_to_read}' not found.```")
                return
            if os.path.getsize(filepath_to_read) == 0:
                await ctx.send(f"```File '{filepath_to_read}' is empty.```")
                return
            if os.path.getsize(filepath_to_read) < record_size_expected:
                 await ctx.send(f"```File '{basename_of_file}' is too small ({os.path.getsize(filepath_to_read)}B) to contain even one record/header (expected min {record_size_expected}B).```")
                 return

            lines_to_send.append(f"Reading up to {max_r} records from: {basename_of_file}")
            if is_activity_file:
                lines_to_send.append(f"Format: EventType(Byte), Timestamp(Int), then event-specific data.")
            elif is_bot_session_file:
                lines_to_send.append(f"Record size: {record_size_expected}B. Format: EventType(Byte), Timestamp(Int).")
            elif is_duration_file:
                lines_to_send.append(f"Record size: {record_size_expected}B. Format: Start_Timestamp(Int), End_Timestamp(Int).")
            elif is_chat_activity_file: 
                lines_to_send.append(f"Record size: {record_size_expected}B. Format: Timestamp(Int), MsgCount(Short), UniqueChatters(Short).")
            else: 
                lines_to_send.append(f"Record size: {record_size_expected}B. Format: Timestamp(Int), {data_type_name} Count(Int).")
            lines_to_send.append("-" * 20)

            read_count = 0
            displayed_count = 0
            try:
                with open(filepath_to_read, 'rb') as f:
                    file_total_size = os.fstat(f.fileno()).st_size
                    while displayed_count < max_r:
                        current_event_start_offset = f.tell()
                        if is_activity_file:
                            if current_event_start_offset + SA_BASE_HEADER_SIZE > file_total_size: break 
                            header_chunk = f.read(SA_BASE_HEADER_SIZE)
                            if not header_chunk: break 
                            read_count += 1
                            event_type, unix_ts = struct.unpack(SA_BASE_HEADER_FORMAT, header_chunk)
                            dt_obj = datetime.fromtimestamp(unix_ts, tz=timezone.utc)
                            line_prefix = f"{dt_obj.isoformat()} ({unix_ts}) | Evt: {event_type} "
                            event_desc = ""
                            incomplete_body_flag = False
                            try:
                                if event_type == EVENT_TYPE_STREAM_START:
                                    title, i1 = read_string_from_file_handle(f)
                                    game, i2 = read_string_from_file_handle(f)
                                    tags, i3 = read_tag_list_from_file_handle(f)
                                    yt_id_str_part = ""
                                    yt_id_inc = False 
                                    pos_before_yt = f.tell()
                                    if file_total_size - pos_before_yt >= SA_STRING_LEN_SIZE:
                                        peek_len_bytes = f.read(SA_STRING_LEN_SIZE)
                                        peek_s_len = struct.unpack(SA_STRING_LEN_FORMAT, peek_len_bytes)[0]
                                        f.seek(pos_before_yt) 
                                        if file_total_size - pos_before_yt >= SA_STRING_LEN_SIZE + peek_s_len :
                                            yt_id, yt_id_inc_attempt = read_string_from_file_handle(f)
                                            if not yt_id_inc_attempt: 
                                                yt_id_str_part = f" | YT_ID: '{yt_id}'" if yt_id else " | YT_ID: (empty)"
                                            yt_id_inc = yt_id_inc_attempt
                                    if i1 or i2 or i3 or yt_id_inc: 
                                        incomplete_body_flag = True; event_desc = "INCOMPLETE START"
                                    else:
                                        event_desc = f"(START) | T: '{title}' | G: '{game}' | Tags: {tags if tags else '[]'}{yt_id_str_part}"

                                elif event_type == EVENT_TYPE_STREAM_END:
                                    duration_peak_bytes = f.read(SA_INT_SIZE * 2)
                                    if len(duration_peak_bytes) < SA_INT_SIZE * 2: incomplete_body_flag=True; event_desc="INCOMPLETE END"
                                    else: dur_val, peak_val = struct.unpack(f'>{SA_INT_FORMAT[1:]}{SA_INT_FORMAT[1:]}', duration_peak_bytes); event_desc = f"(END) | Dur: {format_duration_human(dur_val)} | PeakV: {peak_val}"
                                elif event_type == EVENT_TYPE_GAME_CHANGE: old_g,i1=read_string_from_file_handle(f);new_g,i2=read_string_from_file_handle(f); event_desc = f"(GAME_CHG) | From: '{old_g}' To: '{new_g}'" if not (i1 or i2) else "INCOMPLETE GAME_CHG"; incomplete_body_flag = i1 or i2
                                elif event_type == EVENT_TYPE_TITLE_CHANGE: old_t,i1=read_string_from_file_handle(f);new_t,i2=read_string_from_file_handle(f); event_desc = f"(TITLE_CHG) | From: '{old_t}' To: '{new_t}'" if not (i1 or i2) else "INCOMPLETE TITLE_CHG"; incomplete_body_flag = i1 or i2
                                elif event_type == EVENT_TYPE_TAGS_CHANGE: old_tags,i1=read_tag_list_from_file_handle(f);new_tags,i2=read_tag_list_from_file_handle(f); event_desc = f"(TAGS_CHG) | Old: {old_tags} New: {new_tags}" if not (i1 or i2) else "INCOMPLETE TAGS_CHG"; incomplete_body_flag = i1 or i2
                                else: event_desc = f"Unknown Event Type ({event_type})"; incomplete_body_flag = consume_activity_event_body(f, event_type)
                                
                                lines_to_send.append(f"{line_prefix}{event_desc}")
                                if incomplete_body_flag:
                                    f.seek(current_event_start_offset); 
                                    config_manager.logger.warning(f"Incomplete activity event type {event_type} at offset {current_event_start_offset}. Stopping file read for readdata."); break 
                            except struct.error as se_inner: lines_to_send.append(f"{line_prefix}Struct error in body: {se_inner}"); f.seek(current_event_start_offset); break
                            except Exception as e_inner: lines_to_send.append(f"{line_prefix}Generic error in body: {e_inner}"); f.seek(current_event_start_offset); break

                        elif is_bot_session_file:
                            if current_event_start_offset + BOT_SESSION_RECORD_SIZE > file_total_size: break
                            chunk = f.read(BOT_SESSION_RECORD_SIZE)
                            if not chunk: break
                            read_count += 1
                            event_type, unix_ts = struct.unpack(BOT_SESSION_RECORD_FORMAT, chunk)
                            dt_obj = datetime.fromtimestamp(unix_ts, tz=timezone.utc)
                            event_name_str = "START" if event_type == BOT_EVENT_START else "STOP" if event_type == BOT_EVENT_STOP else f"Unknown ({event_type})"
                            lines_to_send.append(f"{dt_obj.isoformat()} ({unix_ts}) | Bot Event: {event_name_str}")
                        elif is_chat_activity_file: 
                            if current_event_start_offset + CHAT_ACTIVITY_RECORD_SIZE > file_total_size: break
                            chunk = f.read(CHAT_ACTIVITY_RECORD_SIZE)
                            if not chunk: break
                            read_count += 1
                            unix_ts, msg_count, unique_c_count = struct.unpack(CHAT_ACTIVITY_RECORD_FORMAT, chunk)
                            dt_obj = datetime.fromtimestamp(unix_ts, tz=timezone.utc)
                            lines_to_send.append(f"{dt_obj.isoformat()} ({unix_ts}) | Msgs: {msg_count}, Unique: {unique_c_count}")
                        else: 
                            if current_event_start_offset + record_size_expected > file_total_size: break
                            chunk = f.read(record_size_expected)
                            if not chunk: break
                            read_count += 1
                            if is_duration_file:
                                start_ts, end_ts = struct.unpack(record_format_expected, chunk)
                                s_dt = datetime.fromtimestamp(start_ts, tz=timezone.utc)
                                e_dt = datetime.fromtimestamp(end_ts, tz=timezone.utc)
                                lines_to_send.append(f"Start: {s_dt.isoformat()} ({start_ts}) | End: {e_dt.isoformat()} ({end_ts}) | Dur: {format_duration_human(end_ts - start_ts)}")
                            else: 
                                unix_ts, count_val = struct.unpack(record_format_expected, chunk)
                                dt_obj = datetime.fromtimestamp(unix_ts, tz=timezone.utc)
                                lines_to_send.append(f"{dt_obj.isoformat()} ({unix_ts}) | {data_type_name}s: {count_val}")
                        displayed_count += 1
                
                total_possible_records_approx = file_total_size // record_size_expected if record_size_expected > 0 and not is_activity_file else 0
                if not is_activity_file: 
                    if displayed_count < read_count or (total_possible_records_approx > 0 and read_count < total_possible_records_approx):
                         lines_to_send.append(f"\nDisplayed {displayed_count} of {read_count} records processed from file.")
                         if total_possible_records_approx > 0 and read_count < total_possible_records_approx:
                             lines_to_send.append(f"(File has approximately {total_possible_records_approx} potential full records if format is consistent).")
                else: 
                    lines_to_send.append(f"\nDisplayed {displayed_count} of {read_count} {data_type_name.lower()} log events processed.")
                lines_to_send.append(f"\nTotal records/events displayed in this message: {displayed_count}.")

            except FileNotFoundError: 
                await ctx.send(f"```Error: File '{filepath_to_read}' not found during read.```")
                return
            except struct.error as se:
                config_manager.logger.error(f"Struct unpacking error during readdata for {basename_of_file}: {se}", exc_info=True)
                lines_to_send.append(f"\nError: Struct unpack failed. ({se}). Possible file corruption or format mismatch.")
            except Exception as e_main:
                config_manager.logger.error(f"General error processing readdata for {basename_of_file}: {e_main}", exc_info=True)
                lines_to_send.append(f"\nError: {str(e_main)}")

            current_chunk = ""
            for line_item in lines_to_send:
                if len(current_chunk) + len(line_item) + 1 > 1990: 
                    if current_chunk.strip(): 
                        await ctx.send(f"```\n{current_chunk.strip()}\n```")
                    current_chunk = line_item + "\n" 
                else:
                    current_chunk += line_item + "\n"
            
            if current_chunk.strip(): 
                await ctx.send(f"```\n{current_chunk.strip()}\n```")

    @commands.command(name="utastatus", help="Shows status of UTA modules. (Bot owner only)")
    @commands.is_owner()
    async def uta_status_command(self, ctx: commands.Context):
        embed = discord.Embed(title="Bot & UTA Module Status", color=discord.Color.orange())
        uptime_delta = datetime.now(timezone.utc) - config_manager.bot_start_time
        human_uptime = format_duration_human(int(uptime_delta.total_seconds()))
        embed.add_field(name="Bot Uptime (Current Session)", value=f"{human_uptime} (Since: {discord.utils.format_dt(config_manager.bot_start_time, 'F')})", inline=False)

        if not config_manager.UTA_ENABLED:
            embed.add_field(name="UTA Status", value="UTA module disabled in config.", inline=False)
            chat_mon_status_part = "Disabled in Config (UTA Disabled or Chat Monitor Disabled)"
            if config_manager.TWITCH_CHAT_ENABLED:
                chat_cog = self.bot.get_cog("Twitch Chat Monitor")
                if chat_cog and hasattr(chat_cog, 'is_connected_to_twitch_chat') and chat_cog.is_connected_to_twitch_chat:
                    chat_mon_status_part = "Enabled & Connected to Twitch Chat."
                elif chat_cog :
                    chat_mon_status_part = "Enabled but NOT connected to Twitch Chat (Check logs)."
                elif COGS_TWITCHIO_MODULE_AVAILABLE: # TwitchIO lib is there, but cog might not have loaded
                     chat_mon_status_part = f"Enabled in config, but Cog not loaded/connected (Check startup logs for TwitchChatCog)."
                else: # TwitchIO lib missing
                     chat_mon_status_part = f"Enabled in config, but Cog not loaded (TwitchIO missing!)."
            embed.add_field(name="Twitch Chat Monitor", value=chat_mon_status_part, inline=False)
            await ctx.send(embed=embed)
            return

        embed.add_field(name="UTA Enabled", value=str(config_manager.UTA_ENABLED), inline=False)
        embed.add_field(name="Target Twitch Channel (UTA)", value=config_manager.UTA_TWITCH_CHANNEL_NAME or "Not Set", inline=False)

        chat_mon_status_val = "Disabled in Config"
        if config_manager.TWITCH_CHAT_ENABLED:
            chat_cog = self.bot.get_cog("Twitch Chat Monitor") 
            if chat_cog and hasattr(chat_cog, 'is_connected_to_twitch_chat') and chat_cog.is_connected_to_twitch_chat:
                bot_nick = "N/A"
                if hasattr(chat_cog, 'twitch_irc_bot_instance') and chat_cog.twitch_irc_bot_instance and hasattr(chat_cog.twitch_irc_bot_instance, 'nick'):
                    bot_nick = chat_cog.twitch_irc_bot_instance.nick
                chat_mon_status_val = (f"Enabled. Connected to Twitch Chat as `{bot_nick}`.\n"
                                       f"  Mirroring to Discord: {'Enabled' if config_manager.DISCORD_TWITCH_CHAT_MIRROR_ENABLED else 'Disabled'}\n"
                                       f"  Activity Log: `{config_manager.TWITCH_CHAT_ACTIVITY_LOG_FILE}`")
            elif chat_cog:
                chat_mon_status_val = "Enabled, but NOT connected to Twitch Chat (Check logs)."
            elif COGS_TWITCHIO_MODULE_AVAILABLE :
                chat_mon_status_val = f"Enabled in config, but Cog not loaded/connected (Check startup logs for TwitchChatCog)."
            else:
                chat_mon_status_val = f"Enabled in config, but Cog not loaded (TwitchIO missing!)."
        embed.add_field(name="Twitch Chat Monitor", value=chat_mon_status_val, inline=False)


        clip_status = "Disabled in Config"
        if config_manager.UTA_CLIP_MONITOR_ENABLED:
            clip_thread_status = "Active" if config_manager._are_uta_threads_active else "Not Active (or main UTA threads off)" 
            clip_status = f"Enabled. Thread Group: {clip_thread_status}."
        embed.add_field(name="Clip Monitor", value=clip_status, inline=False)

        restream_status_parts = []
        if config_manager.UTA_RESTREAMER_ENABLED:
            restream_status_parts.append("Enabled")
            restream_thread_status = "Active" if config_manager._are_uta_threads_active else "Not Active (or main UTA threads off)"
            restream_status_parts.append(f"Thread Group: {restream_thread_status}")
            restream_status_parts.append(f"Currently Restreaming (Pipe Active): {config_manager.uta_is_restreaming_active}")

            if config_manager.uta_is_restreaming_active:
                if config_manager.UTA_FFMPEG_PID: restream_status_parts.append(f"  FFmpeg PID: `{config_manager.UTA_FFMPEG_PID}`")
                if config_manager.UTA_STREAMLINK_PID: restream_status_parts.append(f"  Streamlink PID: `{config_manager.UTA_STREAMLINK_PID}`")
                if config_manager.UTA_PIPE_START_TIME_UTC:
                    pipe_uptime_delta = datetime.now(timezone.utc) - config_manager.UTA_PIPE_START_TIME_UTC
                    pipe_uptime_str = format_duration_human(int(pipe_uptime_delta.total_seconds()))
                    restream_status_parts.append(f"  Current Pipe Uptime: {pipe_uptime_str}")

            if config_manager.effective_youtube_api_enabled():
                restream_status_parts.append("  Mode: YouTube API")
                if config_manager.uta_yt_service:
                    restream_status_parts.append("    Service: Initialized")
                    if config_manager.UTA_CURRENT_YT_BROADCAST_ID_FOR_LOGGING:
                        restream_status_parts.append(f"    Current YT Broadcast ID: `{config_manager.UTA_CURRENT_YT_BROADCAST_ID_FOR_LOGGING}` (Part {config_manager.uta_current_restream_part_number})")
                    if config_manager.uta_youtube_next_rollover_time_utc:
                        restream_status_parts.append(f"    Next Rollover: {discord.utils.format_dt(config_manager.uta_youtube_next_rollover_time_utc, 'R')}")
                else:
                    restream_status_parts.append("    Service: NOT Initialized (or failed)")
            elif config_manager.UTA_YOUTUBE_API_ENABLED and not config_manager.GOOGLE_API_AVAILABLE:
                restream_status_parts.append("  Mode: YouTube API (Google Libs Missing!)")
            else:
                restream_status_parts.append("  Mode: Legacy RTMP")

            restream_status_parts.append(f"  Consecutive Pipe Failures: {config_manager.UTA_RESTREAM_CONSECUTIVE_FAILURES}/{config_manager.UTA_RESTREAM_MAX_CONSECUTIVE_FAILURES}")
            if config_manager.UTA_YOUTUBE_PLAYABILITY_CHECK_ENABLED:
                restream_status_parts.append(f"  Last YT Playability Check: {config_manager.UTA_LAST_PLAYABILITY_CHECK_STATUS}")
        else:
            restream_status_parts.append("Disabled in Config")
        embed.add_field(name="Restreamer", value="\n".join(restream_status_parts), inline=False)

        status_mon_text_parts = []
        if config_manager.UTA_STREAM_STATUS_NOTIFICATIONS_ENABLED:
            status_mon_text_parts.append("Enabled")
            status_thread_active = "Active" if config_manager._are_uta_threads_active else "Not Active (or main UTA threads off)"
            status_mon_text_parts.append(f"Thread Group: {status_thread_active}")
            if config_manager.UTA_VIEWER_COUNT_LOGGING_ENABLED:
                status_mon_text_parts.append(f"  Viewer Logging: Enabled (Interval: {config_manager.UTA_VIEWER_COUNT_LOG_INTERVAL_SECONDS}s)")
            else:
                status_mon_text_parts.append("  Viewer Logging: Disabled")
            status_mon_text_parts.append(f"  Activity Log File: `{config_manager.UTA_STREAM_ACTIVITY_LOG_FILE}`")
        else:
            status_mon_text_parts.append("Disabled in Config")
        embed.add_field(name="Stream Status Monitor & Activity Logger", value="\n".join(status_mon_text_parts), inline=False)

        token_status = "No Token or Error"
        if config_manager.uta_shared_access_token and config_manager.uta_token_expiry_time > 0:
            expiry_dt = datetime.fromtimestamp(config_manager.uta_token_expiry_time, tz=timezone.utc)
            token_status = f"Token Acquired. Expires: {discord.utils.format_dt(expiry_dt, 'R')} ({discord.utils.format_dt(expiry_dt, 'f')})"
        elif config_manager.uta_token_expiry_time == 0 and not config_manager.uta_shared_access_token: 
            token_status = "Failed to acquire token or token expired and failed refresh."
        embed.add_field(name="UTA Twitch API Token", value=token_status, inline=False)

        if config_manager.BOT_SESSION_LOG_FILE_PATH:
            embed.add_field(name="Bot Session Log", value=f"Enabled (`{os.path.basename(config_manager.BOT_SESSION_LOG_FILE_PATH)}`)", inline=False)
        else:
            embed.add_field(name="Bot Session Log", value="Disabled or not configured", inline=False)

        await ctx.send(embed=embed)


    @commands.command(name="utarestartffmpeg", help="Requests UTA to restart FFmpeg/Streamlink pipe. Owner only.")
    @commands.is_owner()
    async def uta_restart_ffmpeg_command(self, ctx: commands.Context):
        if not (config_manager.UTA_ENABLED and config_manager.UTA_RESTREAMER_ENABLED):
            await ctx.send("UTA Restreamer is not enabled in the configuration.")
            return
        if not config_manager.twitch_session_active_global: 
            await ctx.send("Cannot restart FFmpeg/Streamlink: No active Twitch session is currently being restreamed by UTA (according to StatusService).")
            return

        config_manager.UTA_MANUAL_FFMPEG_RESTART_REQUESTED = True
        config_manager.logger.info(f"Discord command: Manual FFmpeg/Streamlink restart requested by {ctx.author}.")
        await ctx.send("Request to restart FFmpeg/Streamlink pipe has been sent. It will be processed by the restreamer loop shortly if a restream is active.")

    @commands.command(name="utastartnewpart", help="Requests UTA to start a new YouTube broadcast part (API mode only). Owner only.")
    @commands.is_owner()
    async def uta_start_new_part_command(self, ctx: commands.Context):
        if not (config_manager.UTA_ENABLED and config_manager.UTA_RESTREAMER_ENABLED and config_manager.effective_youtube_api_enabled()):
            await ctx.send("UTA Restreamer with YouTube API mode is not active or not configured correctly.")
            return
        if not config_manager.twitch_session_active_global: 
            await ctx.send("Cannot start a new YouTube part: No active Twitch session is currently being restreamed by UTA (according to StatusService).")
            return
        if not config_manager.youtube_api_session_active_global: 
             await ctx.send("Cannot start a new YouTube part: No active YouTube API broadcast part is reported by RestreamService. This command is intended for streams managed via the YouTube API.")
             return

        config_manager.UTA_MANUAL_NEW_PART_REQUESTED = True
        config_manager.logger.info(f"Discord command: Manual new YouTube broadcast part requested by {ctx.author}.")
        await ctx.send("Request to start a new YouTube broadcast part has been sent. It will be processed by the restreamer loop shortly if applicable.")

    @commands.command(name="utaytstatus", help="Shows current YouTube restream status (API mode). Owner only.")
    @commands.is_owner()
    async def uta_yt_status_command(self, ctx: commands.Context):
        if not (config_manager.UTA_ENABLED and config_manager.UTA_RESTREAMER_ENABLED and config_manager.effective_youtube_api_enabled()):
            await ctx.send("YouTube API restreaming is not active or not configured correctly.")
            return
        if not config_manager.twitch_session_active_global: 
            await ctx.send("Not currently in an active Twitch restream session according to UTA's StatusService.")
            return
        if not config_manager.youtube_api_session_active_global: 
            await ctx.send("Currently in a Twitch session, but no active YouTube API broadcast part is reported by RestreamService (possibly using legacy RTMP or an API error occurred).")
            return

        if config_manager.UTA_CURRENT_YT_VIDEO_ID_FOR_LOGGING: 
            embed = discord.Embed(title="UTA YouTube Restream Status (API Mode)", color=discord.Color.blue())
            embed.add_field(name="Target Twitch Channel", value=config_manager.UTA_TWITCH_CHANNEL_NAME or "N/A", inline=False)
            embed.add_field(name="Current YouTube Video ID", value=f"`{config_manager.UTA_CURRENT_YT_VIDEO_ID_FOR_LOGGING}`\n[Watch Link](https://www.youtube.com/watch?v={config_manager.UTA_CURRENT_YT_VIDEO_ID_FOR_LOGGING})", inline=False)
            embed.add_field(name="Current Broadcast ID (for API)", value=f"`{config_manager.UTA_CURRENT_YT_BROADCAST_ID_FOR_LOGGING}`", inline=False)
            embed.add_field(name="Current Part Number", value=str(config_manager.uta_current_restream_part_number), inline=True)
            embed.add_field(name="Bound LiveStream ID", value=f"`{config_manager.uta_current_youtube_live_stream_id or 'N/A'}`", inline=True)

            if config_manager.uta_youtube_next_rollover_time_utc:
                embed.add_field(name="Next Scheduled Rollover", value=discord.utils.format_dt(config_manager.uta_youtube_next_rollover_time_utc, 'F') + f" ({discord.utils.format_dt(config_manager.uta_youtube_next_rollover_time_utc, 'R')})", inline=False)
            else:
                embed.add_field(name="Scheduled Rollover", value="Disabled or not applicable for current part", inline=False)

            embed.set_footer(text="This status reflects the current YouTube 'part' of the ongoing Twitch stream.")
            await ctx.send(embed=embed)
        else:
            await ctx.send("Twitch session is active, but no active YouTube video/broadcast ID found for the current session (API mode).")

    async def _run_test(self, test_name: str, test_func, *args):
        start_time = time.monotonic()
        try:
            result, details = await test_func(*args)
            duration = (time.monotonic() - start_time) * 1000
            status_emoji = "✅" if result else "❌"
            return f"{status_emoji} **{test_name}**: {'Pass' if result else 'Fail'} ({duration:.0f}ms)\n   └── {details}"
        except Exception as e:
            duration = (time.monotonic() - start_time) * 1000
            config_manager.logger.error(f"Error in test '{test_name}': {e}", exc_info=True)
            return f"⚠️ **{test_name}**: Error ({duration:.0f}ms)\n   └── Exception: {str(e)[:200]}"


    async def _test_config_loading(self):
        if config_manager.config_data and config_manager.DISCORD_TOKEN:
            return True, f"Config loaded. Token starts with: {config_manager.DISCORD_TOKEN[:5]}..."
        return False, "Config data seems empty or DISCORD_TOKEN is missing."

    async def _test_fctd_twitch_api_init(self):
        if config_manager.fctd_twitch_api:
            return True, "FCTD TwitchAPIHelper instance exists."
        return False, "FCTD TwitchAPIHelper instance is None. Check client_id/secret."

    async def _test_fctd_get_user_id(self):
        if not config_manager.fctd_twitch_api or not config_manager.FCTD_TWITCH_USERNAME:
            return False, "FCTD API or username not configured."
        user_id = await config_manager.fctd_twitch_api.get_user_id(config_manager.FCTD_TWITCH_USERNAME)
        if user_id:
            return True, f"Successfully fetched User ID for {config_manager.FCTD_TWITCH_USERNAME}: {user_id}"
        return False, f"Failed to fetch User ID for {config_manager.FCTD_TWITCH_USERNAME}."

    async def _test_follower_data_file_read(self):
        if not config_manager.FCTD_FOLLOWER_DATA_FILE or not os.path.exists(config_manager.FCTD_FOLLOWER_DATA_FILE):
            return False, f"Follower data file '{config_manager.FCTD_FOLLOWER_DATA_FILE}' not found or not configured."
        try:
            _, _, _, _, records = await asyncio.to_thread(
                read_and_find_records_for_period, config_manager.FCTD_FOLLOWER_DATA_FILE, 0, int(time.time())
            )
            if records is None:
                 return False, f"Reading follower file returned None (empty or error)."
            return True, f"Follower data file readable. Found {len(records)} records (full scan)."
        except Exception as e:
            return False, f"Error reading follower data file: {str(e)[:100]}"

    async def _test_uta_twitch_token(self):
        if not config_manager.UTA_ENABLED: return True, "UTA disabled, skipping."
        token = get_uta_twitch_access_token()
        if token:
            return True, f"UTA Twitch token acquired. Expires: {datetime.fromtimestamp(config_manager.uta_token_expiry_time, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}"
        return False, "Failed to acquire UTA Twitch token."

    async def _test_uta_get_broadcaster_id(self):
        if not config_manager.UTA_ENABLED or not config_manager.UTA_TWITCH_CHANNEL_NAME:
            return True, "UTA or target channel not configured, skipping."
        original_cache = config_manager.uta_broadcaster_id_cache
        config_manager.uta_broadcaster_id_cache = None 
        b_id = get_uta_broadcaster_id(config_manager.UTA_TWITCH_CHANNEL_NAME)
        config_manager.uta_broadcaster_id_cache = original_cache 
        if b_id:
            return True, f"UTA broadcaster ID for {config_manager.UTA_TWITCH_CHANNEL_NAME}: {b_id}"
        return False, f"Failed to get UTA broadcaster ID for {config_manager.UTA_TWITCH_CHANNEL_NAME}."

    async def _test_youtube_api_service_init(self):
        if not config_manager.UTA_ENABLED or not config_manager.UTA_RESTREAMER_ENABLED or not config_manager.effective_youtube_api_enabled():
            return True, "UTA Restreamer YouTube API mode not configured/enabled, skipping."

        original_service = config_manager.uta_yt_service
        config_manager.uta_yt_service = None 

        service = get_youtube_service(force_reinitialize=True)

        config_manager.uta_yt_service = original_service 

        if service:
            return True, "YouTube API service successfully initialized/re-confirmed."
        elif not os.path.exists(config_manager.UTA_YOUTUBE_TOKEN_FILE) and os.path.exists(config_manager.UTA_YOUTUBE_CLIENT_SECRET_FILE):
            return False, "YouTube API token file missing. Manual OAuth flow likely required via bot startup or re-auth cmd."
        elif not os.path.exists(config_manager.UTA_YOUTUBE_CLIENT_SECRET_FILE):
            return False, "YouTube API client_secret.json missing."
        else:
            return False, "Failed to initialize YouTube API service. Check logs for OAuth errors or token issues."

    async def _test_stream_activity_log_parsing(self):
        if not config_manager.UTA_ENABLED or not config_manager.UTA_STREAM_ACTIVITY_LOG_FILE or not os.path.exists(config_manager.UTA_STREAM_ACTIVITY_LOG_FILE):
            return True, "Stream activity log not configured or found, skipping."
        try:
            segments = await asyncio.to_thread(parse_stream_activity_for_game_segments, config_manager.UTA_STREAM_ACTIVITY_LOG_FILE, 0, int(time.time()))
            return True, f"Stream activity log parsed. Found {len(segments)} game segments in total (full scan)."
        except Exception as e:
            return False, f"Error parsing stream activity log: {str(e)[:100]}"

    async def _test_executable_paths(self):
        if not config_manager.UTA_ENABLED or not config_manager.UTA_RESTREAMER_ENABLED:
            return True, "UTA Restreamer not enabled, skipping executable path checks."

        sl_ok = shutil.which(config_manager.UTA_STREAMLINK_PATH)
        ff_ok = shutil.which(config_manager.UTA_FFMPEG_PATH)

        details = []
        if sl_ok: details.append(f"Streamlink ('{config_manager.UTA_STREAMLINK_PATH}') found at: {sl_ok}")
        else: details.append(f"Streamlink ('{config_manager.UTA_STREAMLINK_PATH}') NOT FOUND.")

        if ff_ok: details.append(f"FFmpeg ('{config_manager.UTA_FFMPEG_PATH}') found at: {ff_ok}")
        else: details.append(f"FFmpeg ('{config_manager.UTA_FFMPEG_PATH}') NOT FOUND.")

        return bool(sl_ok and ff_ok), "\n   ".join(details)

    async def _test_service_thread_status(self): # For UTA services (Clip/Restream/Status)
        if not config_manager.UTA_ENABLED:
            return True, "UTA disabled, skipping UTA service thread checks."

        active_msg = "✅ Active"
        inactive_msg = "❌ Inactive/Not Started"
        disabled_msg = "ℹ️ Disabled in Config"

        details = []
        overall_services_up = config_manager._are_uta_threads_active # From threading_manager

        if config_manager.UTA_CLIP_MONITOR_ENABLED:
            details.append(f"Clip Monitor Service: {active_msg if overall_services_up else inactive_msg}")
        else:
            details.append(f"Clip Monitor Service: {disabled_msg}")

        if config_manager.UTA_RESTREAMER_ENABLED:
            details.append(f"Restreamer Service: {active_msg if overall_services_up else inactive_msg}")
        else:
            details.append(f"Restreamer Service: {disabled_msg}")

        if config_manager.UTA_STREAM_STATUS_NOTIFICATIONS_ENABLED:
            details.append(f"Status Monitor Service: {active_msg if overall_services_up else inactive_msg}")
        else:
            details.append(f"Status Monitor Service: {disabled_msg}")

        all_enabled_services_seem_up = True
        if (config_manager.UTA_CLIP_MONITOR_ENABLED or \
            config_manager.UTA_RESTREAMER_ENABLED or \
            config_manager.UTA_STREAM_STATUS_NOTIFICATIONS_ENABLED) and not overall_services_up:
            all_enabled_services_seem_up = False

        return all_enabled_services_seem_up, "\n   ".join(details)

    async def _test_twitch_chat_cog_status(self):
        if not config_manager.TWITCH_CHAT_ENABLED:
            return True, "Twitch Chat Monitoring disabled in config, skipping."
        if not COGS_TWITCHIO_MODULE_AVAILABLE:
            return False, "Twitch Chat Cog enabled in config, but TwitchIO library NOT FOUND."

        chat_cog = self.bot.get_cog("Twitch Chat Monitor")
        if not chat_cog:
            return False, "Twitch Chat Cog is enabled in config and TwitchIO found, but Cog NOT LOADED (Check for errors during cog loading)."

        if hasattr(chat_cog, 'is_connected_to_twitch_chat') and chat_cog.is_connected_to_twitch_chat:
            bot_nick = "N/A"
            if hasattr(chat_cog, 'twitch_irc_bot_instance') and chat_cog.twitch_irc_bot_instance and hasattr(chat_cog.twitch_irc_bot_instance, 'nick'):
                 bot_nick = chat_cog.twitch_irc_bot_instance.nick
            return True, f"Twitch Chat Cog loaded and CONNECTED to Twitch Chat as '{bot_nick}'."
        else:
            return False, "Twitch Chat Cog loaded but NOT CONNECTED to Twitch Chat (Check logs for connection errors, token validity, etc.)."


    async def _test_background_task_status(self):
        if update_channel_name_and_log_followers.is_running():
            return True, "Follower update background task is running."
        elif config_manager.FCTD_TWITCH_USERNAME and (config_manager.FCTD_TARGET_CHANNEL_ID or config_manager.FCTD_FOLLOWER_DATA_FILE):
            return False, "Follower update background task is configured to run but IS NOT RUNNING."
        else:
            return True, "Follower update background task not configured to run (as expected)."

    async def _test_command_invocation(self, ctx_for_test, command_name, *args):
        command = self.bot.get_command(command_name)
        if not command:
            return False, f"Command '{command_name}' not found."

        try:
            if not await command.can_run(ctx_for_test):
                return True, f"Command '{command_name}' exists but cannot be run by test context (permissions/cog check fail, not necessarily an error for the command itself)."
        except Exception as e_can_run:
            return False, f"Error checking `can_run` for '{command_name}': {str(e_can_run)[:100]}"

        original_command = ctx_for_test.command
        ctx_for_test.command = command
        try:
            if not args and command.clean_params:
                test_args = [None] * len(command.clean_params)
                await command.invoke(ctx_for_test, *test_args)
            else:
                await command.invoke(ctx_for_test, *args)
            ctx_for_test.command = original_command
            return True, f"Command '{command_name}' invoked successfully (or with expected arg errors)."
        except commands.MissingRequiredArgument as mra:
            ctx_for_test.command = original_command
            return True, f"Command '{command_name}' invokable, but MissingRequiredArgument: {mra.param.name} (expected)."
        except commands.CommandError as cmd_err:
            ctx_for_test.command = original_command
            return False, f"Command '{command_name}' errored during invocation: {type(cmd_err).__name__} - {str(cmd_err)[:100]}"
        except Exception as e:
            ctx_for_test.command = original_command
            config_manager.logger.error(f"DeepHealthCheck: Uncaught exception invoking '{command_name}': {e}", exc_info=True)
            return False, f"Command '{command_name}' failed with unhandled exception: {str(e)[:100]}"


    @commands.command(name="deephealthcheck", aliases=['fullhealth', 'diag'], help="Performs a more detailed diagnostic check. Owner only.")
    @commands.is_owner()
    async def deep_health_check_command(self, ctx: commands.Context):
        await ctx.send("🚀 Starting comprehensive health diagnostics... This may take a moment.")

        results = []
        mock_ctx_for_commands = MockContext(self.bot, ctx.author.id, ctx.channel.id)

        async with ctx.typing():
            results.append(await self._run_test("Config File Loading", self._test_config_loading))
            loaded_cogs_list = list(self.bot.cogs.keys())
            results.append(f"ℹ️ **Loaded Discord Cogs**: {', '.join(loaded_cogs_list) if loaded_cogs_list else 'None'}")
            results.append(await self._run_test("FCTD Twitch API Init", self._test_fctd_twitch_api_init))
            results.append(await self._run_test("FCTD Get User ID (configured user)", self._test_fctd_get_user_id))
            results.append(await self._run_test("Follower Data File Read Test", self._test_follower_data_file_read))
            results.append(await self._run_test("Follower Background Task", self._test_background_task_status))
            results.append(await self._run_test("Twitch Chat Cog Status", self._test_twitch_chat_cog_status))


            results.append(await self._run_test("UTA Twitch Token Acquisition", self._test_uta_twitch_token))
            results.append(await self._run_test("UTA Get Broadcaster ID (configured UTA user)", self._test_uta_get_broadcaster_id))
            results.append(await self._run_test("UTA Stream Activity Log Parsing", self._test_stream_activity_log_parsing))

            results.append(await self._run_test("YouTube API Service Init (if enabled)", self._test_youtube_api_service_init))
            results.append(await self._run_test("Executable Paths (Streamlink/FFmpeg)", self._test_executable_paths))
            results.append(await self._run_test("UTA Service Thread Status (Clip/Restream/Status)", self._test_service_thread_status))

            data_file_checks_header = "\n📄 **Data File Status**:"
            results.append(data_file_checks_header)
            log_files_to_check = {
                "Viewer Count Log": config_manager.UTA_VIEWER_COUNT_LOG_FILE if config_manager.UTA_VIEWER_COUNT_LOGGING_ENABLED else None,
                "Stream Duration Log": config_manager.UTA_STREAM_DURATION_LOG_FILE if config_manager.UTA_RESTREAMER_ENABLED else None,
                "Bot Session Log": config_manager.BOT_SESSION_LOG_FILE_PATH,
                "Chat Activity Log": config_manager.TWITCH_CHAT_ACTIVITY_LOG_FILE if config_manager.TWITCH_CHAT_ENABLED else None,
            }
            for name, path in log_files_to_check.items():
                if path:
                    full_log_path = os.path.join(os.getcwd(), path)
                    if os.path.exists(full_log_path) and os.path.getsize(full_log_path) > 0:
                        results.append(f"   ✅ **{name}**: Found and not empty (`{path}`)")
                    elif os.path.exists(full_log_path):
                        results.append(f"   ⚠️ **{name}**: Found but empty (`{path}`)")
                    else:
                        results.append(f"   ❌ **{name}**: Not found (`{path}` at `{full_log_path}`)")
                else:
                    results.append(f"   ℹ️ **{name}**: Not configured/applicable.")

            command_test_header = "\n⚙️ **Command Invocation Tests (Owner Perspective)**:"
            results.append(command_test_header)
            commands_to_test = [
                ("uptime",),
                ("runtime", "7d"),
                ("followers", "1d"),
                ("follrate", "7d"),
                ("daystats", datetime.now(timezone.utc).strftime("%Y-%m-%d")),
                ("streamtime", "24h"),
                ("twitchinfo", config_manager.UTA_TWITCH_CHANNEL_NAME or "twitch"),
                ("gamestats", "Some Game Name", "1w"),
                ("chatstats", "5m"),
                ("utaping",),
                ("plotfollowers", "7d") if config_manager.MATPLOTLIB_AVAILABLE else None,
                ("plotstreamdurations", "all") if config_manager.MATPLOTLIB_AVAILABLE else None,
                ("utastatus",),
                ("utaytstatus",),
            ]
            for cmd_tuple in commands_to_test:
                if cmd_tuple is None: continue
                cmd_name = cmd_tuple[0]
                cmd_args = cmd_tuple[1:]
                results.append(await self._run_test(f"Cmd: !{cmd_name}", self._test_command_invocation, mock_ctx_for_commands, cmd_name, *cmd_args))

        output_message = "📋 **Deep Health Check Results**:\n"
        current_part = ""
        for res_line in results:
            res_line_str = str(res_line)
            if len(output_message + current_part + res_line_str + "\n\n") > 1950:
                await ctx.send(output_message + current_part)
                output_message = ""
                current_part = res_line_str + "\n\n"
            else:
                current_part += res_line_str + "\n\n"

        if current_part:
            await ctx.send(output_message + current_part)

        await ctx.send("✅ Health check diagnostics complete.")


    @commands.command(name="commands", aliases=['help'], help="Lists all available commands.")
    async def list_commands_command(self, ctx: commands.Context):
        embed = discord.Embed(title="Bot Commands", description=f"Prefix: `{config_manager.FCTD_COMMAND_PREFIX}`", color=discord.Color.blue())

        valid_commands = []
        for cmd in self.bot.commands:
            if cmd.hidden:
                continue
            if cmd.cog_name == "Plotting Commands" and not config_manager.MATPLOTLIB_AVAILABLE:
                continue
            if cmd.cog_name == "Twitch Chat Monitor" and (not config_manager.TWITCH_CHAT_ENABLED or not COGS_TWITCHIO_MODULE_AVAILABLE):
                continue
            try:
                if await cmd.can_run(ctx):
                    valid_commands.append(cmd)
            except commands.CheckFailure:
                pass
            except Exception as e:
                config_manager.logger.warning(f"Error checking can_run for command {cmd.name}: {e}")

        sorted_commands = sorted(valid_commands, key=lambda c: c.name)

        for cmd in sorted_commands:
            name_aliases = f"`{config_manager.FCTD_COMMAND_PREFIX}{cmd.name}`"
            if cmd.aliases:
                name_aliases += f" (or {', '.join([f'`{config_manager.FCTD_COMMAND_PREFIX}{a}`' for a in cmd.aliases])})"

            description = cmd.help or "No description available."
            embed.add_field(name=name_aliases, value=description, inline=False)

        if not embed.fields:
            embed.description = "No commands available for you in this context."

        footer_notes = []
        if not config_manager.MATPLOTLIB_AVAILABLE:
            footer_notes.append("Plotting commands hidden (Matplotlib not installed).")
        if not config_manager.TWITCH_CHAT_ENABLED or not COGS_TWITCHIO_MODULE_AVAILABLE:
            footer_notes.append("Chat monitoring commands hidden (Feature disabled or TwitchIO missing).")
        if footer_notes:
            embed.set_footer(text="Note: " + " | ".join(footer_notes))

        await ctx.send(embed=embed)

async def setup(bot_instance):
    await bot_instance.add_cog(AdminCog(bot_instance))
