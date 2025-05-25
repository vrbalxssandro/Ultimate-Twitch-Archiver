import discord
from discord.ext import commands, tasks
import asyncio
from datetime import datetime, timezone
import os 
import struct 

from uta_bot import config_manager
from uta_bot.utils.data_logging import log_chat_activity_binary, read_chat_activity_for_period
from uta_bot.utils.formatters import format_duration_human, parse_duration_to_timedelta
from uta_bot.utils.constants import CHAT_ACTIVITY_RECORD_SIZE, CHAT_ACTIVITY_RECORD_FORMAT, EVENT_TYPE_STREAM_START, EVENT_TYPE_STREAM_END


TWITCHIO_COG_ENABLED = False
try:
    import twitchio
    from twitchio.ext import commands as twitch_commands_ext # Renamed to avoid conflict
    TWITCHIO_COG_ENABLED = True
except ImportError:
    config_manager.logger.warning("TwitchChatCog: TwitchIO library not found during cog import. This cog will be disabled.")

# Helper class for the TwitchIO bot
class TwitchIRCBot(twitchio.Client if TWITCHIO_COG_ENABLED else object): # Conditionally inherit
    def __init__(self, token, initial_channels_list, discord_cog_instance): # Renamed initial_channels to initial_channels_list for clarity
        if not TWITCHIO_COG_ENABLED:
            super().__init__() 
            return
            
        # Store the initial channels list if needed later for logging/reference
        self.intended_initial_channels = initial_channels_list 
        super().__init__(token=token, initial_channels=initial_channels_list)
        self.discord_cog = discord_cog_instance 

    async def event_ready(self):
        """Called once the bot is successfully connected to Twitch chat."""
        self.discord_cog.is_connected_to_twitch_chat = True
        
        # Get the names of the channels the bot is actually connected to
        connected_channel_names = [channel.name for channel in self.connected_channels]

        config_manager.logger.info(f"TwitchChatCog (TwitchIRCBot): Connected to Twitch Chat as {self.nick} in channel(s) #{', '.join(connected_channel_names)}")
        
        # You can still log the intended channels for comparison if you like:
        # config_manager.logger.debug(f"TwitchChatCog (TwitchIRCBot): Intended initial channels were: {', '.join(self.intended_initial_channels)}")

        if self.nick.lower() != config_manager.TWITCH_CHAT_NICKNAME.lower():
            config_manager.logger.warning(f"TwitchChatCog (TwitchIRCBot): Connected with nickname '{self.nick}', but config has TWITCH_CHAT_NICKNAME as '{config_manager.TWITCH_CHAT_NICKNAME}'. The token determines the nickname.")

    async def event_message(self, message: twitchio.Message):
        """Handles incoming messages from Twitch chat."""
        if not self.discord_cog or not config_manager.TWITCH_CHAT_ENABLED:
            return
        if message.echo: 
            return
        
        await self.discord_cog.handle_twitch_message(message)

    async def event_error(self, error: Exception, data: str = None):
        """Handles errors from the TwitchIO client."""
        self.discord_cog.is_connected_to_twitch_chat = False
        config_manager.logger.error(f"TwitchChatCog (TwitchIRCBot): TwitchIO Client Error: {error}")
        if data:
            config_manager.logger.error(f"TwitchChatCog (TwitchIRCBot): TwitchIO Error Data: {data}")
        
        if isinstance(error, twitchio.errors.AuthenticationError):
            config_manager.logger.critical("TwitchChatCog (TwitchIRCBot): TWITCH_CHAT_OAUTH_TOKEN is invalid or lacks permissions. Chat monitoring cannot proceed.")
            self.discord_cog.log_chat_metrics_task.cancel() 


class TwitchChatCog(commands.Cog, name="Twitch Chat Monitor"):
    def __init__(self, discord_bot: commands.Bot): 
        self.discord_bot = discord_bot 
        self.twitch_irc_bot_instance = None 

        if not TWITCHIO_COG_ENABLED:
            config_manager.logger.error("TwitchChatCog disabled because TwitchIO is not installed.")
            return
        
        if not config_manager.TWITCH_CHAT_ENABLED:
            config_manager.logger.info("Twitch Chat Monitoring is disabled in configuration.")
            return

        self.current_interval_message_count = 0
        self.current_interval_unique_chatters = set()
        self.current_interval_start_time_utc = datetime.now(timezone.utc)
        self.is_connected_to_twitch_chat = False 

        self.twitch_irc_bot_instance = TwitchIRCBot(
            token=config_manager.TWITCH_CHAT_OAUTH_TOKEN,
            initial_channels_list=[config_manager.UTA_TWITCH_CHANNEL_NAME], # Pass as list
            discord_cog_instance=self 
        )
        
        self.log_chat_metrics_task.start()

    async def cog_load(self): 
        if not TWITCHIO_COG_ENABLED or not config_manager.TWITCH_CHAT_ENABLED or not self.twitch_irc_bot_instance:
            return
        self.discord_bot.loop.create_task(self.twitch_irc_bot_instance.connect(), name="TwitchIRC_Connect_Task")
        config_manager.logger.info("TwitchChatCog: Queued Twitch IRC bot connection.")

    async def cog_unload(self):
        self.log_chat_metrics_task.cancel()
        if self.twitch_irc_bot_instance and self.is_connected_to_twitch_chat: 
            config_manager.logger.info("TwitchChatCog: Closing Twitch IRC connection...")
            try:
                await self.twitch_irc_bot_instance.close()
                config_manager.logger.info("TwitchChatCog: Twitch IRC connection closed.")
            except Exception as e:
                config_manager.logger.error(f"TwitchChatCog: Error closing Twitch IRC bot: {e}")
        self.is_connected_to_twitch_chat = False


    async def handle_twitch_message(self, message: twitchio.Message):
        """This method is called by the TwitchIRCBot helper class."""
        if not TWITCHIO_COG_ENABLED or not config_manager.TWITCH_CHAT_ENABLED:
            return

        if message.channel and message.channel.name.lower() == config_manager.UTA_TWITCH_CHANNEL_NAME.lower():
            self.current_interval_message_count += 1
            if message.author:
                 self.current_interval_unique_chatters.add(message.author.name)

            if config_manager.DISCORD_TWITCH_CHAT_MIRROR_ENABLED and \
               config_manager.DISCORD_TWITCH_CHAT_MIRROR_CHANNEL_ID:
                try:
                    mirror_channel = self.discord_bot.get_channel(config_manager.DISCORD_TWITCH_CHAT_MIRROR_CHANNEL_ID)
                    if mirror_channel:
                        author_name_for_discord = message.author.display_name if message.author and message.author.display_name else (message.author.name if message.author else "System")
                        
                        discord_msg_content = f"[Twitch] **{discord.utils.escape_markdown(author_name_for_discord)}**: {discord.utils.escape_mentions(discord.utils.escape_markdown(message.content))}"
                        
                        if len(discord_msg_content) > 2000:
                            discord_msg_content = discord_msg_content[:1997] + "..."
                        await mirror_channel.send(discord_msg_content)
                except discord.Forbidden:
                    config_manager.logger.error(f"Twitch Chat Mirror: Bot lacks permission to send to Discord channel {config_manager.DISCORD_TWITCH_CHAT_MIRROR_CHANNEL_ID}.")
                except discord.HTTPException as e:
                    config_manager.logger.error(f"Twitch Chat Mirror: Failed to send message to Discord due to HTTP error: {e}")
                except Exception as e:
                    config_manager.logger.error(f"Twitch Chat Mirror: Unexpected error sending message: {e}", exc_info=True)


    @tasks.loop(seconds=10) 
    async def log_chat_metrics_task(self):
        if not TWITCHIO_COG_ENABLED or not config_manager.TWITCH_CHAT_ENABLED:
            if self.log_chat_metrics_task.is_running(): self.log_chat_metrics_task.cancel()
            return
        
        if not self.is_connected_to_twitch_chat: 
            config_manager.logger.debug("TwitchChatCog: Not connected to Twitch chat, skipping metrics log cycle.")
            return
        
        if self.current_interval_message_count > 0:
            await log_chat_activity_binary(
                self.current_interval_start_time_utc,
                self.current_interval_message_count,
                len(self.current_interval_unique_chatters)
            )

        self.current_interval_start_time_utc = datetime.now(timezone.utc)
        self.current_interval_message_count = 0
        self.current_interval_unique_chatters.clear()

    @log_chat_metrics_task.before_loop
    async def before_log_chat_metrics_task(self):
        if not TWITCHIO_COG_ENABLED or not config_manager.TWITCH_CHAT_ENABLED:
            if self.log_chat_metrics_task.is_running(): self.log_chat_metrics_task.cancel()
            return
        await self.discord_bot.wait_until_ready() 
        
        for _ in range(15): 
            if self.is_connected_to_twitch_chat: 
                break
            await asyncio.sleep(1)
        
        if not self.is_connected_to_twitch_chat:
            config_manager.logger.warning("TwitchChatCog: log_chat_metrics_task starting but Twitch chat not yet connected. Logging may be delayed or not occur if connection fails.")

        desired_interval_config = config_manager.TWITCH_CHAT_LOG_INTERVAL_SECONDS
        if self.log_chat_metrics_task.seconds != desired_interval_config and desired_interval_config > 0:
            self.log_chat_metrics_task.change_interval(seconds=desired_interval_config)
            config_manager.logger.info(f"TwitchChatCog: Updated chat log interval to {desired_interval_config}s.")
    
    @commands.command(name="chatstats", help="Shows recent chat activity. Usage: !chatstats [period (e.g. 5m, 1h) | live]")
    async def chat_stats_command(self, ctx: commands.Context, *, period_input: str = "5m"):
        if not TWITCHIO_COG_ENABLED or not config_manager.TWITCH_CHAT_ENABLED:
            await ctx.send("Twitch chat monitoring is not enabled or TwitchIO library is missing.")
            return
        if not config_manager.TWITCH_CHAT_ACTIVITY_LOG_FILE or not os.path.exists(config_manager.TWITCH_CHAT_ACTIVITY_LOG_FILE):
            await ctx.send(f"Chat activity log file (`{os.path.basename(config_manager.TWITCH_CHAT_ACTIVITY_LOG_FILE)}`) not found or empty.")
            return

        now_utc = datetime.now(timezone.utc)
        query_start_unix, query_end_unix = 0, int(now_utc.timestamp())
        period_name_display = ""

        if period_input.lower() == "live":
            if not config_manager.twitch_session_active_global : 
                await ctx.send("Cannot fetch 'live' chat stats: Bot is not aware of an active stream session via StatusService.")
                return

            session_start_ts_from_status_service = getattr(config_manager, 'current_twitch_session_start_ts_global', None)

            if session_start_ts_from_status_service:
                query_start_unix = session_start_ts_from_status_service
                period_name_display = "current live session"
                config_manager.logger.info(f"!chatstats live: Using session start time from StatusService: {datetime.fromtimestamp(query_start_unix, tz=timezone.utc)}")
            else:
                config_manager.logger.warning("!chatstats live: Precise live session start time not available from StatusService. Using last 1 hour as approximation.")
                delta, parsed_name = parse_duration_to_timedelta("1h")
                if delta: 
                    query_start_unix = int((now_utc - delta).timestamp())
                    period_name_display = "approx. current live session (last 1h)"
                else: 
                    await ctx.send("Error determining 'live' period. Please try a specific duration like '30m'.")
                    return
        else:
            delta, parsed_name = parse_duration_to_timedelta(period_input)
            if not delta:
                await ctx.send(f"Invalid period format. Examples: `5m`, `1h`, `live`. Error: {parsed_name}")
                return
            query_start_unix = int((now_utc - delta).timestamp())
            period_name_display = parsed_name

        async with ctx.typing():
            chat_records = await asyncio.to_thread(
                read_chat_activity_for_period,
                config_manager.TWITCH_CHAT_ACTIVITY_LOG_FILE,
                query_start_unix,
                query_end_unix
            )

        if not chat_records:
            await ctx.send(f"No chat activity data found for the period: {period_name_display}.")
            return

        total_messages = sum(r['message_count'] for r in chat_records)
        avg_unique_chatters_per_interval = sum(r['unique_chatters_count'] for r in chat_records) / len(chat_records) if chat_records else 0
        peak_messages_in_interval = max(r['message_count'] for r in chat_records) if chat_records else 0
        peak_unique_chatters_in_interval = max(r['unique_chatters_count'] for r in chat_records) if chat_records else 0
        
        actual_duration_seconds = query_end_unix - query_start_unix
        actual_duration_minutes = actual_duration_seconds / 60.0
        avg_msg_per_min = total_messages / actual_duration_minutes if actual_duration_minutes > 0 else 0

        embed = discord.Embed(
            title=f"Twitch Chat Stats for {config_manager.UTA_TWITCH_CHANNEL_NAME}",
            description=f"Period: {period_name_display}",
            color=discord.Color.purple()
        )
        embed.add_field(name="Total Messages", value=f"{total_messages:,}", inline=True)
        embed.add_field(name="Avg Messages/Min", value=f"{avg_msg_per_min:,.1f}", inline=True)
        embed.add_field(name="Peak Msgs/Interval", value=f"{peak_messages_in_interval:,}", inline=True) 
        embed.add_field(name="Avg Unique Chatters/Interval", value=f"{avg_unique_chatters_per_interval:,.1f}", inline=True)
        embed.add_field(name="Peak Unique Chatters/Interval", value=f"{peak_unique_chatters_in_interval:,}", inline=True)
        embed.add_field(name="Number of Log Intervals", value=f"{len(chat_records)} (each ~{config_manager.TWITCH_CHAT_LOG_INTERVAL_SECONDS}s)", inline=True)
        
        start_dt_display = datetime.fromtimestamp(query_start_unix, timezone.utc)
        end_dt_display = datetime.fromtimestamp(query_end_unix, timezone.utc)
        embed.set_footer(text=f"Data from {discord.utils.format_dt(start_dt_display, 'f')} to {discord.utils.format_dt(end_dt_display, 'f')}")

        await ctx.send(embed=embed)

async def setup(bot_instance: commands.Bot):
    if not TWITCHIO_COG_ENABLED:
        config_manager.logger.info("TwitchChatCog setup skipped: TwitchIO not available.")
        return
    if not config_manager.TWITCH_CHAT_ENABLED:
        config_manager.logger.info("TwitchChatCog setup skipped: TWITCH_CHAT_ENABLED is false in config.")
        return
        
    valid_config = True
    if not config_manager.TWITCH_CHAT_NICKNAME or "YourBotTwitchNickname" in config_manager.TWITCH_CHAT_NICKNAME:
        config_manager.logger.error("TwitchChatCog: TWITCH_CHAT_NICKNAME is not configured properly. Cog will not load.")
        valid_config = False
    if not config_manager.TWITCH_CHAT_OAUTH_TOKEN or "oauth:yourtwitchtoken" in config_manager.TWITCH_CHAT_OAUTH_TOKEN or not config_manager.TWITCH_CHAT_OAUTH_TOKEN.startswith("oauth:"):
        config_manager.logger.error("TwitchChatCog: TWITCH_CHAT_OAUTH_TOKEN is not configured properly (must start with 'oauth:'). Cog will not load.")
        valid_config = False
    if not config_manager.UTA_TWITCH_CHANNEL_NAME: 
        config_manager.logger.error("TwitchChatCog: UTA_TWITCH_CHANNEL_NAME (used as target chat channel) is not configured. Cog will not load.")
        valid_config = False
    
    if valid_config:
        await bot_instance.add_cog(TwitchChatCog(bot_instance))
    else:
        config_manager.logger.error("TwitchChatCog not loaded due to configuration errors.")