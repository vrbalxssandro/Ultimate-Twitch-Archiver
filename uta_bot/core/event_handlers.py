import discord
from discord.ext import commands, tasks
from datetime import datetime, timezone
import shutil 
import os    

from uta_bot.core.bot_instance import bot
from uta_bot import config_manager 
from uta_bot.utils.data_logging import log_bot_session_event, BOT_EVENT_START, BOT_EVENT_STOP
from uta_bot.core.background_tasks import update_channel_name_and_log_followers 
from uta_bot.services.threading_manager import start_all_services, stop_all_services # shutdown_event is also there


@bot.event
async def on_ready():
    config_manager.logger.info(f'{bot.user.name} (ID: {bot.user.id}) connected to Discord!')
    
    config_manager.bot_start_time = datetime.now(timezone.utc)
    await log_bot_session_event(BOT_EVENT_START, config_manager.bot_start_time)

    config_manager.logger.info(f'Bot ready at: {config_manager.bot_start_time.isoformat()}')
    config_manager.logger.info(f'Command Prefix: {config_manager.FCTD_COMMAND_PREFIX}')
    
    fctd_cmd_ch_msg = (f'Listening for fctd commands in channel ID: {config_manager.FCTD_COMMAND_CHANNEL_ID}'
                       if config_manager.FCTD_COMMAND_CHANNEL_ID else 'Listening for fctd commands in ALL channels.')
    config_manager.logger.info(fctd_cmd_ch_msg)
    config_manager.logger.info(f'Connected to {len(bot.guilds)} guilds.')

    if config_manager.FCTD_TWITCH_USERNAME and config_manager.fctd_twitch_api:
        config_manager.logger.info(f'fctd: Targeting Twitch User for followers: {config_manager.FCTD_TWITCH_USERNAME}')
        config_manager.fctd_current_twitch_user_id = await config_manager.fctd_twitch_api.get_user_id(config_manager.FCTD_TWITCH_USERNAME)
        if not config_manager.fctd_current_twitch_user_id:
            config_manager.logger.error(f"fctd: CRITICAL: Could not get Twitch User ID for {config_manager.FCTD_TWITCH_USERNAME}. Follower features will FAIL.")
        else:
            config_manager.logger.info(f"fctd: Twitch User ID for {config_manager.FCTD_TWITCH_USERNAME} is {config_manager.fctd_current_twitch_user_id}")
            if config_manager.FCTD_TARGET_CHANNEL_ID or config_manager.FCTD_FOLLOWER_DATA_FILE:
                if not update_channel_name_and_log_followers.is_running():
                    update_channel_name_and_log_followers.start()
                    config_manager.logger.info("fctd: Started follower update task.")
    elif not config_manager.FCTD_TWITCH_USERNAME:
        config_manager.logger.warning("fctd: FCTD_TWITCH_USERNAME not set. Follower tracking disabled.")
    elif not config_manager.fctd_twitch_api:
        config_manager.logger.error("fctd: Twitch API not initialized. Follower tracking disabled.")


    if config_manager.UTA_ENABLED:
        config_manager.logger.info("--- UTA Module Enabled ---")
        if not config_manager.UTA_TWITCH_CHANNEL_NAME or "target_twitch_username_for_uta" in config_manager.UTA_TWITCH_CHANNEL_NAME:
            config_manager.logger.error("UTA: UTA_TWITCH_CHANNEL_NAME not configured properly. UTA features may be limited or disabled.")
        else:
            config_manager.logger.info(f"UTA: Targeting Twitch Channel: {config_manager.UTA_TWITCH_CHANNEL_NAME}")
        
        start_all_services(bot) 
    else:
        config_manager.logger.info("--- UTA Module Disabled ---")

    if not config_manager.MATPLOTLIB_AVAILABLE:
        config_manager.logger.warning("Matplotlib library not found. Plotting commands will be disabled. Install with 'pip install matplotlib'.")


@bot.event
async def on_command_error(ctx: commands.Context, error):
    if isinstance(error, commands.CommandNotFound):
        pass 
    elif isinstance(error, commands.MissingRequiredArgument):
        await ctx.send(f"Missing argument for `{ctx.command.name}`. Use `{config_manager.FCTD_COMMAND_PREFIX}help {ctx.command.name}` for more info.", delete_after=15)
    elif isinstance(error, commands.NotOwner):
        await ctx.send("Sorry, this command can only be used by the bot owner.", delete_after=10)
    elif isinstance(error, commands.CheckFailure):
        config_manager.logger.warning(f"Command check failed for {ctx.author} on '{ctx.command}': {error}")
    elif isinstance(error, commands.CommandInvokeError):
        config_manager.logger.error(f'Error in command {ctx.command}: {error.original}', exc_info=error.original)
        await ctx.send(f"An error occurred while executing the command: {error.original}", delete_after=10)
    else:
        config_manager.logger.error(f'Unhandled command error for command {ctx.command}: {error}', exc_info=error)