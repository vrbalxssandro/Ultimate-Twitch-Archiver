import discord
from discord.ext import tasks
from datetime import datetime, timezone

from uta_bot.core.bot_instance import bot
from uta_bot import config_manager 
from uta_bot.utils.data_logging import log_follower_data_binary

@tasks.loop(minutes=config_manager.FCTD_UPDATE_INTERVAL_MINUTES)
async def update_channel_name_and_log_followers():
    if not config_manager.fctd_current_twitch_user_id or not config_manager.FCTD_TWITCH_USERNAME or not config_manager.fctd_twitch_api:
        if config_manager.FCTD_TWITCH_USERNAME: 
            config_manager.logger.debug("Follower task: Prerequisites (user ID, username, or API client) not met. Skipping.")
        return

    count = await config_manager.fctd_twitch_api.get_follower_count(config_manager.fctd_current_twitch_user_id)
    time_utc = datetime.now(timezone.utc)

    if count is not None:
        if config_manager.FCTD_FOLLOWER_DATA_FILE:
            await log_follower_data_binary(time_utc, count) 
        
        if config_manager.FCTD_TARGET_CHANNEL_ID:
            target_channel = bot.get_channel(config_manager.FCTD_TARGET_CHANNEL_ID)
            if target_channel:
                new_name = f"{config_manager.FCTD_CHANNEL_NAME_PREFIX}{count:,}{config_manager.FCTD_CHANNEL_NAME_SUFFIX}"
                if target_channel.name != new_name:
                    try:
                        await target_channel.edit(name=new_name)
                        config_manager.logger.info(f"fctd: Updated channel name for {config_manager.FCTD_TWITCH_USERNAME} to: {new_name}")
                    except discord.Forbidden:
                        config_manager.logger.error(f"fctd: Bot lacks 'Manage Channels' permission for channel ID {config_manager.FCTD_TARGET_CHANNEL_ID}.")
                    except discord.HTTPException as e:
                        config_manager.logger.error(f"fctd: Failed to edit channel name due to an HTTP error: {e}")
                    except Exception as e:
                        config_manager.logger.error(f"fctd: An unexpected error occurred while editing channel name: {e}", exc_info=True)
            else:
                config_manager.logger.warning(f"fctd: Target Discord channel ID {config_manager.FCTD_TARGET_CHANNEL_ID} not found.")
    else:
        config_manager.logger.warning(f"fctd: Could not retrieve follower count for {config_manager.FCTD_TWITCH_USERNAME}. Skipping update/log cycle.")

@update_channel_name_and_log_followers.before_loop
async def before_update_task():
    await bot.wait_until_ready()
    current_interval_from_task = update_channel_name_and_log_followers.minutes
    desired_interval_from_config = config_manager.FCTD_UPDATE_INTERVAL_MINUTES
    
    if current_interval_from_task != desired_interval_from_config:
        config_manager.logger.info(
            f"fctd: Follower task interval mismatch (Task: {current_interval_from_task} min, Config: {desired_interval_from_config} min). Updating loop interval."
        )
        try:
            update_channel_name_and_log_followers.change_interval(minutes=desired_interval_from_config)
            config_manager.logger.info(f"fctd: Follower update task interval successfully changed to {desired_interval_from_config} minutes.")
        except Exception as e:
             config_manager.logger.error(f"fctd: Error changing follower task interval dynamically: {e}")

    should_run_task = bool(
        config_manager.FCTD_TWITCH_USERNAME and \
        config_manager.fctd_current_twitch_user_id and \
        config_manager.fctd_twitch_api and \
        (config_manager.FCTD_TARGET_CHANNEL_ID or config_manager.FCTD_FOLLOWER_DATA_FILE)
    )

    if should_run_task:
        config_manager.logger.info(
            f"fctd: Follower update task for {config_manager.FCTD_TWITCH_USERNAME} (interval: {desired_interval_from_config} min) will start/continue if not already running."
        )
    else:
        config_manager.logger.info("fctd: Follower update task prerequisites not met. Task will not run or will be cancelled.")
        if update_channel_name_and_log_followers.is_running():
            update_channel_name_and_log_followers.cancel()