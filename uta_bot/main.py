import asyncio
import discord
import os
import sys
import shutil 
import time 
from datetime import datetime, timezone

PACKAGE_PARENT = '..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

from uta_bot import config_manager 
from uta_bot.core.bot_instance import bot
from uta_bot.core import event_handlers 
from uta_bot.core import background_tasks 
from uta_bot.utils.data_logging import log_bot_session_event, BOT_EVENT_STOP 
from uta_bot.services.threading_manager import shutdown_event, stop_all_services
# cleanup_restream_processes is called within stop_all_services now

async def load_cogs():
    config_manager.logger.info("Loading cogs...")
    try:
        from uta_bot.cogs import setup as setup_cogs
        await setup_cogs(bot)
        config_manager.logger.info("All cogs loaded successfully.")
    except Exception as e:
        config_manager.logger.error(f"Failed to load cogs: {e}", exc_info=True)


async def run_bot():
    config_manager.logger.info("Starting bot...")
    
    async with bot: 
        await load_cogs() 
        await bot.start(config_manager.DISCORD_TOKEN)

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(run_bot())
    except KeyboardInterrupt:
        config_manager.logger.info("KeyboardInterrupt received. Shutting down...")
    except discord.LoginFailure:
        config_manager.logger.critical("CRITICAL: Invalid Discord Bot Token. Please check your config.json.")
    except Exception as e:
        config_manager.logger.critical(f"Unexpected error during bot startup/runtime: {e}", exc_info=True)
    finally:
        config_manager.logger.info("Initiating final cleanup sequence...")
        
        if bot and bot.loop and not bot.loop.is_closed():
            try:
                asyncio.ensure_future(log_bot_session_event(BOT_EVENT_STOP, datetime.now(timezone.utc)), loop=bot.loop)
                if sys.platform == "win32": 
                    async def _sleep_async(duration): await asyncio.sleep(duration)
                    loop.run_until_complete(_sleep_async(0.2))
                else:
                    time.sleep(0.2)
            except Exception as e_log_stop:
                config_manager.logger.error(f"Error logging bot stop event during shutdown: {e_log_stop}")

        if config_manager.UTA_ENABLED: 
            config_manager.logger.info("Main Shutdown: Initiating stop for UTA services.")
            
            if bot.loop and not bot.loop.is_closed() and bot.loop.is_running():
                 try:
                    loop.run_until_complete(stop_all_services())
                 except RuntimeError as rerr: 
                    config_manager.logger.warning(f"Loop closed before UTA service stop could complete: {rerr}")
            else: 
                asyncio.run(stop_all_services()) 
        
        config_manager.logger.info("Shutdown sequence finished. Exiting.")