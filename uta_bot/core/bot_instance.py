import discord
from discord.ext import commands
from uta_bot import config_manager 

intents = discord.Intents.default()
intents.message_content = True 

bot_owner_id = None
if config_manager.owner_id_from_config:
    try:
        bot_owner_id = int(config_manager.owner_id_from_config)
    except ValueError:
        config_manager.logger.error(
            f"Invalid DISCORD_BOT_OWNER_ID in config: '{config_manager.owner_id_from_config}'. Must be an integer. Owner commands may not work correctly."
        )

bot = commands.Bot(
    command_prefix=config_manager.FCTD_COMMAND_PREFIX,
    intents=intents,
    help_command=None, 
    owner_id=bot_owner_id 
)