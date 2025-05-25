from uta_bot import config_manager 

# Check for twitchio before attempting to import TwitchChatCog
TWITCHIO_AVAILABLE = False # Define it at the module level
try:
    import twitchio
    TWITCHIO_AVAILABLE = True
except ImportError:
    config_manager.logger.warning("cogs/__init__.py: TwitchIO library not found. Twitch Chat Monitoring features will be disabled. Install with 'pip install twitchio'.")


async def setup(bot):
    from .admin_cog import AdminCog
    from .fctd_cog import FCTDCog
    from .uta_info_cog import UTAInfoCog
    from .milestones_cog import MilestonesCog
    from .time_capsule_cog import TimeCapsuleCog # Added for On This Day
    
    await bot.add_cog(AdminCog(bot))
    config_manager.logger.info("Loaded AdminCog")
    
    await bot.add_cog(FCTDCog(bot))
    config_manager.logger.info("Loaded FCTDCog")

    await bot.add_cog(UTAInfoCog(bot))
    config_manager.logger.info("Loaded UTAInfoCog")

    await bot.add_cog(MilestonesCog(bot))
    config_manager.logger.info("Loaded MilestonesCog")

    await bot.add_cog(TimeCapsuleCog(bot)) # Added for On This Day
    config_manager.logger.info("Loaded TimeCapsuleCog")

    if config_manager.MATPLOTLIB_AVAILABLE:
        from .plot_cog import PlotCog
        await bot.add_cog(PlotCog(bot))
        config_manager.logger.info("Loaded PlotCog (Matplotlib available)")
    else:
        config_manager.logger.info("PlotCog not loaded (Matplotlib not available)")

    if TWITCHIO_AVAILABLE: # Use the flag defined in this file
        from .twitch_chat_cog import TwitchChatCog
        await bot.add_cog(TwitchChatCog(bot)) 
        config_manager.logger.info("Loaded TwitchChatCog (TwitchIO available)")
    else:
        config_manager.logger.info("TwitchChatCog not loaded (TwitchIO not available)")