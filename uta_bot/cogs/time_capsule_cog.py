import discord
from discord.ext import commands, tasks
import asyncio
from datetime import datetime, timedelta, timezone
import os
import struct

from uta_bot import config_manager
from uta_bot.utils import data_logging as dl_utils
from uta_bot.utils.formatters import format_duration_human, parse_duration_to_timedelta
from uta_bot.utils.constants import BINARY_RECORD_SIZE

class TimeCapsuleCog(commands.Cog, name="Time Capsule"):
    def __init__(self, bot_instance: commands.Bot):
        self.bot = bot_instance
        self.daily_on_this_day_task.start() # Optional: Start a daily automated task

    def cog_unload(self):
        self.daily_on_this_day_task.cancel()

    async def _get_on_this_day_data(self, target_date: datetime.date):
        """
        Fetches data for a specific date from various logs.
        Returns a dictionary of found data.
        """
        data = {
            "date_str": target_date.isoformat(),
            "follower_change": None, "follower_start": None, "follower_end": None,
            "stream_time_seconds": 0, "games_played": [], "num_sessions": 0,
            "peak_viewers": None, "avg_viewers": None, "viewer_data_points": 0,
            "errors": []
        }

        day_start_utc = datetime.combine(target_date, datetime.min.time(), tzinfo=timezone.utc)
        day_end_utc = datetime.combine(target_date, datetime.max.time(), tzinfo=timezone.utc)
        day_start_unix = int(day_start_utc.timestamp())
        day_end_unix = int(day_end_utc.timestamp())

        # Follower Data
        if config_manager.FCTD_TWITCH_USERNAME and config_manager.FCTD_FOLLOWER_DATA_FILE:
            if not os.path.exists(config_manager.FCTD_FOLLOWER_DATA_FILE):
                data["errors"].append(f"Follower log missing: {config_manager.FCTD_FOLLOWER_DATA_FILE}")
            else:
                foll_res = await asyncio.to_thread(
                    dl_utils.get_counts_for_day_boundaries,
                    config_manager.FCTD_FOLLOWER_DATA_FILE,
                    target_date
                )
                if isinstance(foll_res, dict):
                    data["follower_start"] = foll_res.get('start_count')
                    data["follower_end"] = foll_res.get('end_count')
                    if data["follower_start"] is not None and data["follower_end"] is not None:
                        data["follower_change"] = data["follower_end"] - data["follower_start"]
                else: # Error string from get_counts_for_day_boundaries
                    data["errors"].append(f"Follower data: {foll_res}")
        
        # Stream Activity Data (Games, Stream Time)
        if config_manager.UTA_ENABLED and config_manager.UTA_STREAM_ACTIVITY_LOG_FILE:
            if not os.path.exists(config_manager.UTA_STREAM_ACTIVITY_LOG_FILE):
                data["errors"].append(f"Stream activity log missing: {config_manager.UTA_STREAM_ACTIVITY_LOG_FILE}")
            else:
                game_segments = await asyncio.to_thread(
                    dl_utils.parse_stream_activity_for_game_segments,
                    config_manager.UTA_STREAM_ACTIVITY_LOG_FILE,
                    day_start_unix,
                    day_end_unix
                )
                if game_segments:
                    data["stream_time_seconds"] = sum(seg['end_ts'] - seg['start_ts'] for seg in game_segments)
                    
                    distinct_games = sorted(list(set(
                        seg['game'] for seg in game_segments if seg.get('game') and seg['game'] != "N/A"
                    )))
                    data["games_played"] = distinct_games
                    
                    # Count distinct sessions within the day
                    game_segments.sort(key=lambda s: s['start_ts'])
                    if game_segments:
                        data["num_sessions"] = 1
                        for i in range(1, len(game_segments)):
                            # Consider a new session if gap is > 10 mins (600s) - adjust as needed
                            if game_segments[i]['start_ts'] - game_segments[i-1]['end_ts'] > 600:
                                data["num_sessions"] +=1
                
        # Viewer Data
        if config_manager.UTA_ENABLED and config_manager.UTA_VIEWER_COUNT_LOGGING_ENABLED and config_manager.UTA_VIEWER_COUNT_LOG_FILE:
            if not os.path.exists(config_manager.UTA_VIEWER_COUNT_LOG_FILE):
                 data["errors"].append(f"Viewer count log missing: {config_manager.UTA_VIEWER_COUNT_LOG_FILE}")
            else:
                avg_v, peak_v, num_dp = await asyncio.to_thread(
                    dl_utils.get_viewer_stats_for_period,
                    config_manager.UTA_VIEWER_COUNT_LOG_FILE,
                    day_start_unix,
                    day_end_unix
                )
                data["avg_viewers"] = avg_v
                data["peak_viewers"] = peak_v
                data["viewer_data_points"] = num_dp
        
        return data

    async def _send_on_this_day_embed(self, ctx_or_channel, target_date: datetime.date, historical_data: dict):
        channel_name_display = config_manager.UTA_TWITCH_CHANNEL_NAME or config_manager.FCTD_TWITCH_USERNAME or "The Channel"
        
        embed = discord.Embed(
            title=f"⏳ On This Day: {target_date.strftime('%B %d')}",
            description=f"A look back at what happened on **{target_date.isoformat()}** for {channel_name_display}.",
            color=discord.Color.teal()
        )

        found_any_data = False

        if historical_data["stream_time_seconds"] > 0:
            found_any_data = True
            stream_time_human = format_duration_human(historical_data["stream_time_seconds"])
            sessions_str = f"across {historical_data['num_sessions']} session(s)" if historical_data['num_sessions'] > 0 else ""
            embed.add_field(name="📺 Stream Time", value=f"Streamed for **{stream_time_human}** {sessions_str}.", inline=False)
            if historical_data["games_played"]:
                games_str = ", ".join(historical_data["games_played"][:5]) # Show up to 5
                if len(historical_data["games_played"]) > 5: games_str += "..."
                embed.add_field(name="🎮 Games Played", value=games_str, inline=True)

        if historical_data["peak_viewers"] is not None:
            found_any_data = True
            embed.add_field(name="👀 Peak Viewers", value=f"{historical_data['peak_viewers']:,}", inline=True)
        if historical_data["avg_viewers"] is not None:
            found_any_data = True
            dp_str = f" (from {historical_data['viewer_data_points']} points)" if historical_data['viewer_data_points'] else ""
            embed.add_field(name="📊 Avg. Viewers", value=f"{historical_data['avg_viewers']:,.1f}{dp_str}", inline=True)
        
        if historical_data["follower_change"] is not None:
            found_any_data = True
            change_str = f"{historical_data['follower_change']:+,}"
            current_f_str = f"(Ending at {historical_data['follower_end']:,})" if historical_data['follower_end'] is not None else ""
            embed.add_field(name="📈 Follower Change", value=f"{change_str} followers {current_f_str}", inline=False)

        if not found_any_data and not historical_data["errors"]:
            embed.description += "\n\nNo significant activity or data found for this date in the logs."
        elif not found_any_data and historical_data["errors"]:
             embed.description += "\n\nCould not retrieve sufficient data for this date."

        if historical_data["errors"]:
            embed.add_field(name="⚠️ Data Issues", value="\n".join(historical_data["errors"][:3]), inline=False)

        embed.set_footer(text=f"Data for {historical_data['date_str']}")
        embed.timestamp = datetime.now(timezone.utc)

        if isinstance(ctx_or_channel, commands.Context):
            await ctx_or_channel.send(embed=embed)
        elif isinstance(ctx_or_channel, discord.TextChannel):
            await ctx_or_channel.send(embed=embed)


    @commands.command(name="onthisday", aliases=['otd'], help="Shows stats for this day in previous years. Usage: !onthisday [YYYY-MM-DD]")
    async def on_this_day_command(self, ctx: commands.Context, date_input: str = None):
        
        target_day_month_for_search = (datetime.now(timezone.utc).month, datetime.now(timezone.utc).day)
        specific_date_mode = False

        if date_input:
            try:
                parsed_date = datetime.strptime(date_input, "%Y-%m-%d").date()
                target_day_month_for_search = (parsed_date.month, parsed_date.day)
                specific_date_mode = True
                # For specific date mode, we only look at that exact date
                historical_data = await self._get_on_this_day_data(parsed_date)
                await self._send_on_this_day_embed(ctx, parsed_date, historical_data)
                return
            except ValueError:
                await ctx.send("Invalid date format. Please use YYYY-MM-DD, or no argument for today in previous years.")
                return
        
        # If no date_input, iterate previous years for current month/day
        current_year = datetime.now(timezone.utc).year
        years_to_check = range(current_year - 1, current_year - 6, -1) # Look back up to 5 years

        num_embeds_sent = 0
        async with ctx.typing():
            for year in years_to_check:
                try:
                    historical_date_to_check = datetime(year, target_day_month_for_search[0], target_day_month_for_search[1]).date()
                    historical_data = await self._get_on_this_day_data(historical_date_to_check)
                    
                    # Only send embed if there's something to show (or errors to report for that day)
                    if historical_data["stream_time_seconds"] > 0 or \
                       historical_data["peak_viewers"] is not None or \
                       historical_data["follower_change"] is not None or \
                       historical_data["errors"]:
                        await self._send_on_this_day_embed(ctx, historical_date_to_check, historical_data)
                        num_embeds_sent +=1
                        await asyncio.sleep(0.5) # Brief pause if sending multiple embeds
                except ValueError: # Handles cases like Feb 29 on a non-leap year
                    continue 
                except Exception as e:
                    config_manager.logger.error(f"Error processing 'onthisday' for year {year}: {e}", exc_info=True)
                    await ctx.send(f"An error occurred while fetching data for {year}-{target_day_month_for_search[0]:02d}-{target_day_month_for_search[1]:02d}.")

        if num_embeds_sent == 0 and not specific_date_mode:
            await ctx.send(f"No significant historical data found for {target_day_month_for_search[1]:02d}/{target_day_month_for_search[0]:02d} in the past 5 years.")

    @tasks.loop(hours=24)
    async def daily_on_this_day_task(self):
        # This task will run once a day and post "On This Day" for previous years
        # to a pre-configured channel if one is set in config.
        # For now, let's assume a config var like `ON_THIS_DAY_AUTO_CHANNEL_ID`
        # This part is optional and can be expanded later.
        # Example:
        # channel_id = getattr(config_manager, "ON_THIS_DAY_AUTO_CHANNEL_ID", None)
        # if channel_id:
        #     channel = self.bot.get_channel(int(channel_id))
        #     if channel:
        #         # ... logic similar to on_this_day_command ...
        #         # await self._send_on_this_day_embed(channel, date_to_check, data)
        #         pass
        pass # Placeholder for now

    @daily_on_this_day_task.before_loop
    async def before_daily_on_this_day_task(self):
        await self.bot.wait_until_ready()
        # Logic to ensure it runs at a specific time of day can be added here
        # e.g., calculate seconds until next 9 AM UTC.
        
        # For simplicity now, we'll just let it run 24h after bot start and then every 24h.
        # A more robust solution would use `time=datetime.time(...)` in the @tasks.loop decorator.
        # Example for running at 9 AM UTC:
        # now = datetime.now(timezone.utc)
        # then = now.replace(hour=9, minute=0, second=0, microsecond=0)
        # if then < now: then += timedelta(days=1)
        # await discord.utils.sleep_until(then)
        pass


async def setup(bot_instance):
    await bot_instance.add_cog(TimeCapsuleCog(bot_instance))