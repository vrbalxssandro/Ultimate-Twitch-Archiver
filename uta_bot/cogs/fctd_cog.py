import discord
from discord.ext import commands
from datetime import datetime, timezone, timedelta
import os
import struct
import io 
import asyncio # <--- IMPORT ADDED HERE

from uta_bot import config_manager
from uta_bot.core.bot_instance import bot
from uta_bot.utils.formatters import format_duration_human, parse_duration_to_timedelta
from uta_bot.utils.data_logging import read_and_find_records_for_period, get_counts_for_day_boundaries
from uta_bot.utils.constants import BINARY_RECORD_SIZE


class FCTDCog(commands.Cog, name="Follower Counter Commands"):
    def __init__(self, bot_instance):
        self.bot = bot_instance

    async def cog_check(self, ctx):
        """Checks if the command can be run in the given context."""
        if ctx.guild and config_manager.FCTD_COMMAND_CHANNEL_ID is not None and \
           ctx.channel.id != config_manager.FCTD_COMMAND_CHANNEL_ID:
            return False
        return True

    @commands.command(name="followers", aliases=['foll', 'followerstats'], help="Follower gain/loss over a period. Usage: !followers <period>")
    async def followers_command(self, ctx: commands.Context, *, duration_input: str = None):
        if not config_manager.FCTD_TWITCH_USERNAME:
            await ctx.send("fctd: Twitch user for follower tracking is not configured.")
            return
        
        if duration_input is None:
            embed = discord.Embed(
                title=f"{config_manager.FCTD_TWITCH_USERNAME} Follower Stats",
                description=f"Please specify a duration for the stats report. Use `{config_manager.FCTD_COMMAND_PREFIX}followers <duration>`.",
                color=discord.Color.purple()
            )
            embed.add_field(name="Valid Duration Examples", value="`10m` (minutes), `2h` (hours), `3d` (days), `1w` (weeks), `1mo` (months), `1y` (years)", inline=False)
            await ctx.send(embed=embed)
            return

        if not config_manager.FCTD_FOLLOWER_DATA_FILE or \
           not os.path.exists(config_manager.FCTD_FOLLOWER_DATA_FILE) or \
           os.path.getsize(config_manager.FCTD_FOLLOWER_DATA_FILE) < BINARY_RECORD_SIZE:
            await ctx.send(f"fctd: Not enough follower data has been logged for {config_manager.FCTD_TWITCH_USERNAME} to generate stats. Please wait for data to accumulate.")
            return

        time_delta, period_name_display = parse_duration_to_timedelta(duration_input)
        if not time_delta:
            await ctx.send(period_name_display) 
            return

        msg = "" 
        async with ctx.typing():
            now_utc = datetime.now(timezone.utc)
            cutoff_datetime_utc = now_utc - time_delta
            cutoff_timestamp_unix = int(cutoff_datetime_utc.timestamp())

            start_c, end_c, first_ts_unix, last_ts_unix, all_records = await asyncio.to_thread(
                read_and_find_records_for_period,
                config_manager.FCTD_FOLLOWER_DATA_FILE,
                cutoff_timestamp_unix,
                None 
            )

            if end_c is None or last_ts_unix is None: 
                msg = f"Not enough data in `{config_manager.FCTD_FOLLOWER_DATA_FILE}` to determine current follower count."
            elif start_c is None or first_ts_unix is None: 
                if all_records: 
                    oldest_ts, oldest_count = all_records[0]
                    current_ts, current_count = all_records[-1] 
                    gain_since_oldest = current_count - oldest_count
                    gain_msg_part = f"gained {gain_since_oldest:,}" if gain_since_oldest > 0 else \
                                    f"lost {-gain_since_oldest:,}" if gain_since_oldest < 0 else "had no change in"
                    oldest_dt_display = datetime.fromtimestamp(oldest_ts, timezone.utc)
                    msg = (f"Not enough data for the start of {period_name_display}. "
                           f"The oldest available data is from {discord.utils.format_dt(oldest_dt_display, 'R')}.\n"
                           f"Since then, {config_manager.FCTD_TWITCH_USERNAME} has {gain_msg_part} followers. Current: {current_count:,}")
                else: 
                    msg = "Could not determine a start point for follower data from the log file."
            else: 
                gain = end_c - start_c
                actual_start_dt = datetime.fromtimestamp(first_ts_unix, timezone.utc)
                actual_end_dt = datetime.fromtimestamp(last_ts_unix, timezone.utc)

                final_period_description = period_name_display
                if actual_start_dt > cutoff_datetime_utc + timedelta(minutes=max(1, time_delta.total_seconds() * 0.05 / 60)): 
                    effective_data_span_seconds = (actual_end_dt - actual_start_dt).total_seconds()
                    human_readable_span = format_duration_human(int(effective_data_span_seconds))
                    final_period_description = f"{period_name_display} (effective data spans ~{human_readable_span} from {discord.utils.format_dt(actual_start_dt,'R')})"

                if gain == 0:
                    msg = (f"{config_manager.FCTD_TWITCH_USERNAME}'s follower count has remained stable at {end_c:,} followers in {final_period_description}.\n"
                           f"(Data from {discord.utils.format_dt(actual_start_dt, 'R')} to {discord.utils.format_dt(actual_end_dt, 'R')})")
                else:
                    gain_text = "gained" if gain > 0 else "lost"
                    change_text = f"{abs(gain):,}"
                    msg = (f"{config_manager.FCTD_TWITCH_USERNAME} {gain_text} {change_text} followers in {final_period_description}.\n"
                           f"From {start_c:,} ({discord.utils.format_dt(actual_start_dt, 'R')}) to {end_c:,} ({discord.utils.format_dt(actual_end_dt, 'R')}).")
        
        await ctx.send(msg or "fctd: An unexpected error occurred while fetching follower data.")

    @commands.command(name="follrate", aliases=['growthrate'], help="Follower growth rate & optional graph. Usage: !follrate <period>")
    async def follower_rate_command(self, ctx: commands.Context, *, duration_input: str = None):
        if not config_manager.FCTD_TWITCH_USERNAME:
            await ctx.send("fctd: Twitch user for follower tracking is not configured.")
            return
        if not config_manager.FCTD_FOLLOWER_DATA_FILE or \
           not os.path.exists(config_manager.FCTD_FOLLOWER_DATA_FILE) or \
           os.path.getsize(config_manager.FCTD_FOLLOWER_DATA_FILE) < BINARY_RECORD_SIZE * 2: 
            await ctx.send(f"fctd: Not enough follower data for {config_manager.FCTD_TWITCH_USERNAME} to calculate rates (at least 2 data points are needed).")
            return
        
        if duration_input is None:
            await ctx.send(f"Please specify a period for the follower growth rate calculation. Usage: `{config_manager.FCTD_COMMAND_PREFIX}follrate <duration>` (e.g., `7d`, `30d`, `3mo`).")
            return

        time_delta, period_name_display = parse_duration_to_timedelta(duration_input)
        if not time_delta:
            await ctx.send(period_name_display) 
            return

        async with ctx.typing():
            now_utc = datetime.now(timezone.utc)
            cutoff_datetime_utc = now_utc - time_delta
            cutoff_timestamp_unix = int(cutoff_datetime_utc.timestamp())

            start_c, end_c, first_ts_unix, last_ts_unix, _ = await asyncio.to_thread(
                read_and_find_records_for_period,
                config_manager.FCTD_FOLLOWER_DATA_FILE,
                cutoff_timestamp_unix,
                None 
            )

            if start_c is None or end_c is None or first_ts_unix is None or last_ts_unix is None or last_ts_unix <= first_ts_unix :
                await ctx.send(f"Could not retrieve sufficient distinct data points for the period '{period_name_display}' to calculate follower rates.")
                return

            gain = end_c - start_c
            actual_duration_seconds = last_ts_unix - first_ts_unix
            
            if actual_duration_seconds < 60 * 15 : 
                await ctx.send(f"The effective data range ({format_duration_human(actual_duration_seconds)}) is too short to calculate meaningful follower rates for {period_name_display}.")
                return

            actual_duration_days = actual_duration_seconds / 86400.0
            avg_per_day = gain / actual_duration_days if actual_duration_days > 0 else 0
            avg_per_week = avg_per_day * 7
            avg_per_month = avg_per_day * 30.4375 

            actual_start_dt = datetime.fromtimestamp(first_ts_unix, timezone.utc)
            actual_end_dt = datetime.fromtimestamp(last_ts_unix, timezone.utc)

            embed = discord.Embed(
                title=f"Follower Growth Rate for {config_manager.FCTD_TWITCH_USERNAME}",
                description=f"Analysis period: **{period_name_display}**\nEffective data from {discord.utils.format_dt(actual_start_dt, 'R')} to {discord.utils.format_dt(actual_end_dt, 'R')}",
                color=discord.Color.green() if gain >= 0 else discord.Color.red()
            )
            embed.add_field(name="Total Change", value=f"{gain:+,} followers", inline=False)
            embed.add_field(name="Effective Data Duration", value=format_duration_human(int(actual_duration_seconds)), inline=False)
            embed.add_field(name="Avg. per Day", value=f"{avg_per_day:+.2f}", inline=True)
            embed.add_field(name="Avg. per Week", value=f"{avg_per_week:+.2f}", inline=True)
            embed.add_field(name="Avg. per Month (approx.)", value=f"{avg_per_month:+.2f}", inline=True)
            embed.set_footer(text=f"Initial Follower Count: {start_c:,} | Final Follower Count: {end_c:,}")

        await ctx.send(embed=embed) 

    @commands.command(name="daystats", help="Follower & stream stats on a date (YYYY-MM-DD) with optional graph.")
    async def day_stats_command(self, ctx: commands.Context, date_str: str = None):
        target_twitch_user = config_manager.FCTD_TWITCH_USERNAME
        uta_target_user = config_manager.UTA_TWITCH_CHANNEL_NAME if config_manager.UTA_ENABLED else None
        
        if not target_twitch_user and not (uta_target_user and (config_manager.UTA_RESTREAMER_ENABLED or config_manager.UTA_STREAM_STATUS_NOTIFICATIONS_ENABLED) ):
            await ctx.send("No Twitch user configured for follower stats, and/or UTA module (restreamer/status) is not configured for stream stats.")
            return

        if date_str is None:
            await ctx.send(f"Please provide a date in YYYY-MM-DD format. Example: `{config_manager.FCTD_COMMAND_PREFIX}daystats 2023-10-26`")
            return
        try:
            target_date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            await ctx.send("Invalid date format. Please use YYYY-MM-DD.")
            return
        
        if target_date_obj > (datetime.now(timezone.utc).date() + timedelta(days=1)): 
            await ctx.send("Cannot query stats for dates too far in the future.")
            return

        embed = discord.Embed(
            title=f"Twitch Stats for {target_date_obj.isoformat()}",
            color=discord.Color.blue()
        )
        
        day_start_unix = int(datetime.combine(target_date_obj, datetime.min.time(), tzinfo=timezone.utc).timestamp())
        day_end_unix = int(datetime.combine(target_date_obj, datetime.max.time(), tzinfo=timezone.utc).timestamp())

        async with ctx.typing():
            if target_twitch_user and config_manager.FCTD_FOLLOWER_DATA_FILE:
                result_foll = await asyncio.to_thread(
                    get_counts_for_day_boundaries, 
                    config_manager.FCTD_FOLLOWER_DATA_FILE, 
                    target_date_obj
                )
                if isinstance(result_foll, str): 
                    embed.add_field(name=f"Followers ({target_twitch_user})", value=result_foll, inline=False)
                else:
                    start_count = result_foll['start_count']
                    end_count = result_foll['end_count']
                    start_ts_foll = result_foll['start_ts']
                    end_ts_foll = result_foll['end_ts']
                    num_records_on_day_foll = result_foll['num_records_on_day']
                    
                    gain = end_count - start_count
                    start_dt_foll_display = datetime.fromtimestamp(start_ts_foll, timezone.utc)
                    end_dt_foll_display = datetime.fromtimestamp(end_ts_foll, timezone.utc)
                    
                    action_str = "gained" if gain > 0 else "lost" if gain < 0 else "no net change in"
                    follower_desc_main = (f"{target_twitch_user} {action_str} {abs(gain):,} followers.\n"
                                     f"Initial: {start_count:,} ({discord.utils.format_dt(start_dt_foll_display,'R')})\n"
                                     f"Final: {end_count:,} ({discord.utils.format_dt(end_dt_foll_display,'R')})")
                    
                    notes = []
                    if start_dt_foll_display.date() < target_date_obj: notes.append("Initial count from prior day.")
                    if end_ts_foll == start_ts_foll and num_records_on_day_foll == 0 : notes.append("Count stable (no new data points on this day).")
                    elif num_records_on_day_foll == 0 and end_dt_foll_display.date() < target_date_obj: notes.append("Final count from prior day (no data on target day).")
                    elif num_records_on_day_foll > 0: notes.append(f"{num_records_on_day_foll} data point(s) recorded on this day.")
                    
                    if notes: follower_desc_main += f"\n*({' | '.join(notes)})*"
                    embed.add_field(name=f"Followers ({target_twitch_user})", value=follower_desc_main, inline=False)

            elif target_twitch_user: 
                 embed.add_field(name=f"Followers ({target_twitch_user})", value="Follower data file not configured.", inline=False)
            
            if uta_target_user and config_manager.UTA_STREAM_ACTIVITY_LOG_FILE and config_manager.UTA_STREAM_STATUS_NOTIFICATIONS_ENABLED:
                game_segments_day = await asyncio.to_thread(
                    config_manager.parse_stream_activity_for_game_segments, 
                    config_manager.UTA_STREAM_ACTIVITY_LOG_FILE, 
                    day_start_unix, 
                    day_end_unix
                )
                total_stream_time_on_day_seconds = sum(seg['end_ts'] - seg['start_ts'] for seg in game_segments_day)
                
                num_distinct_streams = 0
                if game_segments_day:
                    game_segments_day.sort(key=lambda s: s['start_ts']) 
                    num_distinct_streams = 1
                    for i in range(1, len(game_segments_day)):
                        if game_segments_day[i]['start_ts'] - game_segments_day[i-1]['end_ts'] > 600:
                            num_distinct_streams +=1
                
                if total_stream_time_on_day_seconds > 0:
                    stream_time_str = format_duration_human(total_stream_time_on_day_seconds)
                    embed.add_field(name=f"Total Stream Time ({uta_target_user})", 
                                    value=f"Streamed for **{stream_time_str}** across {num_distinct_streams} session(s).", 
                                    inline=False)
                else:
                    embed.add_field(name=f"Total Stream Time ({uta_target_user})", 
                                    value="No streams logged via UTA activity log on this day.", 
                                    inline=False)
            elif uta_target_user: 
                embed.add_field(name=f"Total Stream Time ({uta_target_user})", 
                                value="UTA Stream Activity log not configured or status notifications not enabled.", 
                                inline=False)

            if uta_target_user and config_manager.UTA_VIEWER_COUNT_LOGGING_ENABLED and config_manager.UTA_VIEWER_COUNT_LOG_FILE and \
               os.path.exists(config_manager.UTA_VIEWER_COUNT_LOG_FILE):
                avg_viewers, peak_viewers_day, num_datapoints = await asyncio.to_thread(
                     config_manager.get_viewer_stats_for_period, 
                     config_manager.UTA_VIEWER_COUNT_LOG_FILE, 
                     day_start_unix, 
                     day_end_unix
                 )
                if avg_viewers is not None:
                    embed.add_field(name=f"Avg Viewers ({uta_target_user})", 
                                    value=f"{avg_viewers:,.1f} (from {num_datapoints} data points)\nPeak on day: {peak_viewers_day:,}", 
                                    inline=False)
                else:
                    embed.add_field(name=f"Avg Viewers ({uta_target_user})", 
                                    value="No viewer data logged for this day.", 
                                    inline=False)
            elif uta_target_user : 
                 embed.add_field(name=f"Avg Viewers ({uta_target_user})", 
                                 value="Viewer count logging disabled or log file not found.", 
                                 inline=False)
        
        if not embed.fields: 
            embed.description = "No data to display for this day. Please check configurations and data file existence."

        await ctx.send(embed=embed) 


async def setup(bot_instance):
    await bot_instance.add_cog(FCTDCog(bot_instance))