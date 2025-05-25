import discord
from discord.ext import commands
from datetime import datetime, timezone, timedelta
import os
import struct
import io 

from uta_bot import config_manager 
from uta_bot.core.bot_instance import bot
from uta_bot.utils.formatters import parse_duration_to_timedelta
from uta_bot.utils.data_logging import (
    read_and_find_records_for_period, 
    get_counts_for_day_boundaries,
    read_stream_durations_for_period, 
    parse_stream_activity_for_game_segments 
)
from uta_bot.utils.constants import (
    BINARY_RECORD_FORMAT, BINARY_RECORD_SIZE,
    STREAM_DURATION_RECORD_FORMAT, STREAM_DURATION_RECORD_SIZE,
    SA_BASE_HEADER_FORMAT, SA_BASE_HEADER_SIZE, EVENT_TYPE_STREAM_START, EVENT_TYPE_STREAM_END,
    SA_STRING_LEN_FORMAT, SA_STRING_LEN_SIZE, SA_INT_FORMAT, SA_INT_SIZE,
    SA_LIST_HEADER_FORMAT, SA_LIST_HEADER_SIZE # Added for full consume_activity_event_body
)
import asyncio # for to_thread

if config_manager.MATPLOTLIB_AVAILABLE:
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
else:
    plt = None 
    mdates = None

# Helper function to generate and send plot
async def _send_plot_if_available_local(ctx: commands.Context, fig, filename_prefix: str):
    if not config_manager.MATPLOTLIB_AVAILABLE or not fig or plt is None:
        if fig and plt: plt.close(fig) 
        config_manager.logger.warning(f"_send_plot_if_available: Matplotlib not available or figure is None. Plot '{filename_prefix}' not sent.")
        return False 

    try:
        img_bytes = io.BytesIO()
        current_facecolor = fig.get_facecolor()
        save_facecolor = current_facecolor if current_facecolor != (0.0, 0.0, 0.0, 0.0) else '#2C2F33'
        
        fig.savefig(img_bytes, format='png', bbox_inches='tight', facecolor=save_facecolor)
        img_bytes.seek(0)
        
        plot_filename = f"{filename_prefix}_{datetime.now().strftime('%Y%m%d%H%M%S')}.png"
        discord_file = discord.File(fp=img_bytes, filename=plot_filename)

        await ctx.send(file=discord_file) 
        config_manager.logger.info(f"Sent plot: {plot_filename}")
        return True 

    except Exception as e:
        config_manager.logger.error(f"Error in _send_plot_if_available_local for {filename_prefix}: {e}", exc_info=True)
        await ctx.send(f"Sorry, an error occurred while generating or sending the plot for {filename_prefix}: {e}")
        return False
    finally:
        if fig and plt: 
            plt.close(fig)


# --- Copied from data_logging.py for standalone _consume_activity_event_body_local ---
# These are simplified versions just for consuming bytes in plot_cog.
def _read_string_from_file_handle_sync_local_plot(file_handle): 
    len_bytes = file_handle.read(SA_STRING_LEN_SIZE)
    if len(len_bytes) < SA_STRING_LEN_SIZE: return None, True
    s_len = struct.unpack(SA_STRING_LEN_FORMAT, len_bytes)[0]
    s_bytes = file_handle.read(s_len)
    if len(s_bytes) < s_len: return None, True
    return s_bytes.decode('utf-8', errors='replace'), False

def _read_tag_list_from_file_handle_sync_local_plot(file_handle):
    num_tags_bytes = file_handle.read(SA_LIST_HEADER_SIZE)
    if len(num_tags_bytes) < SA_LIST_HEADER_SIZE: return [], True
    num_tags = struct.unpack(SA_LIST_HEADER_FORMAT, num_tags_bytes)[0]
    tags_read = []
    for _ in range(num_tags):
        tag_str, incomplete = _read_string_from_file_handle_sync_local_plot(file_handle)
        if incomplete: return tags_read, True
        tags_read.append(tag_str)
    return tags_read, False

def _consume_activity_event_body_local_plot(f, event_type): 
    try: 
        if event_type == config_manager.EVENT_TYPE_STREAM_START:
            _, i1 = _read_string_from_file_handle_sync_local_plot(f) # title
            _, i2 = _read_string_from_file_handle_sync_local_plot(f) # game
            _, i3 = _read_tag_list_from_file_handle_sync_local_plot(f) # tags
            # Consume optional YT ID string
            current_pos = f.tell()
            f.seek(0, os.SEEK_END)
            file_end_pos = f.tell()
            f.seek(current_pos)
            i4 = False
            if file_end_pos - current_pos >= SA_STRING_LEN_SIZE:
                peek_len_bytes = f.read(SA_STRING_LEN_SIZE)
                peek_s_len = struct.unpack(SA_STRING_LEN_FORMAT, peek_len_bytes)[0]
                f.seek(current_pos)
                if file_end_pos - current_pos >= SA_STRING_LEN_SIZE + peek_s_len:
                    _, i4_attempt = _read_string_from_file_handle_sync_local_plot(f)
                    i4 = i4_attempt
            return i1 or i2 or i3 or i4
        elif event_type == config_manager.EVENT_TYPE_STREAM_END: 
            return len(f.read(SA_INT_SIZE * 2)) < SA_INT_SIZE * 2
        elif event_type == config_manager.EVENT_TYPE_GAME_CHANGE: 
            _, i1 = _read_string_from_file_handle_sync_local_plot(f); _, i2 = _read_string_from_file_handle_sync_local_plot(f); return i1 or i2
        elif event_type == config_manager.EVENT_TYPE_TITLE_CHANGE: 
            _, i1 = _read_string_from_file_handle_sync_local_plot(f); _, i2 = _read_string_from_file_handle_sync_local_plot(f); return i1 or i2
        elif event_type == config_manager.EVENT_TYPE_TAGS_CHANGE:
             _, i1 = _read_tag_list_from_file_handle_sync_local_plot(f); _, i2 = _read_tag_list_from_file_handle_sync_local_plot(f); return i1 or i2
        else: 
            config_manager.logger.warning(f"PlotCog ConsumeHelper: Unknown event type {event_type}. Cannot reliably consume."); return True 
    except Exception as e: 
        config_manager.logger.error(f"PlotCog ConsumeHelper: Error consuming event type {event_type}: {e}"); return True


class PlotCog(commands.Cog, name="Plotting Commands"):
    def __init__(self, bot_instance):
        self.bot = bot_instance

    async def cog_check(self, ctx):
        if not config_manager.MATPLOTLIB_AVAILABLE:
            await ctx.send("Plotting library (matplotlib) is not installed on the bot. This command is unavailable.")
            return False
        if ctx.guild and config_manager.FCTD_COMMAND_CHANNEL_ID is not None and \
           ctx.channel.id != config_manager.FCTD_COMMAND_CHANNEL_ID:
            return False
        return True

    @commands.command(name="plotfollowers", help="Plots follower count over time. Usage: !plotfollowers <period|all>")
    @commands.is_owner() 
    async def plot_followers_command(self, ctx: commands.Context, *, duration_input: str = "all"):
        if not config_manager.FCTD_FOLLOWER_DATA_FILE or not os.path.exists(config_manager.FCTD_FOLLOWER_DATA_FILE):
            await ctx.send("Follower data file not found or not configured. Cannot generate plot.")
            return

        period_name_display = "all time"
        query_start_unix = None 
        now_utc = datetime.now(timezone.utc)
        now_utc_unix = int(now_utc.timestamp())

        if duration_input.lower() != "all":
            delta, parsed_period_name = parse_duration_to_timedelta(duration_input)
            if not delta:
                await ctx.send(parsed_period_name) 
                return
            query_start_unix = int((now_utc - delta).timestamp())
            period_name_display = parsed_period_name
        
        await ctx.send(f"Generating follower plot for {config_manager.FCTD_TWITCH_USERNAME or 'configured user'} ({period_name_display})... This may take a moment.")
        
        async with ctx.typing():
            plot_timestamps = []
            plot_counts = []
            try:
                with open(config_manager.FCTD_FOLLOWER_DATA_FILE, 'rb') as f:
                    while True:
                        chunk = f.read(BINARY_RECORD_SIZE)
                        if not chunk: break
                        if len(chunk) < BINARY_RECORD_SIZE: break 
                        
                        ts, count_val = struct.unpack(BINARY_RECORD_FORMAT, chunk)
                        if query_start_unix and ts < query_start_unix:
                            continue 
                        if ts > now_utc_unix + 3600 : 
                            continue
                        
                        plot_timestamps.append(datetime.fromtimestamp(ts, tz=timezone.utc))
                        plot_counts.append(count_val)
            except FileNotFoundError: 
                await ctx.send(f"Error: Follower data file '{config_manager.FCTD_FOLLOWER_DATA_FILE}' not found during plot generation."); return
            except Exception as e_read_plot:
                await ctx.send(f"An error occurred while reading follower data for the plot: {e_read_plot}"); return
            
            config_manager.logger.info(f"Plotting {len(plot_counts)} data points for plotfollowers ({period_name_display}). Min: {min(plot_counts) if plot_counts else 'N/A'}, Max: {max(plot_counts) if plot_counts else 'N/A'}")
            if not plot_timestamps or len(plot_timestamps) < 2: 
                await ctx.send("Not enough follower data found for the specified period to generate a meaningful plot."); return
            
            fig, ax = plt.subplots(figsize=(12, 6)) 
            ax.plot(plot_timestamps, plot_counts, marker='.', linestyle='-', markersize=4, color='cyan')
            
            title_text = f"Follower Count for {config_manager.FCTD_TWITCH_USERNAME or 'User'} ({period_name_display})"
            ax.set_title(title_text, color='white', fontsize=14)
            ax.set_xlabel("Date/Time (UTC)", color='lightgrey', fontsize=10)
            ax.set_ylabel("Follower Count", color='lightgrey', fontsize=10)
            
            ax.ticklabel_format(style='plain', axis='y', useOffset=False) 
            ax.grid(True, linestyle=':', alpha=0.7, color='gray') 
            
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M'))
            ax.tick_params(axis='x', colors='lightgrey', labelsize=8, labelrotation=30)
            ax.tick_params(axis='y', colors='lightgrey', labelsize=8)
            
            plt.setp(ax.get_xticklabels(), ha="right", rotation_mode="anchor") 

            fig.patch.set_facecolor('#2C2F33')
            ax.set_facecolor('#2C2F33')
            
            ax.spines['top'].set_visible(False)
            ax.spines['right'].set_visible(False)
            ax.spines['bottom'].set_color('grey')
            ax.spines['left'].set_color('grey')
            
            plt.tight_layout() 
            
            if not await _send_plot_if_available_local(ctx, fig, f"followers_plot_{config_manager.FCTD_TWITCH_USERNAME or 'user'}"):
                pass


    @commands.command(name="plotstreamdurations", help="Plots histogram of stream durations. Usage: !plotstreamdurations <period|all>")
    @commands.is_owner() 
    async def plot_stream_durations_command(self, ctx: commands.Context, *, duration_input: str = "all"):
        target_file, data_source_name, is_activity_log_source = None, "", False
        if config_manager.UTA_STREAM_ACTIVITY_LOG_FILE and \
           os.path.exists(config_manager.UTA_STREAM_ACTIVITY_LOG_FILE) and \
           config_manager.UTA_STREAM_STATUS_NOTIFICATIONS_ENABLED: 
            target_file = config_manager.UTA_STREAM_ACTIVITY_LOG_FILE
            data_source_name = "Stream Activity Durations"
            is_activity_log_source = True
        elif config_manager.UTA_STREAM_DURATION_LOG_FILE and \
             os.path.exists(config_manager.UTA_STREAM_DURATION_LOG_FILE) and \
             config_manager.UTA_RESTREAMER_ENABLED: 
            target_file = config_manager.UTA_STREAM_DURATION_LOG_FILE
            data_source_name = "Restream Durations (VOD Parts)"
        else:
            await ctx.send(f"No suitable stream duration data file found. Checked UTA Activity Log (requires Status Notifications enabled) and Restream Log (requires Restreamer enabled).")
            return

        period_name_display = "all time"
        now_utc = datetime.now(timezone.utc)
        query_start_unix, query_end_unix = 0, int(now_utc.timestamp()) 

        if duration_input.lower() != "all":
            delta, parsed_period_name = parse_duration_to_timedelta(duration_input)
            if not delta:
                await ctx.send(parsed_period_name); return
            query_start_unix = int((now_utc - delta).timestamp())
            period_name_display = parsed_period_name
        
        await ctx.send(f"Generating {data_source_name} plot for {config_manager.UTA_TWITCH_CHANNEL_NAME or 'configured channel'} ({period_name_display})...")
        
        async with ctx.typing():
            durations_in_hours = []
            try:
                if is_activity_log_source: 
                    active_stream_start_ts = None
                    with open(target_file, 'rb') as f:
                        file_total_size = os.fstat(f.fileno()).st_size
                        while True:
                            current_offset = f.tell()
                            if current_offset + SA_BASE_HEADER_SIZE > file_total_size: break
                            header_chunk = f.read(SA_BASE_HEADER_SIZE)
                            if not header_chunk: break
                            
                            event_type, ts_event = struct.unpack(SA_BASE_HEADER_FORMAT, header_chunk)
                            incomplete_event_body = False

                            is_relevant_for_parsing = True
                            if query_start_unix != 0 and ts_event < query_start_unix - (86400 * 14): 
                                is_relevant_for_parsing = False
                            if ts_event > query_end_unix + (86400 * 1) : 
                                is_relevant_for_parsing = False

                            if event_type == EVENT_TYPE_STREAM_START:
                                if is_relevant_for_parsing:
                                    if ts_event < query_end_unix + 86400: 
                                         active_stream_start_ts = ts_event
                                incomplete_event_body = _consume_activity_event_body_local_plot(f, event_type)
                            elif event_type == EVENT_TYPE_STREAM_END:
                                if active_stream_start_ts is not None:
                                    stream_s_ts, stream_e_ts = active_stream_start_ts, ts_event
                                    is_relevant_to_query = True
                                    if query_start_unix != 0 and stream_e_ts < query_start_unix : is_relevant_to_query = False
                                    if stream_s_ts > query_end_unix : is_relevant_to_query = False
                                    
                                    if is_relevant_to_query:
                                        eff_s = max(stream_s_ts, query_start_unix) if query_start_unix !=0 else stream_s_ts
                                        eff_e = min(stream_e_ts, query_end_unix)
                                        if eff_e > eff_s:
                                            durations_in_hours.append((eff_e - eff_s) / 3600.0)
                                active_stream_start_ts = None 
                                incomplete_event_body = _consume_activity_event_body_local_plot(f, event_type)
                            elif is_relevant_for_parsing: 
                                incomplete_event_body = _consume_activity_event_body_local_plot(f, event_type)
                            else: # Efficiently skip body for very old/future events
                                f.seek(current_offset + SA_BASE_HEADER_SIZE) # Go to start of body
                                # This is a simplified consumption just to advance pointer. Real consume_activity_event_body might be more robust.
                                # For now, we assume _consume_activity_event_body_local_plot is smart enough.
                                _consume_activity_event_body_local_plot(f, event_type) 

                            if incomplete_event_body:
                                config_manager.logger.warning(f"PlotStreamDurations (Activity): Incomplete body for event {event_type} at {ts_event}. Stopping read for this file.")
                                f.seek(current_offset); break 
                    
                    if active_stream_start_ts and (active_stream_start_ts < query_end_unix or query_start_unix == 0) :
                        eff_s_ongoing = max(active_stream_start_ts, query_start_unix) if query_start_unix !=0 else active_stream_start_ts
                        eff_e_ongoing = query_end_unix 
                        if eff_e_ongoing > eff_s_ongoing:
                            durations_in_hours.append((eff_e_ongoing - eff_s_ongoing) / 3600.0)

                else: 
                    with open(target_file, 'rb') as f:
                        while True:
                            chunk = f.read(STREAM_DURATION_RECORD_SIZE)
                            if not chunk: break
                            if len(chunk) < STREAM_DURATION_RECORD_SIZE: break
                            
                            s_ts, e_ts = struct.unpack(STREAM_DURATION_RECORD_FORMAT, chunk)
                            is_relevant_to_query = True
                            if query_start_unix != 0 and e_ts < query_start_unix : is_relevant_to_query = False
                            if s_ts > query_end_unix : is_relevant_to_query = False
                            
                            if is_relevant_to_query:
                                eff_s_ts = max(s_ts, query_start_unix) if query_start_unix != 0 else s_ts
                                eff_e_ts = min(e_ts, query_end_unix)
                                if eff_e_ts > eff_s_ts:
                                    durations_in_hours.append((eff_e_ts - eff_s_ts) / 3600.0)
            except FileNotFoundError:
                await ctx.send(f"Error: Data file '{target_file}' not found during plot generation."); return
            except Exception as e_read_plot_dur:
                config_manager.logger.error(f"Error reading duration data for plot: {e_read_plot_dur}", exc_info=True)
                await ctx.send(f"An error occurred while reading duration data for the plot: {e_read_plot_dur}"); return
            
            config_manager.logger.info(f"Plotting {len(durations_in_hours)} stream durations for {data_source_name} ({period_name_display}).")
            if not durations_in_hours:
                await ctx.send(f"No {data_source_name.lower()} data found for the specified period to plot."); return
            
            fig, ax = plt.subplots(figsize=(10, 6))
            num_bins = max(1, min(20, int(len(durations_in_hours)**0.5) + 1 if len(durations_in_hours) > 4 else 5))
            if len(durations_in_hours) <= 5 : num_bins = len(durations_in_hours) 

            ax.hist(durations_in_hours, bins=num_bins, edgecolor='black', color='skyblue', rwidth=0.9)
            
            title_text = f"Histogram of {data_source_name} for {config_manager.UTA_TWITCH_CHANNEL_NAME or 'Channel'} ({period_name_display})"
            ax.set_title(title_text, color='white', fontsize=14)
            ax.set_xlabel("Duration (Hours)", color='lightgrey', fontsize=10)
            ax.set_ylabel("Number of Streams/Sessions", color='lightgrey', fontsize=10)
            ax.grid(axis='y', alpha=0.75, linestyle=':', color='gray')
            ax.tick_params(axis='x', colors='lightgrey', labelsize=8)
            ax.tick_params(axis='y', colors='lightgrey', labelsize=8)
            
            fig.patch.set_facecolor('#2C2F33')
            ax.set_facecolor('#2C2F33')
            ax.spines['top'].set_visible(False); ax.spines['right'].set_visible(False)
            ax.spines['bottom'].set_color('grey'); ax.spines['left'].set_color('grey')
            
            plt.tight_layout()
            
            if not await _send_plot_if_available_local(ctx, fig, f"stream_durations_hist_{config_manager.UTA_TWITCH_CHANNEL_NAME or 'channel'}"):
                pass 
    
    # Add plot_gamestats_histogram if gamestats from uta_info_cog will call it
    async def plot_gamestats_histogram(self, ctx: commands.Context, viewer_counts_for_plotting: list, game_name: str, period_name_display: str):
        """Generates and sends a viewer histogram for a specific game."""
        if not viewer_counts_for_plotting or len(viewer_counts_for_plotting) < 2:
            await ctx.send(f"Not enough viewer data for '{game_name}' in '{period_name_display}' to generate a histogram.")
            return None # Indicate no plot generated

        fig, ax = plt.subplots(figsize=(8, 4))
        ax.hist(viewer_counts_for_plotting, bins=15, edgecolor='black', color='skyblue')
        ax.set_title(f"Viewer Distribution for '{game_name}' ({period_name_display})", fontsize=10, color='white')
        ax.set_xlabel("Viewer Count", fontsize=9, color='lightgrey')
        ax.set_ylabel("Frequency (Data Points)", fontsize=9, color='lightgrey')
        ax.grid(True, linestyle=':', alpha=0.5, axis='y')
        ax.tick_params(labelsize=8, colors='lightgrey')
        ax.yaxis.label.set_color('lightgrey')
        ax.xaxis.label.set_color('lightgrey')

        fig.patch.set_facecolor('#2C2F33')
        ax.set_facecolor('#2C2F33')
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.spines['bottom'].set_color('grey')
        ax.spines['left'].set_color('grey')
        
        plt.tight_layout()

        img_bytes = io.BytesIO()
        current_facecolor = fig.get_facecolor()
        save_facecolor = current_facecolor if current_facecolor != (0.0, 0.0, 0.0, 0.0) else '#2C2F33'
        fig.savefig(img_bytes, format='png', bbox_inches='tight', facecolor=save_facecolor)
        img_bytes.seek(0)
        plt.close(fig)
        
        plot_filename = f"gamestats_viewers_{game_name.replace(' ','_')}_{datetime.now().strftime('%Y%m%d%H%M')}.png"
        discord_file = discord.File(fp=img_bytes, filename=plot_filename)
        return discord_file # Return the file object for the calling command to send

async def setup(bot_instance):
    if config_manager.MATPLOTLIB_AVAILABLE: 
        await bot_instance.add_cog(PlotCog(bot_instance))
    else:
        config_manager.logger.info("Matplotlib not available, PlotCog will not be loaded.")