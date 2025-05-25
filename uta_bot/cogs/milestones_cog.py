import discord
from discord.ext import commands
import asyncio
import os
from datetime import datetime, timezone
import math

from uta_bot import config_manager
from uta_bot.utils import data_logging as dl_utils # dl for data_logging
from uta_bot.utils.constants import (
    BINARY_RECORD_SIZE, STREAM_DURATION_RECORD_SIZE, CHAT_ACTIVITY_RECORD_SIZE
)
from uta_bot.utils.formatters import format_duration_human


# Helper function to generate a progress bar string
def get_progress_bar(current, target, length=10):
    if target == 0: return "[N/A]"
    progress = min(1.0, float(current) / float(target)) if current is not None else 0.0
    filled_length = int(length * progress)
    bar = '█' * filled_length + '░' * (length - filled_length)
    return f"[{bar}]"

MILESTONE_DEFINITIONS = [
    # Followers
    {
        "category": "Twitch Followers", "unit": "followers",
        "name_template": "Reach {} Followers",
        "targets": [10, 25, 50, 75, 100, 150, 200, 250, 300, 400, 500, 600, 750, 1000, 1250, 1500, 1750, 2000, 2500, 3000, 4000, 5000],
        "fetch_info": {"func_ref": dl_utils.get_latest_binary_log_value, "data_file_key": "FCTD_FOLLOWER_DATA_FILE"},
        "check_enabled": lambda: config_manager.FCTD_TWITCH_USERNAME and config_manager.FCTD_FOLLOWER_DATA_FILE
    },
    # Total Stream Time (Hours)
    {
        "category": "Streaming Time", "unit": "hours streamed",
        "name_template": "Stream for {} Hours (Total)",
        "targets": [1, 5, 10, 20, 30, 40, 50, 75, 100, 125, 150, 175, 200, 250, 300, 400, 500, 750, 1000],
        "fetch_info": {"func_ref": dl_utils.get_total_stream_time_seconds_from_activity, "data_file_key": "UTA_STREAM_ACTIVITY_LOG_FILE", "transform_func": lambda sec: sec / 3600.0 if sec is not None else 0},
        "check_enabled": lambda: config_manager.UTA_ENABLED and config_manager.UTA_STREAM_ACTIVITY_LOG_FILE and config_manager.UTA_STREAM_STATUS_NOTIFICATIONS_ENABLED
    },
    # Peak Viewers
    {
        "category": "Viewership", "unit": "peak viewers",
        "name_template": "Achieve {} Peak Viewers",
        "targets": [5, 10, 15, 20, 25, 30, 40, 50, 60, 75, 100, 125, 150, 175, 200, 250],
        "fetch_info": {"func_ref": dl_utils.get_max_value_from_binary_log, "data_file_key": "UTA_VIEWER_COUNT_LOG_FILE"},
        "check_enabled": lambda: config_manager.UTA_ENABLED and config_manager.UTA_VIEWER_COUNT_LOGGING_ENABLED and config_manager.UTA_VIEWER_COUNT_LOG_FILE
    },
    # Average Viewers (Overall)
    {
        "category": "Viewership", "unit": "avg viewers",
        "name_template": "Maintain {} Average Viewers (Overall)",
        "targets": [3, 5, 7, 10, 12, 15, 20, 25, 30, 40, 50, 60, 75],
        "fetch_info": {"func_ref": dl_utils.get_avg_value_from_binary_log, "data_file_key": "UTA_VIEWER_COUNT_LOG_FILE"},
        "check_enabled": lambda: config_manager.UTA_ENABLED and config_manager.UTA_VIEWER_COUNT_LOGGING_ENABLED and config_manager.UTA_VIEWER_COUNT_LOG_FILE
    },
    # Total Chat Messages
    {
        "category": "Chat Activity", "unit": "total messages",
        "name_template": "Receive {} Messages in Chat (Total)",
        "targets": [100, 250, 500, 1000, 2000, 3000, 4000, 5000, 7500, 10000, 15000, 20000, 25000, 30000, 40000, 50000],
        "fetch_info": {"func_ref": dl_utils.get_total_chat_messages_from_log, "data_file_key": "TWITCH_CHAT_ACTIVITY_LOG_FILE"},
        "check_enabled": lambda: config_manager.TWITCH_CHAT_ENABLED and config_manager.TWITCH_CHAT_ACTIVITY_LOG_FILE
    },
    # Peak Unique Chatters (in an interval)
    {
        "category": "Chat Activity", "unit": "peak unique chatters",
        "name_template": "Have {} Unique Chatters in a Logging Interval",
        "targets": [2, 3, 4, 5, 7, 10, 12, 15, 20, 25, 30, 40, 50],
        "fetch_info": {"func_ref": dl_utils.get_peak_unique_chatters_from_log, "data_file_key": "TWITCH_CHAT_ACTIVITY_LOG_FILE"},
        "check_enabled": lambda: config_manager.TWITCH_CHAT_ENABLED and config_manager.TWITCH_CHAT_ACTIVITY_LOG_FILE
    },
    # VODs/Parts Archived
    {
        "category": "Content Archival", "unit": "VODs/parts archived",
        "name_template": "Archive {} VODs/Stream Parts",
        "targets": [1, 5, 10, 15, 20, 25, 30, 40, 50, 60, 75, 100, 125, 150, 175, 200],
        "fetch_info": {"func_ref": dl_utils.count_records_in_file, "data_file_key": "UTA_STREAM_DURATION_LOG_FILE", "record_size": STREAM_DURATION_RECORD_SIZE},
        "check_enabled": lambda: config_manager.UTA_ENABLED and config_manager.UTA_RESTREAMER_ENABLED and config_manager.UTA_STREAM_DURATION_LOG_FILE
    },
    # Distinct Games Streamed
    {
        "category": "Content Variety", "unit": "distinct games streamed",
        "name_template": "Stream {} Distinct Games",
        "targets": [1, 2, 3, 4, 5, 7, 10, 12, 15, 20, 25, 30, 35, 40, 45, 50],
        "fetch_info": {"func_ref": dl_utils.count_distinct_games_from_activity, "data_file_key": "UTA_STREAM_ACTIVITY_LOG_FILE"},
        "check_enabled": lambda: config_manager.UTA_ENABLED and config_manager.UTA_STREAM_ACTIVITY_LOG_FILE and config_manager.UTA_STREAM_STATUS_NOTIFICATIONS_ENABLED
    },
]

class MilestonesCog(commands.Cog, name="Channel Milestones"):
    def __init__(self, bot_instance):
        self.bot = bot_instance
        self._unrolled_milestones = []
        self._generate_milestones_list()
        self._category_names = sorted(list(set(m_def["category"] for m_def in MILESTONE_DEFINITIONS)))

    def _generate_milestones_list(self):
        self._unrolled_milestones = []
        for definition in MILESTONE_DEFINITIONS:
            category_display_name = definition["category"] # Store the original category name for display
            for target_value in definition["targets"]:
                unique_id = f"{definition['category'].lower().replace(' ', '_')}_{target_value}"
                self._unrolled_milestones.append({
                    "id": unique_id,
                    "name": definition["name_template"].format(target_value),
                    "category": definition["category"],
                    "target": target_value,
                    "target_display": definition["name_template"].split('{}', 1)[1].strip(), # "Followers", "Hours (Total)"
                    "unit": definition["unit"],
                    "fetch_info": definition["fetch_info"],
                    "check_enabled_func": definition["check_enabled"],
                    "current_value": None,
                    "progress_percent": 0.0,
                    "completed": False,
                    "is_applicable": True, # Will be set to False if check_enabled fails
                    "error_msg": None
                })
        config_manager.logger.info(f"Generated {len(self._unrolled_milestones)} total milestones.")


    async def _fetch_all_milestone_data(self):
        """Fetches current values for all applicable milestones."""
        grouped_fetches = {} # Group by fetch_info to avoid redundant reads for same file/func

        for m_idx, m_data in enumerate(self._unrolled_milestones):
            if not m_data["check_enabled_func"]():
                self._unrolled_milestones[m_idx]["is_applicable"] = False
                self._unrolled_milestones[m_idx]["error_msg"] = "Feature disabled or data file not configured."
                continue
            
            self._unrolled_milestones[m_idx]["is_applicable"] = True # Reset applicability
            fetch_key_parts = [m_data["fetch_info"]["func_ref"].__name__]
            if "data_file_key" in m_data["fetch_info"]:
                fetch_key_parts.append(m_data["fetch_info"]["data_file_key"])
            if "record_size" in m_data["fetch_info"]:
                 fetch_key_parts.append(str(m_data["fetch_info"]["record_size"]))
            fetch_key = tuple(fetch_key_parts)

            if fetch_key not in grouped_fetches:
                grouped_fetches[fetch_key] = {
                    "fetch_info": m_data["fetch_info"],
                    "value": None, # To be filled
                    "error": None  # To be filled
                }
        
        # Perform the actual data fetches
        for fetch_key, data_to_fetch in grouped_fetches.items():
            fetch_info = data_to_fetch["fetch_info"]
            func_ref = fetch_info["func_ref"]
            data_file_key = fetch_info.get("data_file_key")
            record_size = fetch_info.get("record_size")
            transform_func = fetch_info.get("transform_func")

            try:
                current_value = None
                if data_file_key:
                    data_file_path = getattr(config_manager, data_file_key, None)
                    if not data_file_path or not os.path.exists(data_file_path):
                        data_to_fetch["error"] = f"Data file '{data_file_key}' not found or configured."
                        continue # Skip this fetch
                    
                    if record_size: # For functions like count_records_in_file
                        current_value = await asyncio.to_thread(func_ref, data_file_path, record_size)
                    else:
                        current_value = await asyncio.to_thread(func_ref, data_file_path)
                else: # For functions that don't take a filepath (if any)
                    current_value = await asyncio.to_thread(func_ref)
                
                if transform_func and current_value is not None:
                    current_value = transform_func(current_value)
                
                data_to_fetch["value"] = current_value

            except Exception as e:
                config_manager.logger.error(f"Error fetching data for {fetch_key}: {e}", exc_info=True)
                data_to_fetch["error"] = str(e)

        # Apply fetched data back to unrolled_milestones
        for m_idx, m_data in enumerate(self._unrolled_milestones):
            if not m_data["is_applicable"]:
                continue

            fetch_key_parts = [m_data["fetch_info"]["func_ref"].__name__]
            if "data_file_key" in m_data["fetch_info"]:
                fetch_key_parts.append(m_data["fetch_info"]["data_file_key"])
            if "record_size" in m_data["fetch_info"]:
                 fetch_key_parts.append(str(m_data["fetch_info"]["record_size"]))
            fetch_key = tuple(fetch_key_parts)
            
            fetched_group_data = grouped_fetches.get(fetch_key)
            if fetched_group_data:
                if fetched_group_data["error"]:
                    self._unrolled_milestones[m_idx]["error_msg"] = fetched_group_data["error"]
                    self._unrolled_milestones[m_idx]["current_value"] = None
                else:
                    self._unrolled_milestones[m_idx]["current_value"] = fetched_group_data["value"]
                    self._unrolled_milestones[m_idx]["error_msg"] = None # Clear previous error
            else: # Should not happen if logic is correct
                self._unrolled_milestones[m_idx]["error_msg"] = "Internal error: Fetch group not found."

            # Calculate progress
            current_val = self._unrolled_milestones[m_idx]["current_value"]
            target_val = m_data["target"]
            if current_val is not None and target_val > 0:
                self._unrolled_milestones[m_idx]["progress_percent"] = min(100.0, (float(current_val) / float(target_val)) * 100.0)
                self._unrolled_milestones[m_idx]["completed"] = current_val >= target_val
            elif current_val is not None and target_val == 0: # Edge case: target is 0
                 self._unrolled_milestones[m_idx]["progress_percent"] = 100.0 if current_val >= 0 else 0.0
                 self._unrolled_milestones[m_idx]["completed"] = current_val >= 0
            else:
                self._unrolled_milestones[m_idx]["progress_percent"] = 0.0
                self._unrolled_milestones[m_idx]["completed"] = False


    def _format_milestone_entry(self, m_data, show_category=False):
        """Helper to format a single milestone entry string."""
        if not m_data["is_applicable"]:
            return f"⚪ **{m_data['name']}**: N/A ({m_data.get('error_msg', 'Configuration/data issue')})"

        emoji = "✅" if m_data["completed"] else ("🚧" if m_data["progress_percent"] > 50 else "⏳")
        
        current_val_str = "N/A"
        if m_data["current_value"] is not None:
            if isinstance(m_data['current_value'], float):
                current_val_str = f"{m_data['current_value']:.1f}" if m_data['current_value'] % 1 != 0 else f"{m_data['current_value']:.0f}"
            else:
                current_val_str = str(m_data['current_value'])

        target_val_str = f"{m_data['target']:.0f}" if isinstance(m_data['target'], float) and m_data['target'].is_integer() else f"{m_data['target']:.1f}" if isinstance(m_data['target'], float) else str(m_data['target'])
        
        progress_bar = get_progress_bar(m_data['current_value'], m_data['target'])
        
        line1 = f"{emoji} **{m_data['name']}**"
        if show_category:
            line1 += f" ({m_data['category']})"
        
        line2 = f"   {progress_bar} {current_val_str} / {target_val_str} {m_data['unit']} ({m_data['progress_percent']:.1f}%)"
        if m_data["current_value"] is None and m_data.get("error_msg"):
            line2 += f" (Error: {m_data['error_msg']})"
        elif m_data["current_value"] is None:
            line2 += " (No data yet)"
            
        return f"{line1}\n{line2}"

    @commands.command(name="milestones", help="Progress towards milestones. Usage: !milestones [all|<Category Name>]")
    async def milestones_command(self, ctx: commands.Context, *, filter_input: str = None):
        async with ctx.typing():
            await self._fetch_all_milestone_data() # Fetches and updates self._unrolled_milestones

            total_milestones = len(self._unrolled_milestones)
            completed_milestones_count = sum(1 for m in self._unrolled_milestones if m["completed"] and m["is_applicable"])
            applicable_milestones_count = sum(1 for m in self._unrolled_milestones if m["is_applicable"])
            
            channel_name_for_title = config_manager.UTA_TWITCH_CHANNEL_NAME or 'Your Channel'

            # Default: Summary View
            if filter_input is None:
                embed = discord.Embed(
                    title=f"🏆 Channel Milestones Summary for {channel_name_for_title}",
                    color=discord.Color.gold()
                )
                overall_progress_msg = f"{completed_milestones_count} / {applicable_milestones_count} applicable milestones achieved."
                if applicable_milestones_count < total_milestones:
                    overall_progress_msg += f" ({total_milestones - applicable_milestones_count} N/A due to config/data)."
                embed.description = overall_progress_msg

                up_next = sorted(
                    [m for m in self._unrolled_milestones if not m["completed"] and m["is_applicable"] and m["current_value"] is not None],
                    key=lambda m: m["progress_percent"],
                    reverse=True
                )[:5]

                if up_next:
                    up_next_text = "\n\n".join([self._format_milestone_entry(m) for m in up_next])
                    embed.add_field(name="🚀 Up Next", value=up_next_text, inline=False)
                
                recently_achieved = sorted(
                    [m for m in self._unrolled_milestones if m["completed"] and m["is_applicable"]],
                    key=lambda m: (m["category"], m["target"]), # Sort by category then by target value for consistency
                    reverse=True
                )[:5]

                if recently_achieved:
                    achieved_text = "\n".join([f"✅ **{m['name']}** ({m['category']})" for m in recently_achieved])
                    embed.add_field(name="🎉 Recently Achieved", value=achieved_text, inline=False)

                if not up_next and not recently_achieved and applicable_milestones_count > 0:
                     embed.add_field(name="Status", value="No specific milestones to highlight right now for 'Up Next' or 'Recently Achieved'.", inline=False)
                elif applicable_milestones_count == 0:
                    embed.add_field(name="Status", value="No milestones are currently applicable based on your configuration.", inline=False)

                embed.set_footer(text=f"Use '{config_manager.FCTD_COMMAND_PREFIX}milestones all' or '{config_manager.FCTD_COMMAND_PREFIX}milestones <Category Name>' for more.")
                embed.timestamp = datetime.now(timezone.utc)
                await ctx.send(embed=embed)

            # Show All Milestones (Paginated by Category)
            elif filter_input.lower() == "all":
                await ctx.send(f"📋 **All Milestones for {channel_name_for_title}** ({completed_milestones_count}/{applicable_milestones_count} applicable achieved):")
                
                milestones_by_cat = {}
                for m_data in self._unrolled_milestones:
                    if m_data["category"] not in milestones_by_cat:
                        milestones_by_cat[m_data["category"]] = []
                    milestones_by_cat[m_data["category"]].append(m_data)

                for cat_name in self._category_names: # Iterate in defined order
                    if cat_name not in milestones_by_cat:
                        continue

                    cat_milestones = sorted(milestones_by_cat[cat_name], key=lambda m: m["target"])
                    
                    # Split milestones for this category into chunks if too many for one embed field value
                    field_value_parts = []
                    current_field_value = ""
                    for m_data in cat_milestones:
                        entry_str = self._format_milestone_entry(m_data) + "\n"
                        if len(current_field_value) + len(entry_str) > 1020: # Discord field value limit is 1024
                            field_value_parts.append(current_field_value)
                            current_field_value = entry_str
                        else:
                            current_field_value += entry_str
                    if current_field_value:
                        field_value_parts.append(current_field_value)

                    cat_embed = discord.Embed(title=f"Category: {cat_name}", color=discord.Color.blue())
                    cat_embed.set_author(name=f"All Milestones for {channel_name_for_title}")

                    if not field_value_parts:
                         cat_embed.description = "No milestones defined or applicable for this category."
                    else:
                        for i, part_text in enumerate(field_value_parts):
                            field_name = f"Milestones (Part {i+1})" if len(field_value_parts) > 1 else "Milestones"
                            cat_embed.add_field(name=field_name, value=part_text.strip(), inline=False)
                    
                    cat_embed.timestamp = datetime.now(timezone.utc)
                    await ctx.send(embed=cat_embed)
                    await asyncio.sleep(0.5) # Small delay to avoid rate limits with many embeds

            # Show Specific Category
            else:
                target_category_name = None
                for cat_n in self._category_names:
                    if filter_input.lower() == cat_n.lower():
                        target_category_name = cat_n
                        break
                
                if target_category_name:
                    cat_milestones = sorted(
                        [m for m in self._unrolled_milestones if m["category"] == target_category_name],
                        key=lambda m: m["target"]
                    )
                    
                    cat_embed = discord.Embed(
                        title=f"🏆 Milestones: {target_category_name} for {channel_name_for_title}",
                        color=discord.Color.green()
                    )
                    
                    field_value_parts = []
                    current_field_value = ""
                    for m_data in cat_milestones:
                        entry_str = self._format_milestone_entry(m_data) + "\n"
                        if len(current_field_value) + len(entry_str) > 1020:
                            field_value_parts.append(current_field_value)
                            current_field_value = entry_str
                        else:
                            current_field_value += entry_str
                    if current_field_value:
                        field_value_parts.append(current_field_value)

                    if not field_value_parts:
                         cat_embed.description = "No milestones defined or applicable for this category."
                    else:
                        for i, part_text in enumerate(field_value_parts):
                            field_name = f"Details (Part {i+1})" if len(field_value_parts) > 1 else "Details"
                            cat_embed.add_field(name=field_name, value=part_text.strip(), inline=False)
                    
                    cat_embed.timestamp = datetime.now(timezone.utc)
                    await ctx.send(embed=cat_embed)
                else:
                    valid_categories_str = ", ".join([f"`{c}`" for c in self._category_names])
                    await ctx.send(f"Invalid category '{filter_input}'. Valid categories are: {valid_categories_str}, or use `all`.")

async def setup(bot_instance):
    await bot_instance.add_cog(MilestonesCog(bot_instance))