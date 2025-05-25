from .constants import *
from .formatters import format_duration_human, parse_duration_to_timedelta
from .data_logging import (
    log_follower_data_binary,
    log_viewer_data_binary,
    log_stream_duration_binary,
    log_stream_activity_binary,
    log_bot_session_event,
    read_and_find_records_for_period,
    get_counts_for_day_boundaries,
    read_stream_durations_for_period,
    get_viewer_stats_for_period,
    parse_stream_activity_for_game_segments,
    calculate_bot_runtime_in_period,
    read_string_from_file_handle,
    read_tag_list_from_file_handle,
    consume_activity_event_body,
    # New helpers for milestones
    get_latest_binary_log_value,
    get_total_stream_time_seconds_from_activity,
    get_max_value_from_binary_log,
    get_avg_value_from_binary_log,
    get_total_chat_messages_from_log,
    get_peak_unique_chatters_from_log,
    count_records_in_file,
    count_distinct_games_from_activity
)
from .chapter_utils import generate_chapter_text, format_seconds_to_hhmmss