import logging
import struct
import os
import asyncio
from datetime import datetime, timezone, date
import discord


from uta_bot import config_manager
from .constants import (
    BINARY_RECORD_FORMAT, BINARY_RECORD_SIZE,
    STREAM_DURATION_RECORD_FORMAT, STREAM_DURATION_RECORD_SIZE,
    CHAT_ACTIVITY_RECORD_FORMAT, CHAT_ACTIVITY_RECORD_SIZE, # Added
    EVENT_TYPE_STREAM_START, EVENT_TYPE_STREAM_END, EVENT_TYPE_GAME_CHANGE,
    EVENT_TYPE_TITLE_CHANGE, EVENT_TYPE_TAGS_CHANGE,
    SA_BASE_HEADER_FORMAT, SA_BASE_HEADER_SIZE,
    SA_STRING_LEN_FORMAT, SA_STRING_LEN_SIZE,
    SA_LIST_HEADER_FORMAT, SA_LIST_HEADER_SIZE,
    SA_INT_FORMAT, SA_INT_SIZE,
    BOT_EVENT_START, BOT_EVENT_STOP,
    BOT_SESSION_RECORD_FORMAT, BOT_SESSION_RECORD_SIZE
)

logger = logging.getLogger(__name__)

def _write_binary_data_sync(filepath: str, data_bytes: bytes):
    try:
        with open(filepath, 'ab') as f:
            f.write(data_bytes)
    except Exception as e:
        logger.error(f"Error writing binary data to {filepath}: {e}", exc_info=True)

async def log_follower_data_binary(timestamp_dt: datetime, count: int):
    if config_manager.FCTD_FOLLOWER_DATA_FILE:
        try:
            packed_data = struct.pack(BINARY_RECORD_FORMAT, int(timestamp_dt.timestamp()), int(count))
            await asyncio.to_thread(_write_binary_data_sync, config_manager.FCTD_FOLLOWER_DATA_FILE, packed_data)
            logger.debug(f"Logged follower count {count} at {timestamp_dt.isoformat()} to {config_manager.FCTD_FOLLOWER_DATA_FILE}")
        except Exception as e:
            logger.error(f"Failed to log follower data to {config_manager.FCTD_FOLLOWER_DATA_FILE}: {e}", exc_info=True)

async def log_viewer_data_binary(timestamp_dt: datetime, count: int):
    if config_manager.UTA_VIEWER_COUNT_LOGGING_ENABLED and config_manager.UTA_VIEWER_COUNT_LOG_FILE:
        try:
            packed_data = struct.pack(BINARY_RECORD_FORMAT, int(timestamp_dt.timestamp()), int(count))
            await asyncio.to_thread(_write_binary_data_sync, config_manager.UTA_VIEWER_COUNT_LOG_FILE, packed_data)
            logger.debug(f"UTA: Logged viewer count {count} at {timestamp_dt.isoformat()} to {config_manager.UTA_VIEWER_COUNT_LOG_FILE}")
        except Exception as e:
            logger.error(f"UTA: Failed to log viewer count to {config_manager.UTA_VIEWER_COUNT_LOG_FILE}: {e}", exc_info=True)

async def log_stream_duration_binary(start_ts_unix: int, end_ts_unix: int):
    if config_manager.UTA_STREAM_DURATION_LOG_FILE and config_manager.UTA_ENABLED and config_manager.UTA_RESTREAMER_ENABLED:
        if end_ts_unix <= start_ts_unix:
            logger.warning(f"UTA: Invalid stream duration log attempt: start_ts={start_ts_unix}, end_ts={end_ts_unix}. Skipping.")
            return
        try:
            packed_data = struct.pack(STREAM_DURATION_RECORD_FORMAT, start_ts_unix, end_ts_unix)
            await asyncio.to_thread(_write_binary_data_sync, config_manager.UTA_STREAM_DURATION_LOG_FILE, packed_data)
            start_dt_iso = datetime.fromtimestamp(start_ts_unix, tz=timezone.utc).isoformat()
            end_dt_iso = datetime.fromtimestamp(end_ts_unix, tz=timezone.utc).isoformat()
            logger.info(f"UTA: Logged restream duration: {start_dt_iso} to {end_dt_iso}")
        except Exception as e:
            logger.error(f"UTA: Failed to log stream duration to {config_manager.UTA_STREAM_DURATION_LOG_FILE}: {e}", exc_info=True)

async def log_chat_activity_binary(timestamp_dt: datetime, message_count: int, unique_chatters_count: int):
    """Logs aggregated chat activity data to a binary file."""
    if not (config_manager.TWITCH_CHAT_ENABLED and config_manager.TWITCH_CHAT_ACTIVITY_LOG_FILE):
        return

    try:
        ts_unix = int(timestamp_dt.timestamp())
        # Ensure counts fit into unsigned short (H in struct format)
        message_count_packed = min(message_count, 65535)
        unique_chatters_count_packed = min(unique_chatters_count, 65535)

        packed_data = struct.pack(CHAT_ACTIVITY_RECORD_FORMAT, ts_unix, message_count_packed, unique_chatters_count_packed)
        await asyncio.to_thread(_write_binary_data_sync, config_manager.TWITCH_CHAT_ACTIVITY_LOG_FILE, packed_data)
        logger.debug(f"UTA Chat: Logged chat activity: {message_count_packed} msgs, {unique_chatters_count_packed} unique chatters at {timestamp_dt.isoformat()}")
    except Exception as e:
        logger.error(f"UTA Chat: Failed to log chat activity to {config_manager.TWITCH_CHAT_ACTIVITY_LOG_FILE}: {e}", exc_info=True)


def _pack_string_for_binary_log(s: str) -> bytes:
    s_bytes = s.encode('utf-8')
    len_bytes = struct.pack(SA_STRING_LEN_FORMAT, len(s_bytes))
    return len_bytes + s_bytes

def _pack_tag_list_for_binary_log(tags: list[str] = None) -> bytes:
    tags_to_pack = tags if tags is not None else []
    num_tags = len(tags_to_pack)
    header_bytes = struct.pack(SA_LIST_HEADER_FORMAT, num_tags)

    tag_bytes_list = [header_bytes]
    for tag_str in tags_to_pack:
        tag_bytes_list.append(_pack_string_for_binary_log(tag_str))
    return b"".join(tag_bytes_list)

async def log_stream_activity_binary(event_type: int, timestamp_dt: datetime, **kwargs):
    if not (config_manager.UTA_STREAM_ACTIVITY_LOG_FILE and config_manager.UTA_STREAM_STATUS_NOTIFICATIONS_ENABLED):
        return

    try:
        ts_unix = int(timestamp_dt.timestamp())
        log_entry_bytes_list = [struct.pack(SA_BASE_HEADER_FORMAT, event_type, ts_unix)]

        if event_type == EVENT_TYPE_STREAM_START:
            title = kwargs.get("title", "")
            game = kwargs.get("game", "")
            tags = kwargs.get("tags", [])
            youtube_video_id = kwargs.get("youtube_video_id") # This is new for stream start

            log_entry_bytes_list.append(_pack_string_for_binary_log(title))
            log_entry_bytes_list.append(_pack_string_for_binary_log(game))
            log_entry_bytes_list.append(_pack_tag_list_for_binary_log(tags))
            if youtube_video_id is not None: # Handle optional field
                log_entry_bytes_list.append(_pack_string_for_binary_log(youtube_video_id))
            else: # If no YT ID, pack a zero-length string to maintain format consistency for future readers
                log_entry_bytes_list.append(struct.pack(SA_STRING_LEN_FORMAT, 0))


        elif event_type == EVENT_TYPE_STREAM_END:
            duration_seconds = kwargs.get("duration_seconds", 0)
            peak_viewers = kwargs.get("peak_viewers", 0)
            log_entry_bytes_list.append(struct.pack(SA_INT_FORMAT, duration_seconds))
            log_entry_bytes_list.append(struct.pack(SA_INT_FORMAT, peak_viewers))

        elif event_type == EVENT_TYPE_GAME_CHANGE:
            old_game = kwargs.get("old_game", "")
            new_game = kwargs.get("new_game", "")
            log_entry_bytes_list.append(_pack_string_for_binary_log(old_game))
            log_entry_bytes_list.append(_pack_string_for_binary_log(new_game))

        elif event_type == EVENT_TYPE_TITLE_CHANGE:
            old_title = kwargs.get("old_title", "")
            new_title = kwargs.get("new_title", "")
            log_entry_bytes_list.append(_pack_string_for_binary_log(old_title))
            log_entry_bytes_list.append(_pack_string_for_binary_log(new_title))

        elif event_type == EVENT_TYPE_TAGS_CHANGE:
            old_tags = kwargs.get("old_tags", [])
            new_tags = kwargs.get("new_tags", [])
            log_entry_bytes_list.append(_pack_tag_list_for_binary_log(old_tags))
            log_entry_bytes_list.append(_pack_tag_list_for_binary_log(new_tags))
        else:
            logger.warning(f"UTA: Unknown stream activity event type for binary log: {event_type}. Skipping log.")
            return

        final_log_bytes = b"".join(log_entry_bytes_list)
        await asyncio.to_thread(_write_binary_data_sync, config_manager.UTA_STREAM_ACTIVITY_LOG_FILE, final_log_bytes)
        logger.info(f"UTA: Logged stream activity (binary): event type {event_type} at {timestamp_dt.isoformat()}")

    except Exception as e:
        logger.error(f"UTA: Failed to log stream activity (binary) to {config_manager.UTA_STREAM_ACTIVITY_LOG_FILE}: {e}", exc_info=True)


async def log_bot_session_event(event_type: int, timestamp_dt: datetime):
    if not config_manager.BOT_SESSION_LOG_FILE_PATH:
        logger.warning("Bot session log file path not configured. Skipping session event log.")
        return
    try:
        ts_unix = int(timestamp_dt.timestamp())
        packed_data = struct.pack(BOT_SESSION_RECORD_FORMAT, event_type, ts_unix)
        await asyncio.to_thread(_write_binary_data_sync, config_manager.BOT_SESSION_LOG_FILE_PATH, packed_data)

        event_name = "START" if event_type == BOT_EVENT_START else "STOP" if event_type == BOT_EVENT_STOP else "UNKNOWN"
        logger.info(f"Logged bot session event: {event_name} at {timestamp_dt.isoformat()} to {config_manager.BOT_SESSION_LOG_FILE_PATH}")
    except Exception as e:
        logger.error(f"Failed to log bot session event to {config_manager.BOT_SESSION_LOG_FILE_PATH}: {e}", exc_info=True)


def read_string_from_file_handle(file_handle) -> tuple[str | None, bool]:
    try:
        len_bytes = file_handle.read(SA_STRING_LEN_SIZE)
        if len(len_bytes) < SA_STRING_LEN_SIZE: return None, True # Incomplete read for length

        string_len = struct.unpack(SA_STRING_LEN_FORMAT, len_bytes)[0]
        if string_len == 0: return "", False # Empty string, successfully read

        string_bytes = file_handle.read(string_len)
        if len(string_bytes) < string_len: return None, True # Incomplete read for string itself

        return string_bytes.decode('utf-8', errors='replace'), False
    except struct.error as e:
        logger.error(f"Struct error reading string from file handle: {e}")
        return None, True
    except Exception as e:
        logger.error(f"Unexpected error reading string from file handle: {e}")
        return None, True


def read_tag_list_from_file_handle(file_handle) -> tuple[list[str], bool]:
    try:
        num_tags_bytes = file_handle.read(SA_LIST_HEADER_SIZE)
        if len(num_tags_bytes) < SA_LIST_HEADER_SIZE: return [], True

        num_tags = struct.unpack(SA_LIST_HEADER_FORMAT, num_tags_bytes)[0]
        if num_tags == 0: return [], False

        tags_read = []
        for _ in range(num_tags):
            tag_str, incomplete = read_string_from_file_handle(file_handle)
            if incomplete: return tags_read, True # Return partially read list and flag incompleteness
            tags_read.append(tag_str)
        return tags_read, False
    except struct.error as e:
        logger.error(f"Struct error reading tag list from file handle: {e}")
        return [], True
    except Exception as e:
        logger.error(f"Unexpected error reading tag list from file handle: {e}")
        return [], True

def consume_activity_event_body(file_handle, event_type: int) -> bool:
    """
    Consumes (reads past) the body of a stream activity event.
    Returns True if an incomplete read occurred or error, False otherwise.
    """
    try:
        if event_type == EVENT_TYPE_STREAM_START:
            _, i1 = read_string_from_file_handle(file_handle) # title
            if i1: return True
            _, i2 = read_string_from_file_handle(file_handle) # game
            if i2: return True
            _, i3 = read_tag_list_from_file_handle(file_handle) # tags
            if i3: return True

            # Consume the optional YouTube Video ID field, which is always present (even if empty string)
            # This logic assumes the field is always written, even as an empty string if no ID.
            _, i4_yt_id = read_string_from_file_handle(file_handle) # youtube_video_id
            if i4_yt_id: return True
            return False # All parts read successfully

        elif event_type == EVENT_TYPE_STREAM_END:
            bytes_to_read = SA_INT_SIZE * 2 # duration_seconds, peak_viewers
            return len(file_handle.read(bytes_to_read)) < bytes_to_read
        elif event_type == EVENT_TYPE_GAME_CHANGE:
            _, i1 = read_string_from_file_handle(file_handle) # old_game
            _, i2 = read_string_from_file_handle(file_handle) # new_game
            return i1 or i2
        elif event_type == EVENT_TYPE_TITLE_CHANGE:
            _, i1 = read_string_from_file_handle(file_handle) # old_title
            _, i2 = read_string_from_file_handle(file_handle) # new_title
            return i1 or i2
        elif event_type == EVENT_TYPE_TAGS_CHANGE:
            _, i1 = read_tag_list_from_file_handle(file_handle) # old_tags
            _, i2 = read_tag_list_from_file_handle(file_handle) # new_tags
            return i1 or i2
        else:
            logger.warning(f"DataLog Read: Attempting to consume unknown event type {event_type}. This might lead to misaligned reads.")
            return True # Assume incomplete/error for unknown types
    except Exception as e:
        logger.error(f"DataLog Read: Error consuming event body for type {event_type}: {e}", exc_info=True)
        return True # Assume incomplete/error

def read_and_find_records_for_period(filepath: str, cutoff_timestamp_unix: int, inclusive_end_ts_for_query: int | None = None):
    start_count, end_count, first_ts_unix, last_ts_unix = None, None, None, None
    all_records_in_file = []

    if not filepath or not os.path.exists(filepath) or os.path.getsize(filepath) < BINARY_RECORD_SIZE:
        return None, None, None, None, [] # Return empty list for all_records

    try:
        with open(filepath, 'rb') as f:
            while True:
                chunk = f.read(BINARY_RECORD_SIZE)
                if not chunk: break
                if len(chunk) < BINARY_RECORD_SIZE:
                    logger.warning(f"Incomplete record in {filepath}. File might be corrupted."); break
                unix_ts, count = struct.unpack(BINARY_RECORD_FORMAT, chunk)
                all_records_in_file.append((unix_ts, count))
    except FileNotFoundError:
        logger.error(f"File not found: {filepath} (read_and_find_records)")
        return None, None, None, None, []
    except Exception as e:
        logger.error(f"Error reading {filepath} (read_and_find_records): {e}", exc_info=True)
        return None, None, None, None, []

    if not all_records_in_file:
        return None, None, None, None, []

    temp_start_c, temp_first_ts = None, None
    for ts, count in reversed(all_records_in_file): # Find first record AT or BEFORE cutoff
        if ts <= cutoff_timestamp_unix:
            temp_start_c, temp_first_ts = count, ts
            break

    # If no record is at or before cutoff, but we have records, use the oldest one as start
    # IF that oldest record is within the query period (or query period has no end)
    if temp_start_c is None:
        if all_records_in_file:
            oldest_ts_candidate, oldest_count_candidate = all_records_in_file[0]
            # If there's no specific end to the query, or if the oldest record is before or at the query end
            if inclusive_end_ts_for_query is None or oldest_ts_candidate <= inclusive_end_ts_for_query :
               temp_first_ts, temp_start_c = oldest_ts_candidate, oldest_count_candidate

    first_ts_unix, start_count = temp_first_ts, temp_start_c

    # Determine end_count and last_ts_unix
    if inclusive_end_ts_for_query is None: # If no specific end, use the latest record in file
        if all_records_in_file: # Should always be true if we got this far
            last_ts_unix, end_count = all_records_in_file[-1]
    else: # Specific end for the query
        temp_end_c, temp_last_ts = None, None
        for ts, count in reversed(all_records_in_file):
            if ts <= inclusive_end_ts_for_query: # Find latest record AT or BEFORE the query end
                temp_end_c, temp_last_ts = count, ts
                break
        # If no record is at or before query_end, but we found a start_count and it's within query_end,
        # it means the start_count is also the "end" for this query.
        if temp_end_c is None and start_count is not None and first_ts_unix is not None and first_ts_unix <= inclusive_end_ts_for_query:
            temp_last_ts, temp_end_c = first_ts_unix, start_count

        last_ts_unix, end_count = temp_last_ts, temp_end_c

        # Ensure last_ts isn't before first_ts if both are found. If so, period is just one point.
        if last_ts_unix is not None and first_ts_unix is not None and last_ts_unix < first_ts_unix:
            last_ts_unix, end_count = first_ts_unix, start_count # Effectively means no change within the valid overlap

    return start_count, end_count, first_ts_unix, last_ts_unix, all_records_in_file


def get_counts_for_day_boundaries(filepath: str, target_date_obj: date):
    day_start_dt_utc = datetime.combine(target_date_obj, datetime.min.time(), tzinfo=timezone.utc)
    day_end_dt_utc = datetime.combine(target_date_obj, datetime.max.time(), tzinfo=timezone.utc) # End of day

    day_start_unix = int(day_start_dt_utc.timestamp())
    day_end_unix = int(day_end_dt_utc.timestamp())

    all_records = []
    if not os.path.exists(filepath) or os.path.getsize(filepath) < BINARY_RECORD_SIZE:
        return f"Data file '{os.path.basename(filepath)}' not found or is too small."

    try:
        with open(filepath, 'rb') as f:
            while True:
                chunk = f.read(BINARY_RECORD_SIZE)
                if not chunk: break
                if len(chunk) < BINARY_RECORD_SIZE:
                    logger.warning(f"Incomplete record in {filepath} for daystats. File might be corrupted."); break
                unix_ts, count = struct.unpack(BINARY_RECORD_FORMAT, chunk)
                all_records.append({'ts': unix_ts, 'count': count})
    except FileNotFoundError: # Should be caught by os.path.exists already
        return f"File '{os.path.basename(filepath)}' not found."
    except Exception as e:
        logger.error(f"Error reading {filepath} for daystats: {e}", exc_info=True)
        return f"Error reading data file '{os.path.basename(filepath)}'."

    if not all_records:
        return f"Data file '{os.path.basename(filepath)}' is empty."

    all_records.sort(key=lambda x: x['ts']) # Ensure sorted by timestamp

    # Check if any data falls within the target day or before/after
    if all_records[-1]['ts'] < day_start_unix: # All data ends before target day
        last_data_dt = datetime.fromtimestamp(all_records[-1]['ts'], tz=timezone.utc)
        return f"All data in '{os.path.basename(filepath)}' ends before {target_date_obj.isoformat()} (last data: {discord.utils.format_dt(last_data_dt, 'f')})."
    if all_records[0]['ts'] > day_end_unix: # All data begins after target day
        first_data_dt = datetime.fromtimestamp(all_records[0]['ts'], tz=timezone.utc)
        return f"All data in '{os.path.basename(filepath)}' begins after {target_date_obj.isoformat()} (first data: {discord.utils.format_dt(first_data_dt, 'f')})."

    # Find effective start record: last record on or before start of target_date_obj
    effective_start_record = None
    for rec in reversed(all_records):
        if rec['ts'] <= day_start_unix:
            effective_start_record = rec
            break
    if effective_start_record is None: # If no record before start_of_day, use the very first record if it's on the target day
        effective_start_record = all_records[0] if all_records[0]['ts'] <= day_end_unix else None

    # Find effective end record: last record on or before end of target_date_obj
    effective_end_record = None
    records_on_target_day_for_plot = []
    num_records_found_on_day = 0

    for rec in all_records:
        if day_start_unix <= rec['ts'] <= day_end_unix: # Record is within target day
            records_on_target_day_for_plot.append(rec)
            num_records_found_on_day +=1
            if effective_end_record is None or rec['ts'] >= effective_end_record['ts']:
                effective_end_record = rec

    # If no records on target day, effective_end_record might be from a previous day if effective_start_record was.
    # Or, if effective_start_record was from *after* the day, effective_end_record would be None.
    if effective_end_record is None: # If no record found ON the day, use the start_record as end_record
                                     # (implies no change or data only exists before the day)
        effective_end_record = effective_start_record

    if not effective_start_record: # Should not happen if initial checks passed
        return f"Could not determine a starting count for {target_date_obj.isoformat()} from '{os.path.basename(filepath)}'."
    if not effective_end_record:
        return f"Could not determine an ending count for {target_date_obj.isoformat()} from '{os.path.basename(filepath)}'."

    return {
        'start_ts': effective_start_record['ts'],
        'start_count': effective_start_record['count'],
        'end_ts': effective_end_record['ts'],
        'end_count': effective_end_record['count'],
        'num_records_on_day': num_records_found_on_day,
        'day_start_utc_dt': day_start_dt_utc, # For plotting reference
        'day_end_utc_dt': day_end_dt_utc,     # For plotting reference
        'records_for_plot': records_on_target_day_for_plot # Records strictly ON the day
    }


def read_stream_durations_for_period(filepath: str, query_start_unix: int, query_end_unix: int) -> tuple[int, int]:
    if not filepath or not os.path.exists(filepath) or os.path.getsize(filepath) < STREAM_DURATION_RECORD_SIZE:
        return 0, 0

    total_duration_seconds = 0
    num_streams_in_period = 0
    try:
        with open(filepath, 'rb') as f:
            while True:
                chunk = f.read(STREAM_DURATION_RECORD_SIZE)
                if not chunk: break
                if len(chunk) < STREAM_DURATION_RECORD_SIZE:
                    logger.warning(f"Incomplete record in {filepath} while reading stream durations. File might be corrupted."); break

                stream_start_ts, stream_end_ts = struct.unpack(STREAM_DURATION_RECORD_FORMAT, chunk)

                # Calculate overlap with the query period
                overlap_start = max(stream_start_ts, query_start_unix)
                overlap_end = min(stream_end_ts, query_end_unix)

                if overlap_start < overlap_end: # If there is an overlap
                    total_duration_seconds += (overlap_end - overlap_start)
                    num_streams_in_period +=1
    except FileNotFoundError: # Should be caught by os.path.exists
        return 0,0
    except Exception as e:
        logger.error(f"Error reading {filepath} for stream durations: {e}", exc_info=True)
        return 0,0

    return total_duration_seconds, num_streams_in_period


def get_viewer_stats_for_period(viewer_log_file: str, start_ts_unix: int, end_ts_unix: int) -> tuple[float | None, int, int]:
    if not viewer_log_file or not os.path.exists(viewer_log_file) or \
       not config_manager.UTA_VIEWER_COUNT_LOGGING_ENABLED or \
       os.path.getsize(viewer_log_file) < BINARY_RECORD_SIZE:
        return None, 0, 0

    viewer_counts_in_period = []
    peak_viewers = 0
    try:
        with open(viewer_log_file, 'rb') as f:
            while True:
                chunk = f.read(BINARY_RECORD_SIZE)
                if not chunk: break
                if len(chunk) < BINARY_RECORD_SIZE: break # Incomplete record

                ts, count = struct.unpack(BINARY_RECORD_FORMAT, chunk)
                if start_ts_unix <= ts < end_ts_unix: # Records within the period
                    viewer_counts_in_period.append(count)
                    if count > peak_viewers:
                        peak_viewers = count
    except Exception as e:
        logger.error(f"Error reading viewer log '{viewer_log_file}' for stats: {e}", exc_info=True)
        return None, 0, 0

    if not viewer_counts_in_period:
        return None, 0, 0

    avg_viewers = sum(viewer_counts_in_period) / len(viewer_counts_in_period)
    return avg_viewers, peak_viewers, len(viewer_counts_in_period)


def parse_stream_activity_for_game_segments(filepath: str, query_start_unix: int = None, query_end_unix: int = None) -> list[dict]:
    if not filepath or not os.path.exists(filepath) or os.path.getsize(filepath) < SA_BASE_HEADER_SIZE:
        return []

    all_events_parsed = []
    try:
        with open(filepath, 'rb') as f:
            file_total_size = os.fstat(f.fileno()).st_size
            while True:
                current_event_start_pos = f.tell()
                if current_event_start_pos + SA_BASE_HEADER_SIZE > file_total_size: # Not enough for header
                    break

                header_chunk = f.read(SA_BASE_HEADER_SIZE)
                if not header_chunk: break # EOF

                event_type, unix_ts = struct.unpack(SA_BASE_HEADER_FORMAT, header_chunk)
                event_data_dict = {'type': event_type, 'timestamp': unix_ts}

                body_is_incomplete = False
                try:
                    if event_type == EVENT_TYPE_STREAM_START:
                        title_str, inc1 = read_string_from_file_handle(f)
                        game_str, inc2 = read_string_from_file_handle(f)
                        tags_list, inc3 = read_tag_list_from_file_handle(f)

                        # Read the youtube_video_id string (which is always present, even if empty)
                        youtube_video_id_str, inc4_yt_id = read_string_from_file_handle(f)

                        if inc1 or inc2 or inc3 or inc4_yt_id:
                            body_is_incomplete = True
                            logger.warning(f"GameSegmentParser: Incomplete STREAM_START event data at timestamp {unix_ts}.")
                        else:
                            event_data_dict.update({'title': title_str, 'game': game_str, 'tags': tags_list, 'youtube_video_id': youtube_video_id_str})

                    elif event_type == EVENT_TYPE_GAME_CHANGE:
                        old_game_str, inc1 = read_string_from_file_handle(f)
                        new_game_str, inc2 = read_string_from_file_handle(f)
                        if inc1 or inc2:
                            body_is_incomplete = True; logger.warning(f"GameSegmentParser: Incomplete GAME_CHANGE event data at {unix_ts}.")
                        else: event_data_dict.update({'old_game': old_game_str, 'new_game': new_game_str})

                    elif event_type == EVENT_TYPE_TITLE_CHANGE:
                        old_title_str, inc1 = read_string_from_file_handle(f)
                        new_title_str, inc2 = read_string_from_file_handle(f)
                        if inc1 or inc2:
                            body_is_incomplete = True; logger.warning(f"GameSegmentParser: Incomplete TITLE_CHANGE event data at {unix_ts}.")
                        else: event_data_dict.update({'old_title': old_title_str, 'new_title': new_title_str})

                    else: # For STREAM_END, TAGS_CHANGE, or unknown
                        body_is_incomplete = consume_activity_event_body(f, event_type)
                        if body_is_incomplete and event_type in [EVENT_TYPE_STREAM_END, EVENT_TYPE_TAGS_CHANGE]:
                            logger.warning(f"GameSegmentParser: Incomplete event body for known type {event_type} at {unix_ts}.")
                        elif body_is_incomplete: # Unknown type, already logged in consume_activity_event_body
                            pass

                    if body_is_incomplete:
                        f.seek(current_event_start_pos) # Rewind to start of this bad event
                        logger.warning(f"GameSegmentParser: Skipping rest of file due to incomplete event (type {event_type}) at offset {current_event_start_pos} (timestamp {unix_ts}).")
                        break # Stop processing this file

                    # Only add events relevant for game segment parsing
                    if event_type in [EVENT_TYPE_STREAM_START, EVENT_TYPE_GAME_CHANGE, EVENT_TYPE_TITLE_CHANGE, EVENT_TYPE_STREAM_END]:
                        all_events_parsed.append(event_data_dict)

                except struct.error as e_struct:
                    logger.error(f"GameSegmentParser: Struct error processing event body (type {event_type}) at ts {unix_ts} in {filepath}: {e_struct}"); break
                except Exception as e_body:
                    logger.error(f"GameSegmentParser: Generic error processing event body (type {event_type}) at ts {unix_ts} in {filepath}: {e_body}"); break

    except FileNotFoundError:
        logger.error(f"GameSegmentParser: File not found: {filepath}"); return []
    except Exception as e_open:
        logger.error(f"GameSegmentParser: Error opening or reading {filepath}: {e_open}"); return []

    all_events_parsed.sort(key=lambda x: x['timestamp'])

    game_segments_list = []
    active_stream_details = None # Holds {'game': str, 'start_ts': int, 'title': str}

    for event in all_events_parsed:
        event_ts = event['timestamp']

        # Handle events before the query_start_unix to establish initial state
        if query_start_unix and event_ts < query_start_unix:
            if event['type'] == EVENT_TYPE_STREAM_START:
                 active_stream_details = {'game': event.get('game',"N/A"), 'start_ts': event_ts, 'title': event.get('title',"N/A")}
            elif event['type'] == EVENT_TYPE_GAME_CHANGE and active_stream_details:
                 active_stream_details['game'] = event.get('new_game',"N/A")
                 active_stream_details['start_ts'] = event_ts # Game change implies new segment start time for that game
            elif event['type'] == EVENT_TYPE_TITLE_CHANGE and active_stream_details:
                 active_stream_details['title'] = event.get('new_title',"N/A") # Title change doesn't start a new game segment
            elif event['type'] == EVENT_TYPE_STREAM_END:
                 active_stream_details = None
            continue # Move to next event if this one is before query start

        # Handle events after query_end_unix to finalize any open segment
        if query_end_unix and event_ts > query_end_unix:
            if active_stream_details: # If a stream was active when we crossed query_end_unix
                 segment_start_ts_eff = max(active_stream_details['start_ts'], query_start_unix) if query_start_unix else active_stream_details['start_ts']
                 if query_end_unix > segment_start_ts_eff: # Ensure positive duration for the segment part within query
                    game_segments_list.append({
                        'game': active_stream_details['game'],
                        'start_ts': segment_start_ts_eff,
                        'end_ts': query_end_unix, # Cap segment at query_end_unix
                        'title_at_start': active_stream_details['title']
                    })
            active_stream_details = None # Stream segment processing ends here
            break # Stop processing further events

        # Process events within the query period (or all if no period defined)
        if event['type'] == EVENT_TYPE_STREAM_START:
            if active_stream_details: # If a stream was already 'active' (e.g. no STREAM_END from prev)
                prev_segment_start_eff = max(active_stream_details['start_ts'], query_start_unix) if query_start_unix else active_stream_details['start_ts']
                if event_ts > prev_segment_start_eff: # End previous segment at this new START
                    game_segments_list.append({
                        'game': active_stream_details['game'],
                        'start_ts': prev_segment_start_eff,
                        'end_ts': event_ts,
                        'title_at_start': active_stream_details['title']
                    })
            active_stream_details = {'game': event.get('game',"N/A"), 'start_ts': event_ts, 'title': event.get('title',"N/A")}

        elif event['type'] == EVENT_TYPE_GAME_CHANGE:
            if active_stream_details: # If there's an active stream session
                segment_start_ts_eff = max(active_stream_details['start_ts'], query_start_unix) if query_start_unix else active_stream_details['start_ts']
                if event_ts > segment_start_ts_eff: # If game change happens after segment start (positive duration)
                    game_segments_list.append({
                        'game': active_stream_details['game'],
                        'start_ts': segment_start_ts_eff,
                        'end_ts': event_ts, # Current segment ends at game change
                        'title_at_start': active_stream_details['title']
                    })
                # Start new segment details for the new game
                active_stream_details['game'] = event.get('new_game',"N/A")
                active_stream_details['start_ts'] = event_ts # New game segment starts now
                # Title persists until a TITLE_CHANGE event
            else: # Game change event without a preceding STREAM_START (e.g., bot started mid-stream after game change)
                logger.debug(f"GameSegmentParser: GAME_CHANGE event at {event_ts} without an active stream context. Starting new segment for '{event.get('new_game','N/A')}' from this point.")
                active_stream_details = {'game': event.get('new_game',"N/A"), 'start_ts': event_ts, 'title': "N/A (Title from before this game change)"}

        elif event['type'] == EVENT_TYPE_TITLE_CHANGE:
            if active_stream_details: # Update title for the current segment
                active_stream_details['title'] = event.get('new_title',"N/A")

        elif event['type'] == EVENT_TYPE_STREAM_END:
            if active_stream_details:
                segment_start_ts_eff = max(active_stream_details['start_ts'], query_start_unix) if query_start_unix else active_stream_details['start_ts']
                segment_end_ts_eff = min(event_ts, query_end_unix) if query_end_unix else event_ts

                if segment_end_ts_eff > segment_start_ts_eff: # Ensure positive duration
                    game_segments_list.append({
                        'game': active_stream_details['game'],
                        'start_ts': segment_start_ts_eff,
                        'end_ts': segment_end_ts_eff,
                        'title_at_start': active_stream_details['title']
                    })
            active_stream_details = None # Stream session ends

    # After loop, if a stream was still active and no query_end_unix was hit (or it's beyond last event)
    if active_stream_details:
        segment_start_ts_eff = max(active_stream_details['start_ts'], query_start_unix) if query_start_unix else active_stream_details['start_ts']
        # If no query_end_unix, the segment effectively ends at the last known event's timestamp
        # or if no events, it means it's an open segment from query_start to now (but this function deals with historical data)
        # For historical parsing, if query_end_unix is None, it means "up to the last event processed".
        # If all_events_parsed is empty, this block won't be hit unless query_start_unix itself caused active_stream_details
        effective_end_timestamp_for_segment = query_end_unix if query_end_unix else (all_events_parsed[-1]['timestamp'] if all_events_parsed else segment_start_ts_eff)

        if effective_end_timestamp_for_segment > segment_start_ts_eff:
             game_segments_list.append({
                'game': active_stream_details['game'],
                'start_ts': segment_start_ts_eff,
                'end_ts': effective_end_timestamp_for_segment,
                'title_at_start': active_stream_details['title']
            })

    # Final filter for valid segments (e.g., game name exists, positive duration)
    valid_segments_final = [s for s in game_segments_list if s.get('game') and s['end_ts'] > s['start_ts']]
    return valid_segments_final


def calculate_bot_runtime_in_period(filepath: str, query_start_unix: int, query_end_unix: int) -> tuple[int, int]:
    if not filepath or not os.path.exists(filepath) or os.path.getsize(filepath) < BOT_SESSION_RECORD_SIZE:
        return 0, 0

    session_records = []
    try:
        with open(filepath, 'rb') as f:
            while True:
                chunk = f.read(BOT_SESSION_RECORD_SIZE)
                if not chunk: break
                if len(chunk) < BOT_SESSION_RECORD_SIZE:
                    logger.warning(f"Incomplete record in bot session log '{filepath}'. Skipping rest of file."); break
                event_type, ts = struct.unpack(BOT_SESSION_RECORD_FORMAT, chunk)
                session_records.append({'type': event_type, 'ts': ts})
    except Exception as e:
        logger.error(f"Error reading bot session log '{filepath}': {e}", exc_info=True)
        return 0, 0

    if not session_records:
        return 0, 0

    session_records.sort(key=lambda r: r['ts']) # Crucial for correct pairing

    total_uptime_seconds_in_query_period = 0
    num_sessions_contributing_to_uptime = 0
    active_session_start_timestamp = None

    for record in session_records:
        event_ts = record['ts']
        event_type = record['type']

        if event_type == BOT_EVENT_START:
            if active_session_start_timestamp is not None:
                # This implies a missing STOP event or bot crash. Assume previous session ended at new START.
                logger.warning(f"Bot session log: Encountered a START event at {event_ts} while a session was already active from {active_session_start_timestamp}. Previous session considered ended at new START.")
                session_end_ts_for_calc = event_ts # End previous session here

                overlap_start = max(active_session_start_timestamp, query_start_unix)
                overlap_end = min(session_end_ts_for_calc, query_end_unix)
                if overlap_end > overlap_start: # If the "ended" session overlaps with query period
                    total_uptime_seconds_in_query_period += (overlap_end - overlap_start)
                    num_sessions_contributing_to_uptime +=1 # Count this "implicitly ended" session

            active_session_start_timestamp = event_ts # Start new session

        elif event_type == BOT_EVENT_STOP:
            if active_session_start_timestamp is not None: # If a session was active
                session_end_ts_for_calc = event_ts

                overlap_start = max(active_session_start_timestamp, query_start_unix)
                overlap_end = min(session_end_ts_for_calc, query_end_unix)
                if overlap_end > overlap_start: # If this ended session overlaps with query period
                    total_uptime_seconds_in_query_period += (overlap_end - overlap_start)
                    num_sessions_contributing_to_uptime +=1

                active_session_start_timestamp = None # Session is now stopped
            else:
                # This implies a STOP event without a preceding START (e.g. bot stopped while it thought it wasn't running).
                logger.warning(f"Bot session log: Encountered a STOP event at {event_ts} without an active session start. Ignoring this STOP for uptime calculation.")

    # After iterating all records, if a session is still considered active (last event was START)
    if active_session_start_timestamp is not None:
        # Assume this session runs until the end of the query period (or "now" if query_end_unix is now)
        session_end_ts_for_calc = query_end_unix # Cap its duration at the query end

        overlap_start = max(active_session_start_timestamp, query_start_unix)
        overlap_end = min(session_end_ts_for_calc, query_end_unix) # Effectively just query_end_unix if session started before/during
        if overlap_end > overlap_start: # If the ongoing session overlaps with query period
            total_uptime_seconds_in_query_period += (overlap_end - overlap_start)
            if num_sessions_contributing_to_uptime == 0 or (num_sessions_contributing_to_uptime > 0 and (query_end_unix - active_session_start_timestamp > 0)):
                 # Avoid double counting if last START was the only contributing factor
                 # Count it if no other sessions contributed OR if this session meaningfully contributed
                 if not any(sr['type'] == BOT_EVENT_STOP and sr['ts'] > active_session_start_timestamp for sr in session_records): # if no stop after this start
                    num_sessions_contributing_to_uptime +=1

    return total_uptime_seconds_in_query_period, num_sessions_contributing_to_uptime


def read_chat_activity_for_period(filepath: str, query_start_unix: int, query_end_unix: int) -> list[dict]:
    """Reads chat activity records for a given period."""
    if not filepath or not os.path.exists(filepath) or os.path.getsize(filepath) < CHAT_ACTIVITY_RECORD_SIZE:
        return []

    records = []
    try:
        with open(filepath, 'rb') as f:
            while True:
                chunk = f.read(CHAT_ACTIVITY_RECORD_SIZE)
                if not chunk: break
                if len(chunk) < CHAT_ACTIVITY_RECORD_SIZE:
                    logger.warning(f"Incomplete record in chat activity file {filepath}. File might be corrupted.")
                    break

                ts_unix, msg_count, unique_count = struct.unpack(CHAT_ACTIVITY_RECORD_FORMAT, chunk)

                if query_start_unix <= ts_unix <= query_end_unix:
                    records.append({
                        "timestamp": ts_unix,
                        "message_count": msg_count,
                        "unique_chatters_count": unique_count
                    })
    except FileNotFoundError: # Should be caught by os.path.exists
        logger.error(f"Chat activity file not found: {filepath}")
        return []
    except Exception as e:
        logger.error(f"Error reading chat activity file {filepath}: {e}", exc_info=True)
        return []
    return records

# --- New Helper Functions for Milestones Cog ---

def get_latest_binary_log_value(filepath: str) -> int | None:
    """Reads the last record from a (timestamp, value) binary log and returns the value."""
    if not filepath or not os.path.exists(filepath) or os.path.getsize(filepath) < BINARY_RECORD_SIZE:
        return None
    try:
        with open(filepath, 'rb') as f:
            f.seek(-BINARY_RECORD_SIZE, os.SEEK_END) # Go to the start of the last record
            chunk = f.read(BINARY_RECORD_SIZE)
            if len(chunk) == BINARY_RECORD_SIZE:
                _, value = struct.unpack(BINARY_RECORD_FORMAT, chunk)
                return value
    except Exception as e:
        logger.error(f"Error reading latest value from {filepath}: {e}", exc_info=True)
    return None

def get_total_stream_time_seconds_from_activity(filepath: str) -> int:
    """Parses stream activity log and sums up all stream durations within the entire file."""
    # We use query_start_unix=0, query_end_unix=current_time to parse all segments.
    now_unix = int(datetime.now(timezone.utc).timestamp())
    game_segments = parse_stream_activity_for_game_segments(filepath, 0, now_unix + 86400*365*10) # 10 years in future for "all"
    return sum(seg['end_ts'] - seg['start_ts'] for seg in game_segments)

def get_max_value_from_binary_log(filepath: str) -> int | None:
    """Scans a (timestamp, value) binary log and returns the maximum value found."""
    if not filepath or not os.path.exists(filepath) or os.path.getsize(filepath) < BINARY_RECORD_SIZE:
        return None
    max_val = None
    try:
        with open(filepath, 'rb') as f:
            while True:
                chunk = f.read(BINARY_RECORD_SIZE)
                if not chunk or len(chunk) < BINARY_RECORD_SIZE: break
                _, value = struct.unpack(BINARY_RECORD_FORMAT, chunk)
                if max_val is None or value > max_val:
                    max_val = value
    except Exception as e:
        logger.error(f"Error reading max value from {filepath}: {e}", exc_info=True)
        return None # Return None if error, even if partial max_val was found
    return max_val

def get_avg_value_from_binary_log(filepath: str) -> float | None:
    """Scans a (timestamp, value) binary log and returns the average value."""
    if not filepath or not os.path.exists(filepath) or os.path.getsize(filepath) < BINARY_RECORD_SIZE:
        return None
    total_sum = 0
    count_records = 0
    try:
        with open(filepath, 'rb') as f:
            while True:
                chunk = f.read(BINARY_RECORD_SIZE)
                if not chunk or len(chunk) < BINARY_RECORD_SIZE: break
                _, value = struct.unpack(BINARY_RECORD_FORMAT, chunk)
                total_sum += value
                count_records += 1
    except Exception as e:
        logger.error(f"Error reading avg value from {filepath}: {e}", exc_info=True)
        return None
    return total_sum / count_records if count_records > 0 else None

def get_total_chat_messages_from_log(filepath: str) -> int:
    """Reads chat activity log and sums message_count."""
    if not filepath or not os.path.exists(filepath) or os.path.getsize(filepath) < CHAT_ACTIVITY_RECORD_SIZE:
        return 0
    total_messages = 0
    try:
        with open(filepath, 'rb') as f:
            while True:
                chunk = f.read(CHAT_ACTIVITY_RECORD_SIZE)
                if not chunk or len(chunk) < CHAT_ACTIVITY_RECORD_SIZE: break
                _, msg_count, _ = struct.unpack(CHAT_ACTIVITY_RECORD_FORMAT, chunk)
                total_messages += msg_count
    except Exception as e:
        logger.error(f"Error reading total chat messages from {filepath}: {e}", exc_info=True)
    return total_messages

def get_peak_unique_chatters_from_log(filepath: str) -> int | None:
    """Reads chat activity log and returns the peak unique_chatters_count in any interval."""
    if not filepath or not os.path.exists(filepath) or os.path.getsize(filepath) < CHAT_ACTIVITY_RECORD_SIZE:
        return None
    peak_chatters = None
    try:
        with open(filepath, 'rb') as f:
            while True:
                chunk = f.read(CHAT_ACTIVITY_RECORD_SIZE)
                if not chunk or len(chunk) < CHAT_ACTIVITY_RECORD_SIZE: break
                _, _, unique_c_count = struct.unpack(CHAT_ACTIVITY_RECORD_FORMAT, chunk)
                if peak_chatters is None or unique_c_count > peak_chatters:
                    peak_chatters = unique_c_count
    except Exception as e:
        logger.error(f"Error reading peak unique chatters from {filepath}: {e}", exc_info=True)
        return None
    return peak_chatters

def count_records_in_file(filepath: str, record_size: int) -> int:
    """Counts records in a binary file given the record size."""
    if not filepath or not os.path.exists(filepath) or record_size <= 0:
        return 0
    try:
        file_size = os.path.getsize(filepath)
        return file_size // record_size
    except Exception as e:
        logger.error(f"Error counting records in {filepath}: {e}", exc_info=True)
    return 0

def count_distinct_games_from_activity(filepath: str) -> int:
    """Parses stream activity log for unique game names across all time."""
    now_unix = int(datetime.now(timezone.utc).timestamp())
    game_segments = parse_stream_activity_for_game_segments(filepath, 0, now_unix + 86400*365*10)
    if not game_segments:
        return 0
    distinct_games = set(seg['game'] for seg in game_segments if seg.get('game'))
    return len(distinct_games)