import logging
from datetime import datetime, timezone
from uta_bot import config_manager # For chapter config options
from .formatters import format_seconds_to_hhmmss # Updated import

logger = logging.getLogger(__name__)


def generate_chapter_text(game_segments: list, vod_part_start_timestamp_unix: int) -> str | None:
    """
    Generates a YouTube chapter string from game segments relative to the VOD part's start.
    Returns None if no valid chapters can be generated.
    """
    if not game_segments:
        logger.debug("Chapter Gen: No game segments provided.")
        return None

    chapters = []
    # Segments should already be sorted by start_ts from parse_stream_activity_for_game_segments
    # but let's ensure it for safety.
    sorted_segments = sorted(game_segments, key=lambda x: x['start_ts'])

    # First chapter must be 0:00. Use the first segment's info for its title.
    first_segment = sorted_segments[0]

    # Use a general title for the 0:00 mark if the first segment starts later,
    # or use the first segment's details if it starts at/near 0:00 of the VOD part.
    first_segment_relative_start_s = first_segment['start_ts'] - vod_part_start_timestamp_unix

    title_for_0_00 = "Stream Start" # Default
    if first_segment_relative_start_s < config_manager.UTA_YOUTUBE_MIN_CHAPTER_DURATION_SECONDS : # If first game starts very early in VOD
        try:
            title_for_0_00 = config_manager.UTA_YOUTUBE_CHAPTER_TITLE_TEMPLATE.format(
                game_name=first_segment.get('game', "Stream Start"),
                twitch_title=first_segment.get('title_at_start', "Live Stream"),
                part_num=config_manager.uta_current_restream_part_number,
                date=datetime.fromtimestamp(vod_part_start_timestamp_unix, tz=timezone.utc).strftime("%Y-%m-%d"),
                time=datetime.fromtimestamp(vod_part_start_timestamp_unix, tz=timezone.utc).strftime("%H:%M:%S")
            ).strip(" -").strip()
            if not title_for_0_00 : title_for_0_00 = first_segment.get('game', "Stream Start")
        except KeyError as e:
            logger.warning(f"Chapter Gen: Missing placeholder in UTA_YOUTUBE_CHAPTER_TITLE_TEMPLATE: {e}. Using default title.")
            title_for_0_00 = first_segment.get('game', "Stream Start")


    chapters.append(f"0:00 {title_for_0_00}")
    logger.debug(f"Chapter Gen: Added initial chapter: 0:00 {title_for_0_00}")
    last_added_timestamp_str = "0:00"

    for segment in sorted_segments:
        segment_start_relative_to_vod_s = segment['start_ts'] - vod_part_start_timestamp_unix
        segment_duration_s = segment['end_ts'] - segment['start_ts']

        # Skip if segment starts before VOD part (already handled by query to parse_stream_activity)
        # or if its relative start is effectively 0 and it's the first segment (already added as 0:00)
        if segment_start_relative_to_vod_s < 1 and segment == first_segment:
            continue

        # Skip very short segments
        if segment_duration_s < config_manager.UTA_YOUTUBE_MIN_CHAPTER_DURATION_SECONDS:
            logger.debug(f"Chapter Gen: Skipping short chapter for '{segment.get('game', 'Unknown Game')}' (duration: {segment_duration_s}s)")
            continue

        current_timestamp_str = format_seconds_to_hhmmss(segment_start_relative_to_vod_s)

        # Avoid duplicate timestamps for chapters
        if current_timestamp_str == last_added_timestamp_str:
            logger.debug(f"Chapter Gen: Skipping chapter for '{segment.get('game', 'Unknown Game')}' due to same timestamp as previous: {current_timestamp_str}")
            continue

        try:
            chapter_title = config_manager.UTA_YOUTUBE_CHAPTER_TITLE_TEMPLATE.format(
                game_name=segment.get('game', "Gameplay"),
                twitch_title=segment.get('title_at_start', "Live"),
                part_num=config_manager.uta_current_restream_part_number,
                date=datetime.fromtimestamp(segment['start_ts'], tz=timezone.utc).strftime("%Y-%m-%d"),
                time=datetime.fromtimestamp(segment['start_ts'], tz=timezone.utc).strftime("%H:%M:%S")
            ).strip(" -").strip()
            if not chapter_title: chapter_title = segment.get('game', "Gameplay")
        except KeyError as e:
            logger.warning(f"Chapter Gen: Missing placeholder in UTA_YOUTUBE_CHAPTER_TITLE_TEMPLATE for segment: {e}. Using default title.")
            chapter_title = segment.get('game', "Gameplay")

        chapters.append(f"{current_timestamp_str} {chapter_title}")
        last_added_timestamp_str = current_timestamp_str
        logger.debug(f"Chapter Gen: Added chapter: {current_timestamp_str} {chapter_title}")

    if len(chapters) <= 1: # Only "0:00 Stream Start" is not useful if no other chapters
        logger.info("Chapter Gen: Not enough distinct segments (after filtering) to generate meaningful chapters.")
        return None

    return "\n".join(chapters)