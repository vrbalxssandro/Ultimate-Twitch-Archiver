from datetime import timedelta
import re

def parse_duration_to_timedelta(duration_str: str):
    """
    Parses a human-readable duration string (e.g., "10m", "2h", "3d")
    into a timedelta object and a display string.
    Returns (timedelta | None, error_message_or_period_name_str).
    """
    if not duration_str:
        return None, "No duration provided."
    
    duration_str = duration_str.lower().strip()
    
    match = re.fullmatch(r"(\d+)\s*(m|min|mins|minute|minutes|h|hr|hrs|hour|hours|d|day|days|w|wk|wks|week|weeks|mo|mon|mth|month|months|y|yr|yrs|year|years)", duration_str)
    
    if not match:
        return None, "Invalid duration format. Use N<unit>. Examples: `10m`, `2h`, `3d`, `1w`, `1mo`, `1y`."
        
    value = int(match.group(1))
    unit_group = match.group(2)
    
    if value <= 0:
        return None, "Duration value must be greater than 0."

    delta = None
    period_name_for_display = ""

    if unit_group in ["m", "min", "mins", "minute", "minutes"]:
        delta = timedelta(minutes=value)
        period_name_for_display = f"last {value} minute{'s' if value > 1 else ''}"
    elif unit_group in ["h", "hr", "hrs", "hour", "hours"]:
        delta = timedelta(hours=value)
        period_name_for_display = f"last {value} hour{'s' if value > 1 else ''}"
    elif unit_group in ["d", "day", "days"]:
        delta = timedelta(days=value)
        period_name_for_display = f"last {value} day{'s' if value > 1 else ''}"
    elif unit_group in ["w", "wk", "wks", "week", "weeks"]:
        delta = timedelta(weeks=value)
        period_name_for_display = f"last {value} week{'s' if value > 1 else ''}"
    elif unit_group in ["mo", "mon", "mth", "month", "months"]:
        delta = timedelta(days=value * 30) 
        period_name_for_display = f"last {value} month{'s' if value > 1 else ''} (approx. {value*30} days)"
    elif unit_group in ["y", "yr", "yrs", "year", "years"]:
        delta = timedelta(days=value * 365)
        period_name_for_display = f"last {value} year{'s' if value > 1 else ''} (approx. {value*365} days)"
    
    return (delta, period_name_for_display) if delta else (None, "Internal error: Unrecognized unit after regex match.")


def format_duration_human(total_seconds: int) -> str:
    """
    Formats a duration in total seconds into a human-readable string
    (e.g., "1 day, 2 hours, 30 minutes, 15 seconds").
    """
    if total_seconds < 0:
        total_seconds = 0 
    if total_seconds == 0:
        return "no time"

    days, remainder = divmod(total_seconds, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, seconds = divmod(remainder, 60)

    parts = []
    if days > 0:
        parts.append(f"{days} day{'s' if days > 1 else ''}")
    if hours > 0:
        parts.append(f"{hours} hour{'s' if hours > 1 else ''}")
    if minutes > 0:
        parts.append(f"{minutes} minute{'s' if minutes > 1 else ''}")
    
    if seconds > 0 or not parts : 
        parts.append(f"{seconds} second{'s' if seconds > 1 else ''}")
    
    if not parts: 
        return "less than a second" 

    return ", ".join(parts)


def format_seconds_to_hhmmss(seconds: int) -> str:
    """Formats seconds into HH:MM:SS or MM:SS or M:SS string."""
    seconds = max(0, int(seconds))
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    if h > 0:
        return f"{h:d}:{m:02d}:{s:02d}" # No leading zero for hour if < 10
    elif m > 0 : # No leading zero for minute if < 10 and no hours
        return f"{m:d}:{s:02d}"
    else: # Only seconds, or 0:00
         return f"0:{s:02d}"