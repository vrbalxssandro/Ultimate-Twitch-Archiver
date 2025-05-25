import struct

# --- Binary Data Formats ---
BINARY_RECORD_FORMAT = '>II' # (timestamp, count) for followers/viewers
BINARY_RECORD_SIZE = struct.calcsize(BINARY_RECORD_FORMAT)

STREAM_DURATION_RECORD_FORMAT = '>II' # (start_timestamp, end_timestamp)
STREAM_DURATION_RECORD_SIZE = struct.calcsize(STREAM_DURATION_RECORD_FORMAT)

# --- Chat Activity Log Binary Formats ---
# Timestamp (Unix Int), Message Count (Unsigned Short), Unique Chatters Count (Unsigned Short)
CHAT_ACTIVITY_RECORD_FORMAT = '>IHH'
CHAT_ACTIVITY_RECORD_SIZE = struct.calcsize(CHAT_ACTIVITY_RECORD_FORMAT)

# --- Stream Activity Log Event Types ---
EVENT_TYPE_STREAM_START = 1
EVENT_TYPE_STREAM_END = 2
EVENT_TYPE_GAME_CHANGE = 3
EVENT_TYPE_TITLE_CHANGE = 4
EVENT_TYPE_TAGS_CHANGE = 5

# --- Stream Activity Log Binary Formats ---
# Base Header: EventType (Unsigned Byte), Timestamp (Unsigned Int)
SA_BASE_HEADER_FORMAT = '>BI'
SA_BASE_HEADER_SIZE = struct.calcsize(SA_BASE_HEADER_FORMAT)

# String: Length (Unsigned Short), String Bytes (UTF-8)
SA_STRING_LEN_FORMAT = '>H'
SA_STRING_LEN_SIZE = struct.calcsize(SA_STRING_LEN_FORMAT)

# List (e.g., Tags): Number of Items (Unsigned Short), followed by N items
SA_LIST_HEADER_FORMAT = '>H'
SA_LIST_HEADER_SIZE = struct.calcsize(SA_LIST_HEADER_FORMAT)

# Integer (e.g., duration, peak_viewers in EVENT_TYPE_STREAM_END)
SA_INT_FORMAT = '>I'
SA_INT_SIZE = struct.calcsize(SA_INT_FORMAT)


# --- Bot Session Log Event Types & Formats ---
BOT_EVENT_START = 1
BOT_EVENT_STOP = 2

# Bot Session Record: EventType (Unsigned Byte), Timestamp (Unsigned Int)
BOT_SESSION_RECORD_FORMAT = '>BI'
BOT_SESSION_RECORD_SIZE = struct.calcsize(BOT_SESSION_RECORD_FORMAT)