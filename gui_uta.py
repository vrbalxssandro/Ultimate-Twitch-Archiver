import os
import sys
import subprocess
import threading
import time
import logging
import json # For config.json
import signal # For stopping bot process on non-Windows
import re # For parsing logs
import webbrowser # For opening YT link

try:
    import customtkinter as ctk
    from customtkinter import CTkFont, CTkLabel, CTkButton, CTkEntry, CTkSwitch, CTkFrame, CTkTextbox, CTkScrollableFrame, CTkOptionMenu
    from PIL import Image, ImageTk
    from tkinter import filedialog, messagebox
except ImportError:
    print("CustomTkinter or Pillow not found. Attempting to install...")
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "customtkinter", "pillow"])
        import customtkinter as ctk
        from customtkinter import CTkFont, CTkLabel, CTkButton, CTkEntry, CTkSwitch, CTkFrame, CTkTextbox, CTkScrollableFrame, CTkOptionMenu
        from PIL import Image, ImageTk
        from tkinter import filedialog, messagebox
        print("Installation successful. Please re-run the script.")
        sys.exit(0) # Exit so user can re-run with imported libraries
    except Exception as e:
        print(f"Failed to install packages: {e}")
        print("Please install them manually: pip install customtkinter pillow")
        sys.exit(1)

# --- Configuration Constants and Functions ---
CONFIG_FILE = 'config.json'
DEFAULT_CONFIG = {
    "DISCORD_TOKEN": "YOUR_DISCORD_TOKEN", "DISCORD_BOT_OWNER_ID": None,
    "TWITCH_CLIENT_ID": "YOUR_TWITCH_CLIENT_ID", "TWITCH_CLIENT_SECRET": "YOUR_TWITCH_CLIENT_SECRET",
    "FCTD_TWITCH_USERNAME": "target_twitch_username_for_fctd", "FCTD_TARGET_CHANNEL_ID": None,
    "FCTD_COMMAND_CHANNEL_ID": None, "FCTD_COMMAND_PREFIX": "!", "FCTD_UPDATE_INTERVAL_MINUTES": 2,
    "FCTD_CHANNEL_NAME_PREFIX": "Followers: ", "FCTD_CHANNEL_NAME_SUFFIX": "", "FCTD_FOLLOWER_DATA_FILE": "follower_counts.bin",
    "UTA_STREAM_DURATION_LOG_FILE": "stream_durations.bin", "UTA_ENABLED": False,
    "UTA_TWITCH_CHANNEL_NAME": "target_twitch_username_for_uta", "UTA_CLIP_MONITOR_ENABLED": False,
    "UTA_DISCORD_WEBHOOK_URL_CLIPS": "YOUR_DISCORD_WEBHOOK_URL_CLIPS", "UTA_CHECK_INTERVAL_SECONDS_CLIPS": 300,
    "UTA_CLIP_LOOKBACK_MINUTES": 5, "UTA_RESTREAMER_ENABLED": False,
    "UTA_DISCORD_WEBHOOK_URL_RESTREAMER": "YOUR_DISCORD_WEBHOOK_URL_RESTREAMER",
    "UTA_YOUTUBE_RTMP_URL_BASE": "rtmp://a.rtmp.youtube.com/live2", "UTA_YOUTUBE_STREAM_KEY": "YOUR_YOUTUBE_STREAM_KEY",
    "UTA_CHECK_INTERVAL_SECONDS_RESTREAMER": 60, "UTA_RESTREAM_CHECK_INTERVAL_WHEN_LIVE": 300,
    "UTA_POST_RESTREAM_COOLDOWN_SECONDS": 60, "UTA_STREAMLINK_PATH": "streamlink", "UTA_FFMPEG_PATH": "ffmpeg",
    "UTA_STREAM_STATUS_NOTIFICATIONS_ENABLED": False, "UTA_STREAM_STATUS_WEBHOOK_URL": "YOUR_DISCORD_WEBHOOK_URL_STATUS",
    "UTA_STREAM_STATUS_CHANNEL_ID": None, "UTA_STREAM_STATUS_POLL_INTERVAL_SECONDS": 60,
    "UTA_STREAM_ACTIVITY_LOG_FILE": "stream_activity.bin", "UTA_VIEWER_COUNT_LOGGING_ENABLED": False,
    "UTA_VIEWER_COUNT_LOG_INTERVAL_SECONDS": 300, "UTA_VIEWER_COUNT_LOG_FILE": "viewer_counts.bin",
    "BOT_SESSION_LOG_FILE": "bot_sessions.bin", "UTA_YOUTUBE_API_ENABLED": False,
    "UTA_YOUTUBE_CLIENT_SECRET_FILE": "client_secret.json", "UTA_YOUTUBE_TOKEN_FILE": "youtube_token.json",
    "UTA_YOUTUBE_PLAYLIST_ID": None, "UTA_YOUTUBE_DEFAULT_PRIVACY": "unlisted",
    "UTA_YOUTUBE_MAKE_PUBLIC_AFTER_STREAM": False, "UTA_YOUTUBE_SCHEDULED_ROLLOVER_HOURS": 0.0,
    "UTA_YOUTUBE_DYNAMIC_TITLE_TEMPLATE": "{twitch_username} - {twitch_title} ({game_name}) - Part {part_num} [{date}]",
    "UTA_YOUTUBE_DYNAMIC_DESCRIPTION_TEMPLATE": "Originally streamed by {twitch_username} on Twitch: https://twitch.tv/{twitch_username}\nGame: {game_name}\nTitle: {twitch_title}\n\nArchived by UTA.",
    "UTA_RESTREAM_MAX_CONSECUTIVE_FAILURES": 3, "UTA_RESTREAM_LONG_COOLDOWN_SECONDS": 300,
    "UTA_YOUTUBE_PLAYABILITY_CHECK_ENABLED": True, "UTA_YOUTUBE_PLAYABILITY_CHECK_RETRIES": 2,
    "UTA_YOUTUBE_PLAYABILITY_CHECK_DELAY_SECONDS": 15, "UTA_FFMPEG_STARTUP_WAIT_SECONDS": 10,
    "UTA_YOUTUBE_AUTO_CHAPTERS_ENABLED": True,
    "UTA_YOUTUBE_MIN_CHAPTER_DURATION_SECONDS": 60,
    "UTA_YOUTUBE_DESCRIPTION_CHAPTER_MARKER": "## UTA Auto Chapters ##",
    "UTA_YOUTUBE_CHAPTER_TITLE_TEMPLATE": "{game_name} - {twitch_title}",
    # New Twitch Chat Configs
    "TWITCH_CHAT_ENABLED": False,
    "TWITCH_CHAT_NICKNAME": "YourBotTwitchNickname", # Bot's Twitch username
    "TWITCH_CHAT_OAUTH_TOKEN": "oauth:yourtwitchtoken", # oauth:xxxxxxxxxxxx
    # TWITCH_CHAT_TARGET_CHANNEL will reuse UTA_TWITCH_CHANNEL_NAME
    "TWITCH_CHAT_LOG_INTERVAL_SECONDS": 60,
    "TWITCH_CHAT_ACTIVITY_LOG_FILE": "chat_activity.bin",
    "DISCORD_TWITCH_CHAT_MIRROR_ENABLED": False,
    "DISCORD_TWITCH_CHAT_MIRROR_CHANNEL_ID": None,
}
current_config = {}

gui_logger = logging.getLogger("UTA_GUI")
gui_logger.setLevel(logging.INFO)
if not gui_logger.handlers:
    console_handler_gui = logging.StreamHandler(sys.stdout)
    console_handler_gui.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    gui_logger.addHandler(console_handler_gui)

def load_config_from_file():
    global current_config
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r') as f:
                loaded_config = json.load(f)
                current_config = DEFAULT_CONFIG.copy()
                current_config.update(loaded_config)
        except json.JSONDecodeError:
            gui_logger.error(f"Error decoding {CONFIG_FILE}. Using default values and attempting to overwrite.")
            current_config = DEFAULT_CONFIG.copy()
            save_config_to_file(current_config)
        except Exception as e:
            gui_logger.error(f"Error loading {CONFIG_FILE}: {e}. Using default values.")
            current_config = DEFAULT_CONFIG.copy()
    else:
        gui_logger.info(f"{CONFIG_FILE} not found. Creating with default values.")
        current_config = DEFAULT_CONFIG.copy()
        save_config_to_file(current_config)

def save_config_to_file(config_to_save):
    try:
        with open(CONFIG_FILE, 'w') as f:
            json.dump(config_to_save, f, indent=4)
        gui_logger.info(f"Configuration saved to {CONFIG_FILE}")
        return True
    except Exception as e:
        gui_logger.error(f"Error saving configuration to {CONFIG_FILE}: {e}")
        try:
            if ctk.get_appearance_mode(): # Check if GUI is initialized
                 messagebox.showerror("Config Save Error", f"Could not save config: {e}")
        except Exception: pass # In case messagebox itself errors before GUI is up
        return False

load_config_from_file() # Load config when GUI script starts
ctk.set_appearance_mode("Dark")
ctk.set_default_color_theme("dark-blue")
bot_process = None
bot_active = False # Tracks if the bot process is intended to be running

class ScrollableLogFrame(CTkFrame):
    def __init__(self, master, **kwargs):
        super().__init__(master, **kwargs)
        self.log_textbox = CTkTextbox(self, wrap="word", activate_scrollbars=True, state="disabled",
                                      font=("Consolas", 11), border_width=1, border_color=("gray70", "gray30"))
        self.log_textbox.pack(fill="both", expand=True, padx=5, pady=5)
        # Configure tags for colored text
        self.log_textbox.tag_config("red_error", foreground="#FF6B6B") # Light red for errors
        self.log_textbox.tag_config("warning", foreground="#FFAE42") # Orange for warnings

    def add_log(self, message):
        if not self.winfo_exists(): return # Check if widget exists
        self.log_textbox.configure(state="normal")
        start_index = self.log_textbox.index("end-1c") # Get index before inserting
        self.log_textbox.insert("end", message)
        end_index = self.log_textbox.index("end-1c") # Get index after inserting

        # Apply color tags based on content
        message_lower = str(message).lower()
        if "error:" in message_lower or "critical:" in message_lower or "traceback (most recent call last):" in message_lower or "exception:" in message_lower or "fail" in message_lower :
            self.log_textbox.tag_add("red_error", start_index, end_index)
        elif "warning:" in message_lower:
            self.log_textbox.tag_add("warning", start_index, end_index)

        self.log_textbox.see("end")
        self.log_textbox.configure(state="disabled")

    def clear_logs(self):
        if not self.winfo_exists(): return
        self.log_textbox.configure(state="normal")
        self.log_textbox.delete("1.0", "end")
        # Re-apply tag configurations as they might be cleared with delete
        self.log_textbox.tag_config("red_error", foreground="#FF6B6B")
        self.log_textbox.tag_config("warning", foreground="#FFAE42")
        self.log_textbox.configure(state="disabled")

class UtaBotGui(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title("UTA Bot Control Panel")
        self.geometry("1100x850")
        self.protocol("WM_DELETE_WINDOW", self.on_close)

        self.config_changed_by_user = False
        self.vars = {} # To store CTk variables for config fields
        self.is_quick_restarting = False

        # Define fonts
        self.heading_font = CTkFont(size=15, weight="bold")
        self.sub_heading_font = CTkFont(size=13, weight="bold")
        self.label_font = CTkFont(size=12)
        self.small_label_font = CTkFont(size=10)

        # Status colors
        self.status_active_color = "#4CAF50"  # Green
        self.status_inactive_color = "gray60" # Default gray
        self.status_error_color = "#F44336"   # Red
        self.status_stopping_color = "#FF9800" # Orange

        self.link_color = "#60A5FA" # Light blue for links

        # Dynamic status variables for restreamer
        self.current_yt_video_id_var = ctk.StringVar(value="N/A")
        self.current_yt_part_num_var = ctk.StringVar(value="N/A")
        self.current_yt_watch_link = None # Store the actual URL

        self.yt_playability_status_var = ctk.StringVar(value="N/A")
        self.consecutive_failures_var = ctk.StringVar(value="0/0") # e.g. "1/3"
        self.cooldown_status_var = ctk.StringVar(value="Inactive")


        self.setup_ui()
        self.populate_fields_from_config()
        self.setup_config_tracers() # After populating, to avoid premature triggers
        self.update_dynamic_buttons_state() # Initial state of buttons

    def _get_var(self, key, val_from_cfg):
        """Helper to get or create a CTk Variable for a config key."""
        if key not in self.vars:
            if isinstance(val_from_cfg, bool):
                self.vars[key] = ctk.BooleanVar(value=val_from_cfg)
            else: # Handles str, int, float (will be stored as string in StringVar)
                self.vars[key] = ctk.StringVar(value=str(val_from_cfg) if val_from_cfg is not None else "")
        return self.vars[key]

    def populate_fields_from_config(self):
        """Populates GUI fields from the global current_config."""
        for key, value in current_config.items():
            var = self._get_var(key, value) # Ensures var is created if not exists
            try:
                if isinstance(var, ctk.StringVar) and value is None:
                    var.set("") # Set empty string for None in StringVars
                elif isinstance(var, ctk.BooleanVar):
                    var.set(bool(value))
                else:
                    var.set(value if value is not None else "") # Set value or empty string
            except Exception as e:
                gui_logger.error(f"Error populating var {key} with {value}: {e}")


    def setup_config_tracers(self):
        """Adds trace to all config variables to detect changes."""
        for key, var_obj in self.vars.items():
            var_obj.trace_add("write", lambda n,i,m,k=key: self.on_config_entry_change(k))

    def on_config_entry_change(self, key_changed):
        gui_logger.debug(f"Configuration entry '{key_changed}' changed by user.")
        self.config_changed_by_user = True


    def setup_ui(self):
        # Main layout
        self.main_container = CTkFrame(self, fg_color="transparent")
        self.main_container.pack(fill="both", expand=True, padx=15, pady=15)

        self.left_panel = CTkFrame(self.main_container)
        self.left_panel.pack(side="left", fill="both", expand=True, padx=(0, 10), pady=0)

        self.right_panel = CTkFrame(self.main_container, width=380) # Fixed width for right panel
        self.right_panel.pack(side="right", fill="y", expand=False, padx=(10, 0), pady=0)
        self.right_panel.pack_propagate(False) # Prevent right panel from shrinking

        self.setup_tabs()
        self.setup_control_panel()
        self.setup_log_panel()

    def _create_entry_field(self, parent, label_text, config_key, is_password=False, is_nullable_int=False):
        frame = CTkFrame(parent, fg_color="transparent")
        frame.pack(fill="x", padx=10, pady=4) # Consistent padding
        CTkLabel(frame, text=label_text, width=230, anchor="w", font=self.label_font).pack(side="left", padx=(0,10))

        initial_val = current_config.get(config_key)
        # For nullable_int, default to empty string if None, otherwise convert to string.
        # Booleans are handled by _create_switch_field. Other Nones also become empty string.
        if is_nullable_int and initial_val is None:
            var_val_str = ""
        else:
            var_val_str = str(initial_val) if initial_val is not None else ""


        var = self._get_var(config_key, var_val_str) # Use var_val_str which is always string or empty
        entry = CTkEntry(frame, textvariable=var, show="*" if is_password else None, width=300) # Ensure entry width
        entry.pack(side="left", fill="x", expand=True)

    def _create_switch_field(self, parent, label_text, config_key):
        frame = CTkFrame(parent, fg_color="transparent")
        frame.pack(fill="x", padx=10, pady=4) # Consistent padding
        var = self._get_var(config_key, current_config.get(config_key, False))
        CTkLabel(frame, text=label_text, anchor="w", font=self.label_font).pack(side="left", padx=(0,10))
        switch = CTkSwitch(frame, text="", variable=var, onvalue=True, offvalue=False)
        switch.pack(side="right") # Anchor switch to the right

    def _create_browse_field(self, parent, label_text, config_key):
        frame = CTkFrame(parent, fg_color="transparent")
        frame.pack(fill="x", padx=10, pady=4)
        CTkLabel(frame, text=label_text, width=230, anchor="w", font=self.label_font).pack(side="left", padx=(0,10))
        var = self._get_var(config_key, current_config.get(config_key, ""))
        entry = CTkEntry(frame, textvariable=var, width=250) # Adjusted width
        entry.pack(side="left", fill="x", expand=True, padx=(0,5))
        CTkButton(frame, text="Browse", width=70, command=lambda vr=var: self.browse_executable(vr)).pack(side="left")

    def setup_tabs(self):
        self.tabview = ctk.CTkTabview(self.left_panel, border_width=1, border_color=("gray70", "gray30"))
        self.tabview.pack(fill="both", expand=True, padx=0, pady=0)

        # Configuration for tabs and their fields
        tabs_config = {
            "General": [
                ("DISCORD_TOKEN", "Discord Bot Token:", {"is_password": True}),
                ("DISCORD_BOT_OWNER_ID", "Bot Owner User ID:", {"is_nullable_int": True}),
                ("TWITCH_CLIENT_ID", "Twitch Client ID:"),
                ("TWITCH_CLIENT_SECRET", "Twitch Client Secret:", {"is_password": True}),
                ("FCTD_COMMAND_PREFIX", "Bot Command Prefix:"),
                ("BOT_SESSION_LOG_FILE", "Bot Session Log File:")
            ],
            "Follower Counter": [
                ("FCTD_TWITCH_USERNAME", "Twitch Username (Followers):"),
                ("FCTD_TARGET_CHANNEL_ID", "Discord Channel ID (Update Name):", {"is_nullable_int": True}),
                ("FCTD_COMMAND_CHANNEL_ID", "Discord Command Channel ID (Optional):", {"is_nullable_int": True}),
                ("FCTD_UPDATE_INTERVAL_MINUTES", "Update Interval (Minutes):"),
                ("FCTD_CHANNEL_NAME_PREFIX", "Channel Name Prefix:"),
                ("FCTD_CHANNEL_NAME_SUFFIX", "Channel Name Suffix:"),
                ("FCTD_FOLLOWER_DATA_FILE", "Follower Data File:")
            ],
            "UTA General": [
                ("UTA_ENABLED", "Enable UTA Features", {"is_switch": True}),
                ("UTA_TWITCH_CHANNEL_NAME", "Target Twitch Channel (UTA & Chat):"), # Clarified usage
                ("UTA_STREAM_DURATION_LOG_FILE", "Stream Duration Log File:")
            ],
            "UTA Chat Monitor": [ # New Tab
                ("TWITCH_CHAT_ENABLED", "Enable Twitch Chat Monitoring", {"is_switch": True}),
                ("TWITCH_CHAT_NICKNAME", "Bot's Twitch Username (for chat):"),
                ("TWITCH_CHAT_OAUTH_TOKEN", "Bot's Twitch Chat OAuth Token:", {"is_password": True}),
                ("TWITCH_CHAT_LOG_INTERVAL_SECONDS", "Chat Log Interval (s):"),
                ("TWITCH_CHAT_ACTIVITY_LOG_FILE", "Chat Activity Log File:"),
                ("DISCORD_TWITCH_CHAT_MIRROR_ENABLED", "Mirror Twitch Chat to Discord", {"is_switch": True}),
                ("DISCORD_TWITCH_CHAT_MIRROR_CHANNEL_ID", "Discord Mirror Channel ID:", {"is_nullable_int": True}),
            ],
            "UTA Clip Monitor": [
                ("UTA_CLIP_MONITOR_ENABLED", "Enable Clip Monitor", {"is_switch": True}),
                ("UTA_DISCORD_WEBHOOK_URL_CLIPS", "Clips Discord Webhook URL:"),
                ("UTA_CHECK_INTERVAL_SECONDS_CLIPS", "Clip Check Interval (s):"),
                ("UTA_CLIP_LOOKBACK_MINUTES", "Clip Lookback (min):")
            ],
            "UTA Restreamer": [
                ("UTA_RESTREAMER_ENABLED", "Enable Restreamer", {"is_switch": True}),
                ("UTA_DISCORD_WEBHOOK_URL_RESTREAMER", "Restreamer Discord Webhook URL:"),
                ("UTA_YOUTUBE_API_ENABLED", "Use YouTube API Mode", {"is_switch": True}),
                ("UTA_YOUTUBE_CLIENT_SECRET_FILE", "YouTube Client Secret File:"),
                ("UTA_YOUTUBE_TOKEN_FILE", "YouTube Token File:"),
                ("UTA_YOUTUBE_PLAYLIST_ID", "YouTube Playlist ID (Optional):"),
                ("UTA_YOUTUBE_DEFAULT_PRIVACY", "YouTube Default Privacy:", {"options": ["public", "unlisted", "private"]}),
                ("UTA_YOUTUBE_MAKE_PUBLIC_AFTER_STREAM", "Make VOD Public After Stream", {"is_switch": True}),
                ("UTA_YOUTUBE_SCHEDULED_ROLLOVER_HOURS", "YT Rollover Hours (0=disable):"),
                ("UTA_YOUTUBE_DYNAMIC_TITLE_TEMPLATE", "YT Title Template:"),
                ("UTA_YOUTUBE_DYNAMIC_DESCRIPTION_TEMPLATE", "YT Description Template:"),
                ("UTA_YOUTUBE_RTMP_URL_BASE", "Legacy YT RTMP URL Base:"),
                ("UTA_YOUTUBE_STREAM_KEY", "Legacy YT Stream Key:", {"is_password": True}),
                ("UTA_CHECK_INTERVAL_SECONDS_RESTREAMER", "Restreamer Offline Check (s):"),
                ("UTA_RESTREAM_CHECK_INTERVAL_WHEN_LIVE", "Restreamer Live Check (s):"),
                ("UTA_POST_RESTREAM_COOLDOWN_SECONDS", "Post-Restream Cooldown (s):"),
                ("UTA_YOUTUBE_AUTO_CHAPTERS_ENABLED", "Enable Auto YouTube Chapters", {"is_switch": True}),
                ("UTA_YOUTUBE_MIN_CHAPTER_DURATION_SECONDS", "Min. Duration for YT Chapter (s):"),
                ("UTA_YOUTUBE_DESCRIPTION_CHAPTER_MARKER", "Description Marker for Chapters:"),
                ("UTA_YOUTUBE_CHAPTER_TITLE_TEMPLATE", "YT Chapter Title Template:")
            ],
            "UTA Status Monitor": [
                ("UTA_STREAM_STATUS_NOTIFICATIONS_ENABLED", "Enable Stream Status Notifications", {"is_switch": True}),
                ("UTA_STREAM_STATUS_WEBHOOK_URL", "Status Discord Webhook URL:"),
                ("UTA_STREAM_STATUS_CHANNEL_ID", "Status Discord Channel ID:", {"is_nullable_int": True}),
                ("UTA_STREAM_STATUS_POLL_INTERVAL_SECONDS", "Status Poll Interval (s):"),
                ("UTA_STREAM_ACTIVITY_LOG_FILE", "Stream Activity Log File:"),
                ("UTA_VIEWER_COUNT_LOGGING_ENABLED", "Enable Viewer Count Logging", {"is_switch": True}),
                ("UTA_VIEWER_COUNT_LOG_INTERVAL_SECONDS", "Viewer Log Interval (s):"),
                ("UTA_VIEWER_COUNT_LOG_FILE", "Viewer Count Log File:")
            ],
             "Reliability (UTA)": [
                ("UTA_RESTREAM_MAX_CONSECUTIVE_FAILURES", "Max Restream Fails:"),
                ("UTA_RESTREAM_LONG_COOLDOWN_SECONDS", "Long Cooldown (s):"),
                ("UTA_YOUTUBE_PLAYABILITY_CHECK_ENABLED", "Enable YT Playability Check", {"is_switch":True}),
                ("UTA_YOUTUBE_PLAYABILITY_CHECK_RETRIES", "Playability Retries:"),
                ("UTA_YOUTUBE_PLAYABILITY_CHECK_DELAY_SECONDS", "Playability Delay (s):"),
                ("UTA_FFMPEG_STARTUP_WAIT_SECONDS", "FFmpeg Startup Wait (s):")
            ],
            "Paths": [
                ("UTA_STREAMLINK_PATH", "Streamlink Path:", {"is_browse": True}),
                ("UTA_FFMPEG_PATH", "FFmpeg Path:", {"is_browse": True})
            ]
        }

        for tab_name, fields in tabs_config.items():
            tab = self.tabview.add(tab_name)
            scroll_frame = CTkScrollableFrame(tab, fg_color="transparent") # Ensure scrollable frame is transparent
            scroll_frame.pack(fill="both", expand=True, padx=0, pady=0)

            for item in fields:
                key, label_text = item[0], item[1]
                options = item[2] if len(item) > 2 else {}

                if options.get("is_switch"):
                    self._create_switch_field(scroll_frame, label_text, key)
                elif options.get("is_browse"):
                    self._create_browse_field(scroll_frame, label_text, key)
                elif "options" in options: # Dropdown / OptionMenu
                    frame=CTkFrame(scroll_frame, fg_color="transparent");frame.pack(fill="x",padx=10,pady=4)
                    CTkLabel(frame,text=label_text,width=230,anchor="w", font=self.label_font).pack(side="left",padx=(0,10))
                    var=self._get_var(key,current_config.get(key,options["options"][0]))
                    CTkOptionMenu(frame,variable=var,values=options["options"], width=300).pack(side="left",fill="x",expand=True)
                else: # Default to entry field
                    self._create_entry_field(scroll_frame, label_text, key,
                                             is_password=options.get("is_password", False),
                                             is_nullable_int=options.get("is_nullable_int", False))
        self.tabview.set("General") # Default to General tab

    def setup_control_panel(self):
        control_outer_frame = CTkFrame(self.right_panel) # No fg_color, use default
        control_outer_frame.pack(fill="x", padx=0, pady=0, side="top")

        # Status Section
        status_section = CTkFrame(control_outer_frame, corner_radius=6) # Add corner radius
        status_section.pack(fill="x", padx=10, pady=(10,5)) # Add some padding
        CTkLabel(status_section, text="Bot Status:", font=self.heading_font).pack(side="left", padx=(10,5), pady=10)
        self.status_label = CTkLabel(status_section, text="Inactive", text_color=self.status_inactive_color, font=self.label_font)
        self.status_label.pack(side="left", padx=5, pady=10)

        # Container for dynamic info (YT ID, playability, etc.)
        self.dynamic_info_container = CTkFrame(control_outer_frame, fg_color="transparent")
        self.dynamic_info_container.pack(fill="x", padx=0, pady=0) # No vertical padding for the container itself

        # YouTube API Info Frame (conditionally shown)
        self.yt_api_info_frame = CTkFrame(self.dynamic_info_container, fg_color="transparent") # Initially not packed
        # Content within yt_api_info_frame (this gets the border/background)
        yt_api_info_content_frame = CTkFrame(self.yt_api_info_frame, corner_radius=6) # Add corner radius
        yt_api_info_content_frame.pack(fill="x", expand=True, padx=10, pady=(0,5)) # Pad content frame

        yt_info_row1 = CTkFrame(yt_api_info_content_frame, fg_color="transparent"); yt_info_row1.pack(fill="x",pady=(5,2), padx=5)
        CTkLabel(yt_info_row1,text="YT Video ID:",font=self.small_label_font, anchor="w").pack(side="left",padx=(0,2))
        self.yt_video_id_label=CTkLabel(yt_info_row1,textvariable=self.current_yt_video_id_var,font=self.small_label_font, anchor="w", wraplength=150); self.yt_video_id_label.pack(side="left",padx=2)

        yt_info_row2 = CTkFrame(yt_api_info_content_frame, fg_color="transparent"); yt_info_row2.pack(fill="x",pady=(0,5), padx=5)
        CTkLabel(yt_info_row2,text="Part:",font=self.small_label_font, anchor="w").pack(side="left",padx=(0,2))
        self.yt_part_num_label=CTkLabel(yt_info_row2,textvariable=self.current_yt_part_num_var,font=self.small_label_font, anchor="w"); self.yt_part_num_label.pack(side="left",padx=2)
        self.yt_watch_link_label = CTkLabel(yt_info_row2, text="", text_color=self.link_color, cursor="hand2", font=CTkFont(size=10, underline=True), anchor="w")
        # self.yt_watch_link_label will be packed/unpacked in _update_youtube_info_display


        # Detailed Restream Status Frame (conditionally shown)
        self.detailed_restream_status_frame = CTkFrame(self.dynamic_info_container, fg_color="transparent") # Initially not packed
        detailed_restream_content_frame = CTkFrame(self.detailed_restream_status_frame, corner_radius=6)
        detailed_restream_content_frame.pack(fill="x", expand=True, padx=10, pady=5) # Pad content frame

        detail_row1 = CTkFrame(detailed_restream_content_frame, fg_color="transparent"); detail_row1.pack(fill="x", pady=2, padx=5)
        CTkLabel(detail_row1, text="Playability:", font=self.small_label_font, anchor="w").pack(side="left", padx=(0,2))
        self.playability_label = CTkLabel(detail_row1, textvariable=self.yt_playability_status_var, font=self.small_label_font, anchor="w", wraplength=150); self.playability_label.pack(side="left", padx=2)

        detail_row2 = CTkFrame(detailed_restream_content_frame, fg_color="transparent"); detail_row2.pack(fill="x", pady=2, padx=5)
        CTkLabel(detail_row2, text="Pipe Fails:", font=self.small_label_font, anchor="w").pack(side="left", padx=(0,2))
        self.failures_label = CTkLabel(detail_row2, textvariable=self.consecutive_failures_var, font=self.small_label_font, anchor="w"); self.failures_label.pack(side="left", padx=2)

        detail_row3 = CTkFrame(detailed_restream_content_frame, fg_color="transparent"); detail_row3.pack(fill="x", pady=2, padx=5)
        CTkLabel(detail_row3, text="Cooldown:", font=self.small_label_font, anchor="w").pack(side="left", padx=(0,2))
        self.cooldown_label = CTkLabel(detail_row3, textvariable=self.cooldown_status_var, font=self.small_label_font, anchor="w", wraplength=150); self.cooldown_label.pack(side="left", padx=2)


        # Buttons Section
        buttons_section = CTkFrame(control_outer_frame) # No fg_color
        buttons_section.pack(fill="x", padx=10, pady=(10,5))

        button_frame_row1 = CTkFrame(buttons_section, fg_color="transparent"); button_frame_row1.pack(fill="x", pady=3)
        self.start_button = CTkButton(button_frame_row1, text="Start Bot", command=self.start_bot_process, fg_color=self.status_active_color, hover_color="#3E8E41") # Darker green hover
        self.start_button.pack(side="left", padx=5, pady=5, fill="x", expand=True)
        self.stop_button = CTkButton(button_frame_row1, text="Stop Bot", command=self.stop_bot_process_command, fg_color=self.status_error_color, hover_color="#C9302C", state="disabled") # Darker red hover
        self.stop_button.pack(side="left", padx=5, pady=5, fill="x", expand=True)

        button_frame_row2 = CTkFrame(buttons_section, fg_color="transparent"); button_frame_row2.pack(fill="x", pady=3)
        self.quick_restart_button = CTkButton(button_frame_row2, text="Quick Restart Bot", command=self.quick_restart_bot, state="disabled")
        self.quick_restart_button.pack(side="left", padx=5, pady=5, fill="x", expand=True)
        self.force_new_part_button = CTkButton(button_frame_row2, text="Force New YT Part", command=self.force_new_yt_part, state="disabled")
        self.force_new_part_button.pack(side="left", padx=5, pady=5, fill="x", expand=True)

        button_frame_row3 = CTkFrame(buttons_section, fg_color="transparent"); button_frame_row3.pack(fill="x", pady=3)
        self.test_yt_api_button = CTkButton(button_frame_row3, text="Test YT API", command=self.test_youtube_api_connection)
        self.test_yt_api_button.pack(side="left", padx=5, pady=5, fill="x", expand=True)
        self.auth_yt_api_button = CTkButton(button_frame_row3, text="Re-Auth YT API", command=self.authorize_youtube_api)
        self.auth_yt_api_button.pack(side="left", padx=5, pady=5, fill="x", expand=True)


        self.save_cfg_button = CTkButton(buttons_section, text="Save Configuration", command=self.save_gui_config)
        self.save_cfg_button.pack(fill="x", padx=5, pady=(10,5)) # Add some top margin

    def _update_youtube_info_display(self, video_id=None, part_num=None):
        self.dynamic_info_container.update_idletasks() # Ensure container is up-to-date

        # Determine if the YouTube API info section should be visible
        show_yt_api_info = bot_active and \
                           current_config.get("UTA_RESTREAMER_ENABLED", False) and \
                           current_config.get("UTA_YOUTUBE_API_ENABLED", False) and \
                           video_id and part_num

        if show_yt_api_info:
            self.current_yt_video_id_var.set(video_id)
            self.current_yt_part_num_var.set(str(part_num))
            self.current_yt_watch_link = f"https://www.youtube.com/watch?v={video_id}"
            self.yt_watch_link_label.configure(text="[Watch on YouTube]")
            self.yt_watch_link_label.bind("<Button-1>", lambda e: self.open_youtube_link())
            if not self.yt_watch_link_label.winfo_ismapped(): # Only pack if not already visible
                 self.yt_watch_link_label.pack(side="left",padx=(10,2), pady=0) # Pack next to part number

            if not self.yt_api_info_frame.winfo_ismapped():
                self.yt_api_info_frame.pack(fill="x", padx=0, pady=0, before=self.detailed_restream_status_frame)
        else:
            self.current_yt_video_id_var.set("N/A")
            self.current_yt_part_num_var.set("N/A")
            self.current_yt_watch_link = None
            if self.yt_watch_link_label.winfo_ismapped(): # Unpack if visible
                self.yt_watch_link_label.pack_forget()
            if self.yt_api_info_frame.winfo_ismapped(): # Unpack the whole frame if visible
                self.yt_api_info_frame.pack_forget()

        # Ensure detailed status display is updated correctly relative to the (possibly now hidden) yt_api_info_frame
        self._update_detailed_restream_status_display()


    def _update_detailed_restream_status_display(self, play_status=None, fails_str=None, cool_status=None):
        self.dynamic_info_container.update_idletasks() # Ensure container is up-to-date

        if play_status is not None: self.yt_playability_status_var.set(play_status)
        if fails_str is not None: self.consecutive_failures_var.set(fails_str)
        if cool_status is not None: self.cooldown_status_var.set(cool_status)

        should_show_details = bot_active and current_config.get("UTA_RESTREAMER_ENABLED", False)

        if should_show_details:
            if not self.detailed_restream_status_frame.winfo_ismapped():
                 # This ensures it's packed into dynamic_info_container.
                 # If yt_api_info_frame is also visible, detailed_restream_status_frame will be packed after it.
                 # If yt_api_info_frame is hidden, detailed_restream_status_frame will be packed directly.
                 self.detailed_restream_status_frame.pack(fill="x", padx=0, pady=(0,5))
        else: # Hide if not applicable
            if self.detailed_restream_status_frame.winfo_ismapped():
                self.detailed_restream_status_frame.pack_forget()

    def open_youtube_link(self):
        if self.current_yt_watch_link:
            webbrowser.open_new_tab(self.current_yt_watch_link)
            self.log_display_frame.add_log(f"Opened YouTube link: {self.current_yt_watch_link}\n")

    def add_log_entry(self, message):
        # Ensure the log display frame exists and is valid
        if not hasattr(self, 'log_display_frame') or not self.log_display_frame.winfo_exists():
            return
        self.log_display_frame.add_log(message)

        # Check for YouTube Video ID and Part Number log messages
        yt_match = re.search(r"UTA YouTube: .* broadcast [a-zA-Z0-9_-]+ \(Video ID: ([a-zA-Z0-9_-]+)\) for Part (\d+)\.", message)
        if yt_match:
            video_id = yt_match.group(1)
            part_num = yt_match.group(2)
            self.after(0, self._update_youtube_info_display, video_id, part_num)
        elif "UTA_GUI_LOG: YouTubeVideoID=N/A" in message: # Explicit reset from bot log
             self.after(0, self._update_youtube_info_display) # Clears YT info


        # Check for GUI-specific log messages for detailed restream status
        playability_match = re.search(r"UTA_GUI_LOG: PlayabilityCheckStatus=([a-zA-Z0-9\s/()_-]+)", message)
        if playability_match:
            self.after(0, self._update_detailed_restream_status_display, play_status=playability_match.group(1).strip())

        failures_match = re.search(r"UTA_GUI_LOG: ConsecutiveFailures=(\d+)", message)
        if failures_match:
            max_fails = current_config.get('UTA_RESTREAM_MAX_CONSECUTIVE_FAILURES', 3)
            self.after(0, self._update_detailed_restream_status_display, fails_str=f"{failures_match.group(1)}/{max_fails}")

        cooldown_match = re.search(r"UTA_GUI_LOG: CooldownStatus=([a-zA-Z0-9_()]+(\d+s)?)", message) # Updated regex for optional duration
        if cooldown_match:
             self.after(0, self._update_detailed_restream_status_display, cool_status=cooldown_match.group(1).strip())


        # If bot exits or specific UTA conditions, reset the display
        if any(term in message for term in ["Bot process exited", "UTA Restream: Target Twitch Channel not set", "UTA_GUI_LOG: RestreamPipeStatus=Stopped", "UTA_GUI_LOG: RestreamPipeStatus=Inactive_SessionEnded", "UTA_GUI_LOG: RestreamPipeStatus=ErrorState"]):
             if not bot_active: # Only reset if bot is confirmed not active, to avoid flicker during restarts
                self.after(0, self._update_youtube_info_display) # Clears YT info
                self.after(0, self._update_detailed_restream_status_display,
                           play_status="N/A",
                           fails_str=f"0/{current_config.get('UTA_RESTREAM_MAX_CONSECUTIVE_FAILURES',3)}",
                           cool_status="Inactive")


    def test_youtube_api_connection(self):
        if not current_config.get("UTA_YOUTUBE_API_ENABLED"):
            messagebox.showinfo("API Disabled","YouTube API is not enabled in the configuration.")
            return
        if not os.path.exists(current_config.get("UTA_YOUTUBE_CLIENT_SECRET_FILE","client_secret.json")):
            messagebox.showerror("Client Secret Missing",f"YouTube client secret file ('{current_config.get('UTA_YOUTUBE_CLIENT_SECRET_FILE')}') not found.")
            return

        self.log_display_frame.add_log("Testing YouTube API: Please start the bot. It will log API initialization success or guide you through OAuth if the token is invalid/missing.\n")
        messagebox.showinfo("Test Initiated","Start the bot to test YouTube API. The bot logs will show API status or guide you through OAuth if needed.")

    def authorize_youtube_api(self):
        if not current_config.get("UTA_YOUTUBE_API_ENABLED"):
            messagebox.showinfo("API Disabled","YouTube API is not enabled in the configuration.")
            return

        token_file = current_config.get("UTA_YOUTUBE_TOKEN_FILE","youtube_token.json")
        if os.path.exists(token_file):
            if messagebox.askyesno("Token File Exists",
                                   f"The YouTube token file ('{token_file}') already exists. "
                                   "Deleting it will force re-authorization when the bot next needs API access. Continue?"):
                try:
                    os.remove(token_file)
                    self.log_display_frame.add_log(f"Removed existing token file: '{token_file}'. Start the bot to re-authorize.\n")
                    messagebox.showinfo("Token File Removed",f"'{token_file}' has been removed. Start the bot and trigger a YouTube API action (like starting a restream in API mode) to re-authorize.")
                except Exception as e:
                    self.log_display_frame.add_log(f"Error removing token file '{token_file}': {e}\n")
                    messagebox.showerror("Error",f"Could not remove token file '{token_file}': {e}")
            else: # User chose not to delete
                return
        else:
            self.log_display_frame.add_log(f"No existing token file ('{token_file}') found. Start the bot to authorize.\n")
            messagebox.showinfo("Authorize via Bot",f"Token file ('{token_file}') not found. Start the bot and trigger a YouTube API action to begin authorization.")

        self.log_display_frame.add_log("The bot will attempt to initiate the OAuth flow when it first needs to use the YouTube API (e.g., when a restream starts if in API mode).\nFollow instructions in the bot's console/logs or browser.\n")


    def update_dynamic_buttons_state(self):
        """Updates the state of control buttons based on bot_active and config."""
        global bot_active # Use the global bot_active state

        is_restreamer_enabled = current_config.get("UTA_RESTREAMER_ENABLED", False)
        is_yt_api_mode = current_config.get("UTA_YOUTUBE_API_ENABLED", False)

        if bot_active:
            self.start_button.configure(state="disabled")
            self.stop_button.configure(state="normal")
            if is_restreamer_enabled:
                self.quick_restart_button.configure(state="normal")
                if is_yt_api_mode:
                    self.force_new_part_button.configure(state="normal")
                else:
                    self.force_new_part_button.configure(state="disabled")
            else:
                self.quick_restart_button.configure(state="disabled")
                self.force_new_part_button.configure(state="disabled")
        else: # Bot not active
            self.start_button.configure(state="normal")
            self.stop_button.configure(state="disabled")
            self.quick_restart_button.configure(state="disabled")
            self.force_new_part_button.configure(state="disabled")

        # Ensure dynamic info display is also updated based on bot_active and config
        video_id_to_pass = self.current_yt_video_id_var.get() if self.current_yt_video_id_var.get() != "N/A" else None
        part_num_to_pass = self.current_yt_part_num_var.get() if self.current_yt_part_num_var.get() != "N/A" else None
        self._update_youtube_info_display(video_id_to_pass, part_num_to_pass)
        self._update_detailed_restream_status_display(
            play_status=self.yt_playability_status_var.get(),
            fails_str=self.consecutive_failures_var.get(),
            cool_status=self.cooldown_status_var.get()
        )

    def setup_log_panel(self):
        log_outer_frame = CTkFrame(self.right_panel) # No fg_color
        log_outer_frame.pack(fill="both", expand=True, padx=0, pady=(5,0), side="bottom") # Take remaining space

        log_header = CTkFrame(log_outer_frame, fg_color="transparent")
        log_header.pack(fill="x", padx=10, pady=(5,2))
        CTkLabel(log_header, text="Bot Logs", font=self.heading_font).pack(side="left")
        CTkButton(log_header, text="Clear Logs", width=80, command=self.clear_gui_logs).pack(side="right")

        self.log_display_frame = ScrollableLogFrame(log_outer_frame) # Use the custom class
        self.log_display_frame.pack(fill="both", expand=True, padx=10, pady=(2,10))

    def browse_executable(self, string_var):
        filepath = filedialog.askopenfilename(
            title="Select Executable or Path",
            filetypes=(("All files", "*.*"),) # More generic for paths like 'streamlink'
        )
        if filepath:
            string_var.set(filepath)

    def save_gui_config(self):
        global current_config
        gui_logger.info("Save GUI Config button pressed.")

        new_config_from_gui = {}
        for key, var_obj in self.vars.items():
            try:
                val = var_obj.get()
                original_default_val = DEFAULT_CONFIG.get(key) # Get default to infer type
                original_type = type(original_default_val)
                # For nullable fields, check if their default in DEFAULT_CONFIG is None
                is_nullable_in_default = (key in DEFAULT_CONFIG and DEFAULT_CONFIG[key] is None)

                if isinstance(var_obj, ctk.BooleanVar):
                    new_config_from_gui[key] = bool(val)
                elif isinstance(var_obj, ctk.StringVar):
                    # If the string var is empty AND the field is nullable (None by default), save as None
                    if val == "" and is_nullable_in_default:
                        new_config_from_gui[key] = None
                    # Else, if original type was int, try to convert
                    elif original_type == int:
                        # If empty, and nullable, it's already None. If not nullable, use 0 or original default.
                        new_config_from_gui[key] = int(val) if val else (None if is_nullable_in_default else (0 if original_default_val is not int else original_default_val))
                    elif original_type == float:
                        new_config_from_gui[key] = float(val) if val else (None if is_nullable_in_default else (0.0 if original_default_val is not float else original_default_val))
                    else: # Default to string
                        new_config_from_gui[key] = str(val)
                else: # Should not happen if using _get_var correctly
                    new_config_from_gui[key] = val
            except ValueError as ve: # Handle conversion errors for int/float
                gui_logger.warning(f"Conversion error for config key '{key}' with value '{val}': {ve}. Defaulting or keeping as string.")
                new_config_from_gui[key] = None if val == "" and is_nullable_in_default else str(val) # Fallback: None if empty & nullable, else string
            except Exception as e:
                gui_logger.error(f"Error getting value for config key {key}: {e}")
                new_config_from_gui[key] = current_config.get(key) # Fallback to existing loaded value

        # Create a full config dictionary based on defaults, then existing, then GUI overrides
        updated_config_full = DEFAULT_CONFIG.copy()
        updated_config_full.update(current_config) # Apply currently loaded values over defaults
        updated_config_full.update(new_config_from_gui) # Apply GUI values over that

        current_config = updated_config_full # Update the global current_config

        if save_config_to_file(current_config):
            messagebox.showinfo("Configuration Saved", f"Settings have been saved to {CONFIG_FILE}")
            self.config_changed_by_user = False
            self.update_dynamic_buttons_state() # Reflect potential changes like UTA_RESTREAMER_ENABLED
        else:
            messagebox.showerror("Error", "Failed to save configuration.")


    def start_bot_process(self, is_restarting=False):
        global bot_process, bot_active
        if bot_active and not is_restarting:
            messagebox.showwarning("Bot Already Running", "The bot process is already active.")
            return

        if self.config_changed_by_user and not is_restarting: # Don't ask if it's an auto-restart
            if messagebox.askyesno("Unsaved Changes", "You have unsaved configuration changes. Save them before starting the bot?"):
                self.save_gui_config()
                if self.config_changed_by_user: # If save failed, don't start
                    gui_logger.error("Configuration save failed, aborting bot start.")
                    return
            else: # User chose not to save, reload from file to discard GUI changes
                load_config_from_file()
                self.populate_fields_from_config() # Repopulate GUI with file content
                self.config_changed_by_user = False

        log_action = "Restarting" if is_restarting else "Starting"
        gui_logger.info(f"{log_action} uta_bot/main.py...")
        self.log_display_frame.add_log(f"Attempting {log_action.lower()} uta_bot/main.py...\n")
        # Reset detailed status on new start/restart
        self._update_detailed_restream_status_display(play_status="N/A",
                                                    fails_str=f"0/{current_config.get('UTA_RESTREAM_MAX_CONSECUTIVE_FAILURES',3)}",
                                                    cool_status="Inactive")


        try:
            # Correct path to uta_bot/main.py relative to gui_uta.py
            bot_script_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "uta_bot", "main.py")
            if not os.path.exists(bot_script_path):
                self.log_display_frame.add_log(f"ERROR: Bot script not found at {bot_script_path}\n")
                messagebox.showerror("Script Not Found", f"The bot script (main.py) was not found at the expected location: {bot_script_path}")
                return

            # Use -u for unbuffered output, CREATE_NO_WINDOW on Windows
            creationflags = subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0
            bot_process = subprocess.Popen(
                [sys.executable, "-u", bot_script_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT, # Redirect stderr to stdout
                text=True, # Decode output as text
                creationflags=creationflags,
                encoding='utf-8', # Explicitly set encoding
                errors='replace' # Handle potential decoding errors
            )
            bot_active = True
            self.status_label.configure(text="Active", text_color=self.status_active_color)
            self.update_dynamic_buttons_state()
            # Start a thread to read the bot's output
            threading.Thread(target=self.read_bot_output, daemon=True).start()
        except Exception as e:
            gui_logger.error(f"Failed to start uta_bot/main.py: {e}", exc_info=True)
            self.log_display_frame.add_log(f"ERROR starting bot: {e}\n")
            messagebox.showerror("Bot Start Error", f"Failed to start the bot: {e}")
            bot_active = False # Ensure state is correct
            self.update_dynamic_buttons_state()

    def read_bot_output(self):
        global bot_process # Access the global bot_process
        if not bot_process or not bot_process.stdout:
            return
        try:
            # iter(file.readline, '') is a common way to read lines until EOF
            for line in iter(bot_process.stdout.readline, ''):
                if line and self.winfo_exists(): # Check if GUI window still exists
                    # Schedule the log update on the main Tkinter thread
                    self.after(0, self.add_log_entry, line)
        except Exception as e:
            # This might happen if the process is terminated abruptly
            gui_logger.error(f"Exception while reading bot output: {e}")
        finally:
            if bot_process: # Check if bot_process still exists
                 # Ensure stdout is closed if it was opened
                if bot_process.stdout and not bot_process.stdout.closed:
                    bot_process.stdout.close()
                return_code = bot_process.wait() # Wait for the process to terminate fully
                if self.winfo_exists():
                    self.after(0, self.handle_bot_exit, return_code)


    def handle_bot_exit(self, return_code):
        global bot_active, bot_process
        previous_active_state = bot_active # Store state before modification
        bot_active = False # Set bot_active to False first
        bot_process_was_not_none = bot_process is not None
        bot_process = None # Clear the process variable

        # Reset dynamic info display only if the bot was truly considered active and is now confirmed stopped
        if previous_active_state:
            self._update_youtube_info_display() # Clears YT info
            self._update_detailed_restream_status_display(play_status="N/A",
                                                          fails_str=f"0/{current_config.get('UTA_RESTREAM_MAX_CONSECUTIVE_FAILURES',3)}",
                                                          cool_status="Inactive")


        if self.is_quick_restarting:
            self.log_display_frame.add_log("Quick Restart: Bot process stopped, attempting to start again...\n")
            self.is_quick_restarting = False # Reset flag
            self.after(500, lambda: self.start_bot_process(is_restarting=True)) # Delay slightly before restart
        elif previous_active_state: # Only log exit if it was considered active
            ok_return_codes = [0, 1, None]
            if os.name != 'nt': ok_return_codes.append(-signal.SIGINT.value)

            if return_code in ok_return_codes or (os.name == 'nt' and return_code == 1 and bot_process_was_not_none) :
                self.status_label.configure(text="Inactive", text_color=self.status_inactive_color)
                self.log_display_frame.add_log(f"Bot process exited (Code: {return_code}).\n")
            else:
                self.status_label.configure(text="Error/Exited", text_color=self.status_error_color)
                self.log_display_frame.add_log(f"Bot process exited with error (Code: {return_code}). Check logs for details.\n")
            gui_logger.info(f"uta_bot/main.py exited with code: {return_code}")

        self.update_dynamic_buttons_state() # This should be called regardless to update buttons


    def stop_bot_process(self, called_by_gui_button=False):
        global bot_process, bot_active
        if not bot_active or not bot_process:
            if called_by_gui_button:
                gui_logger.info("Stop command: Bot is not currently running.")
            # Ensure state is correct even if called programmatically (e.g., during quick_restart)
            # This ensures that handle_bot_exit is called to update UI etc. if somehow bot_active was true but bot_process was None
            # or if stop is called multiple times.
            if bot_active: # If it was marked active but process is None, handle this discrepancy
                 self.handle_bot_exit(bot_process.returncode if bot_process and hasattr(bot_process, 'returncode') else -999) # -999 indicates abnormal situation
            return False # Indicate bot was not running or already stopped

        gui_logger.info("Attempting to stop uta_bot/main.py...")
        self.log_display_frame.add_log("Attempting to stop the bot process...\n")
        self.status_label.configure(text="Stopping...", text_color=self.status_stopping_color)
        # Disable stop/restart buttons immediately to prevent multiple clicks
        self.stop_button.configure(state="disabled")
        self.quick_restart_button.configure(state="disabled")
        self.force_new_part_button.configure(state="disabled")


        proc_to_signal = bot_process # Work with a local copy

        try:
            if os.name == 'nt':
                proc_to_signal.terminate()
            else:
                proc_to_signal.send_signal(signal.SIGINT)

            def check_and_kill_if_needed(process_to_check, timeout=10):
                try:
                    process_to_check.wait(timeout=timeout)
                except subprocess.TimeoutExpired:
                    if process_to_check.poll() is None:
                        gui_logger.warning(f"Bot process did not respond to terminate/SIGINT within {timeout}s. Forcing kill.")
                        if self.winfo_exists():
                            self.after(0, self.log_display_frame.add_log, f"Bot did not stop gracefully after {timeout}s, forcing kill...\n")
                        try:
                            process_to_check.kill()
                            process_to_check.wait(5)
                        except Exception as kill_err:
                            gui_logger.error(f"Error during force kill: {kill_err}")
                # Ensure handle_bot_exit is called after wait/kill logic, on the main thread
                # This was previously in read_bot_output, but stop_bot_process needs to ensure it too
                # if self.winfo_exists():
                #    self.after(0, self.handle_bot_exit, process_to_check.returncode if process_to_check else -1)
                # handle_bot_exit is now more robustly called from read_bot_output's finally block

            threading.Thread(target=check_and_kill_if_needed, args=(proc_to_signal,)).start()
            self.log_display_frame.add_log("Bot stop signal sent. Waiting for process to exit...\n")
            return True # Signal sent

        except Exception as e:
            gui_logger.error(f"Error sending stop signal to bot: {e}", exc_info=True)
            if self.winfo_exists():
                self.log_display_frame.add_log(f"ERROR sending stop signal: {e}\n")

            if proc_to_signal and proc_to_signal.poll() is None:
                try:
                    proc_to_signal.kill()
                    proc_to_signal.wait(2)
                except Exception as final_kill_error:
                    gui_logger.error(f"Error during emergency kill in stop_bot_process: {final_kill_error}")
            # Directly call handle_bot_exit as the normal exit path via read_bot_output might not occur
            if self.winfo_exists():
                self.after(0, self.handle_bot_exit, -1) # Assume error exit
            return False

    def stop_bot_process_command(self):
        self.is_quick_restarting = False # Ensure this is reset if stop is clicked manually
        self.stop_bot_process(called_by_gui_button=True)

    def quick_restart_bot(self):
        global bot_active
        if not bot_active:
            messagebox.showinfo("Bot Not Running", "Cannot restart, the bot is not currently running.")
            return
        if self.is_quick_restarting: # Prevent multiple restart attempts
            return

        gui_logger.info("Initiating Quick Bot Restart.")
        self.log_display_frame.add_log("Quick Restart: Stopping current bot instance...\n")
        self.is_quick_restarting = True # Set flag
        if not self.stop_bot_process(called_by_gui_button=True): # If stop fails immediately or bot wasn't actually running
            self.is_quick_restarting = False # Reset flag as stop failed
            self.log_display_frame.add_log("Quick Restart: Failed to stop the bot (or it was already stopped). Restart aborted.\n")
            self.update_dynamic_buttons_state() # Re-enable buttons if stop failed or bot wasn't running

    def force_new_yt_part(self):
        if not current_config.get("UTA_YOUTUBE_API_ENABLED", False):
            messagebox.showinfo("API Mode Disabled","This feature requires YouTube API Mode to be enabled in the configuration.")
            return
        global bot_active
        if not bot_active:
            messagebox.showinfo("Bot Not Running","Cannot force new YT part, the bot is not currently running.")
            return
        if self.is_quick_restarting:
            return

        gui_logger.info("Forcing New YouTube Part (via bot restart).")
        self.log_display_frame.add_log("Forcing New YouTube Part: This will restart the bot...\n")
        self.is_quick_restarting = True # Use the same quick restart mechanism
        if not self.stop_bot_process(called_by_gui_button=True):
            self.is_quick_restarting = False
            self.log_display_frame.add_log("Force New YT Part: Failed to stop the bot (or it was already stopped). Action aborted.\n")
            self.update_dynamic_buttons_state()


    def clear_gui_logs(self):
        self.log_display_frame.clear_logs()

    def on_close(self):
        global bot_active
        if bot_active:
            if messagebox.askyesno("Confirm Quit", "The bot process is currently active. Are you sure you want to quit and stop the bot?"):
                self.is_quick_restarting = False # Not a restart
                if self.stop_bot_process(called_by_gui_button=True):
                    # If stop_bot_process initiated a stop, wait for read_bot_output to handle exit and then destroy.
                    # The timeout here is a fallback.
                    self.after(3000, self.destroy_app) # Wait up to 3s
                else: # Bot wasn't running or stop signal failed immediately
                    self.destroy_app()
            else:
                return # Don't close
        elif self.config_changed_by_user:
            if messagebox.askyesno("Unsaved Changes", "You have unsaved configuration changes. Quit anyway?"):
                self.destroy_app()
            else:
                return # Don't close
        else:
            self.destroy_app()

    def destroy_app(self):
        global bot_process
        # Ensure bot process is truly gone if GUI is destroyed
        if bot_process and bot_process.poll() is None: # Check if process exists and is running
            gui_logger.info("GUI is being destroyed, ensuring bot process is terminated.")
            try:
                bot_process.kill() # Force kill
                bot_process.wait(1) # Short wait for kill to register
            except Exception as e:
                gui_logger.error(f"Error during final bot process kill on GUI destroy: {e}")
        self.destroy() # Close the Tkinter window

if __name__ == "__main__":
    # Ensure a config file exists before starting the GUI
    if not os.path.exists(CONFIG_FILE):
        temp_root = ctk.CTk()
        temp_root.withdraw()
        if messagebox.askyesno("Config File Not Found",
                               f"The configuration file ({CONFIG_FILE}) was not found.\n"
                               "Would you like to create one with default values?"):
            save_config_to_file(DEFAULT_CONFIG)
            current_config = DEFAULT_CONFIG.copy()
        else:
            gui_logger.error(f"{CONFIG_FILE} not found and user chose not to create. GUI cannot start.")
            sys.exit(1)
        temp_root.destroy()

    app = UtaBotGui()
    try:
        app.mainloop()
    except KeyboardInterrupt:
        gui_logger.info("GUI KeyboardInterrupt received. Closing application.")
        app.on_close()
    except Exception as e:
        gui_logger.critical(f"Unhandled exception in GUI mainloop: {e}", exc_info=True)
    finally:
        if bot_process and bot_process.poll() is None:
            gui_logger.warning("GUI mainloop exited, but bot process might still be running. Attempting final kill.")
            try:
                bot_process.kill()
                bot_process.wait(2)
            except Exception as final_kill_e:
                gui_logger.error(f"Error during final kill after mainloop exit: {final_kill_e}")