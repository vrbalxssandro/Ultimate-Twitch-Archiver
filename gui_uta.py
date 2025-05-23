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
    "UTA_YOUTUBE_PLAYABILITY_CHECK_DELAY_SECONDS": 15, "UTA_FFMPEG_STARTUP_WAIT_SECONDS": 10
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
            if ctk.get_appearance_mode():
                 messagebox.showerror("Config Save Error", f"Could not save config: {e}")
        except Exception: pass
        return False

load_config_from_file()
ctk.set_appearance_mode("Dark")
ctk.set_default_color_theme("dark-blue")
bot_process = None
bot_active = False

class ScrollableLogFrame(CTkFrame):
    def __init__(self, master, **kwargs):
        super().__init__(master, **kwargs)
        self.log_textbox = CTkTextbox(self, wrap="word", activate_scrollbars=True, state="disabled", 
                                      font=("Consolas", 11), border_width=1, border_color=("gray70", "gray30"))
        self.log_textbox.pack(fill="both", expand=True, padx=5, pady=5)
        self.log_textbox.tag_config("red_error", foreground="#FF6B6B")
        self.log_textbox.tag_config("warning", foreground="#FFAE42")

    def add_log(self, message):
        if not self.winfo_exists(): return
        self.log_textbox.configure(state="normal")
        start_index = self.log_textbox.index("end-1c")
        self.log_textbox.insert("end", message)
        end_index = self.log_textbox.index("end-1c")
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
        self.vars = {}
        self.is_quick_restarting = False
        self.heading_font = CTkFont(size=15, weight="bold")
        self.sub_heading_font = CTkFont(size=13, weight="bold")
        self.label_font = CTkFont(size=12)
        self.small_label_font = CTkFont(size=10)
        self.status_active_color = "#4CAF50" 
        self.status_inactive_color = "gray60"
        self.status_error_color = "#F44336"  
        self.status_stopping_color = "#FF9800" 
        self.link_color = "#60A5FA" 
        self.current_yt_video_id_var = ctk.StringVar(value="N/A")
        self.current_yt_part_num_var = ctk.StringVar(value="N/A")
        self.current_yt_watch_link = None
        self.yt_playability_status_var = ctk.StringVar(value="N/A")
        self.consecutive_failures_var = ctk.StringVar(value="0/0")
        self.cooldown_status_var = ctk.StringVar(value="Inactive")
        self.setup_ui()
        self.populate_fields_from_config()
        self.setup_config_tracers()
        self.update_dynamic_buttons_state()

    def _get_var(self, key, val_from_cfg):
        if key not in self.vars:
            if isinstance(val_from_cfg, bool): self.vars[key] = ctk.BooleanVar(value=val_from_cfg)
            else: self.vars[key] = ctk.StringVar(value=str(val_from_cfg) if val_from_cfg is not None else "")
        return self.vars[key]

    def populate_fields_from_config(self):
        for key, value in current_config.items():
            var = self._get_var(key, value) 
            try:
                if isinstance(var, ctk.StringVar) and value is None: var.set("")
                elif isinstance(var, ctk.BooleanVar): var.set(bool(value))
                else: var.set(value if value is not None else "")
            except Exception as e: gui_logger.error(f"Error populating var {key} with {value}: {e}")

    def setup_config_tracers(self):
        for key, var_obj in self.vars.items(): var_obj.trace_add("write", lambda n,i,m,k=key: self.on_config_entry_change(k))
    def on_config_entry_change(self, key): gui_logger.debug(f"Config changed: {key}"); self.config_changed_by_user = True

    def setup_ui(self):
        self.main_container=CTkFrame(self, fg_color="transparent"); self.main_container.pack(fill="both",expand=True,padx=15,pady=15)
        self.left_panel=CTkFrame(self.main_container); self.left_panel.pack(side="left",fill="both",expand=True,padx=(0,10),pady=0)
        self.right_panel=CTkFrame(self.main_container, width=380); self.right_panel.pack(side="right",fill="y",expand=False,padx=(10,0),pady=0); self.right_panel.pack_propagate(False)
        self.setup_tabs(); self.setup_control_panel(); self.setup_log_panel()

    def _create_entry_field(self, parent, label_text, config_key, is_password=False, is_nullable_int=False):
        frame = CTkFrame(parent, fg_color="transparent")
        frame.pack(fill="x", padx=10, pady=4)
        CTkLabel(frame, text=label_text, width=230, anchor="w", font=self.label_font).pack(side="left", padx=(0,10))
        initial_val = current_config.get(config_key)
        var_val_str = str(initial_val) if initial_val is not None else ""
        var = self._get_var(config_key, var_val_str)
        entry = CTkEntry(frame, textvariable=var, show="*" if is_password else None, width=300)
        entry.pack(side="left", fill="x", expand=True)

    def _create_switch_field(self, parent, label_text, config_key):
        frame = CTkFrame(parent, fg_color="transparent")
        frame.pack(fill="x", padx=10, pady=4)
        var = self._get_var(config_key, current_config.get(config_key, False))
        CTkLabel(frame, text=label_text, anchor="w", font=self.label_font).pack(side="left", padx=(0,10))
        switch = CTkSwitch(frame, text="", variable=var, onvalue=True, offvalue=False)
        switch.pack(side="right")

    def _create_browse_field(self, parent, label_text, config_key):
        frame = CTkFrame(parent, fg_color="transparent")
        frame.pack(fill="x", padx=10, pady=4)
        CTkLabel(frame, text=label_text, width=230, anchor="w", font=self.label_font).pack(side="left", padx=(0,10))
        var = self._get_var(config_key, current_config.get(config_key, ""))
        entry = CTkEntry(frame, textvariable=var, width=250)
        entry.pack(side="left", fill="x", expand=True, padx=(0,5))
        CTkButton(frame, text="Browse", width=70, command=lambda vr=var: self.browse_executable(vr)).pack(side="left")

    def setup_tabs(self):
        self.tabview = ctk.CTkTabview(self.left_panel, border_width=1, border_color=("gray70", "gray30"))
        self.tabview.pack(fill="both", expand=True, padx=0, pady=0)
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
                ("UTA_TWITCH_CHANNEL_NAME", "Target Twitch Channel (UTA):"),
                ("UTA_STREAM_DURATION_LOG_FILE", "Stream Duration Log File:")
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
                ("UTA_POST_RESTREAM_COOLDOWN_SECONDS", "Post-Restream Cooldown (s):")
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
            tab = self.tabview.add(tab_name); scroll_frame = CTkScrollableFrame(tab, fg_color="transparent"); scroll_frame.pack(fill="both", expand=True, padx=0, pady=0)
            for item in fields:
                key, lbl_txt = item[0], item[1]; opts = item[2] if len(item) > 2 else {}
                if opts.get("is_switch"): self._create_switch_field(scroll_frame, lbl_txt, key)
                elif opts.get("is_browse"): self._create_browse_field(scroll_frame, lbl_txt, key)
                elif "options" in opts:
                    f=CTkFrame(scroll_frame, fg_color="transparent");f.pack(fill="x",padx=10,pady=4) # Use consistent padding & fg_color
                    CTkLabel(f,text=lbl_txt,width=230,anchor="w", font=self.label_font).pack(side="left",padx=(0,10))
                    var=self._get_var(key,current_config.get(key,opts["options"][0]));CTkOptionMenu(f,variable=var,values=opts["options"], width=300).pack(side="left",fill="x",expand=True)
                else: self._create_entry_field(scroll_frame, lbl_txt, key, is_password=opts.get("is_password",False), is_nullable_int=opts.get("is_nullable_int",False))
        self.tabview.set("General")

    def setup_control_panel(self):
        control_outer_frame = CTkFrame(self.right_panel) 
        control_outer_frame.pack(fill="x", padx=0, pady=0, side="top")

        status_section = CTkFrame(control_outer_frame, corner_radius=6)
        status_section.pack(fill="x", padx=10, pady=(10,5))
        CTkLabel(status_section, text="Bot Status:", font=self.heading_font).pack(side="left", padx=(10,5), pady=10)
        self.status_label = CTkLabel(status_section, text="Inactive", text_color=self.status_inactive_color, font=self.label_font)
        self.status_label.pack(side="left", padx=5, pady=10)

        self.dynamic_info_container = CTkFrame(control_outer_frame, fg_color="transparent")
        self.dynamic_info_container.pack(fill="x", padx=0, pady=0)

        self.yt_api_info_frame=CTkFrame(self.dynamic_info_container, fg_color="transparent") 
        yt_api_info_content_frame = CTkFrame(self.yt_api_info_frame, corner_radius=6) 
        yt_api_info_content_frame.pack(fill="x", expand=True, padx=10, pady=(0,5)) 
        yt_ir1=CTkFrame(yt_api_info_content_frame, fg_color="transparent"); yt_ir1.pack(fill="x",pady=(5,2), padx=5)
        CTkLabel(yt_ir1,text="YT Video ID:",font=self.small_label_font, anchor="w").pack(side="left",padx=(0,2))
        self.yt_video_id_label=CTkLabel(yt_ir1,textvariable=self.current_yt_video_id_var,font=self.small_label_font, anchor="w", wraplength=150); self.yt_video_id_label.pack(side="left",padx=2)
        yt_ir2=CTkFrame(yt_api_info_content_frame, fg_color="transparent"); yt_ir2.pack(fill="x",pady=(0,5), padx=5)
        CTkLabel(yt_ir2,text="Part:",font=self.small_label_font, anchor="w").pack(side="left",padx=(0,2))
        self.yt_part_num_label=CTkLabel(yt_ir2,textvariable=self.current_yt_part_num_var,font=self.small_label_font, anchor="w"); self.yt_part_num_label.pack(side="left",padx=2)
        self.yt_watch_link_label=CTkLabel(yt_ir2,text="",text_color=self.link_color,cursor="hand2",font=CTkFont(size=10,underline=True), anchor="w")

        self.detailed_restream_status_frame = CTkFrame(self.dynamic_info_container, fg_color="transparent")
        detailed_restream_content_frame = CTkFrame(self.detailed_restream_status_frame, corner_radius=6)
        detailed_restream_content_frame.pack(fill="x", expand=True, padx=10, pady=5)
        detail_row1 = CTkFrame(detailed_restream_content_frame, fg_color="transparent"); detail_row1.pack(fill="x", pady=2, padx=5)
        CTkLabel(detail_row1, text="Playability:", font=self.small_label_font, anchor="w").pack(side="left", padx=(0,2)); self.playability_label = CTkLabel(detail_row1, textvariable=self.yt_playability_status_var, font=self.small_label_font, anchor="w", wraplength=150); self.playability_label.pack(side="left", padx=2)
        detail_row2 = CTkFrame(detailed_restream_content_frame, fg_color="transparent"); detail_row2.pack(fill="x", pady=2, padx=5)
        CTkLabel(detail_row2, text="Pipe Fails:", font=self.small_label_font, anchor="w").pack(side="left", padx=(0,2)); self.failures_label = CTkLabel(detail_row2, textvariable=self.consecutive_failures_var, font=self.small_label_font, anchor="w"); self.failures_label.pack(side="left", padx=2)
        detail_row3 = CTkFrame(detailed_restream_content_frame, fg_color="transparent"); detail_row3.pack(fill="x", pady=2, padx=5)
        CTkLabel(detail_row3, text="Cooldown:", font=self.small_label_font, anchor="w").pack(side="left", padx=(0,2)); self.cooldown_label = CTkLabel(detail_row3, textvariable=self.cooldown_status_var, font=self.small_label_font, anchor="w", wraplength=150); self.cooldown_label.pack(side="left", padx=2)

        buttons_section = CTkFrame(control_outer_frame)
        buttons_section.pack(fill="x", padx=10, pady=(10,5))
        bfr1=CTkFrame(buttons_section, fg_color="transparent");bfr1.pack(fill="x",pady=3)
        self.start_button=CTkButton(bfr1,text="Start Bot",command=self.start_bot_process,fg_color=self.status_active_color, hover_color="#3E8E41");self.start_button.pack(side="left",padx=5,pady=5,fill="x",expand=True)
        self.stop_button=CTkButton(bfr1,text="Stop Bot",command=self.stop_bot_process_command,fg_color=self.status_error_color, hover_color="#C9302C",state="disabled");self.stop_button.pack(side="left",padx=5,pady=5,fill="x",expand=True)
        bfr2=CTkFrame(buttons_section, fg_color="transparent");bfr2.pack(fill="x",pady=3)
        self.quick_restart_button=CTkButton(bfr2,text="Quick Restart Bot",command=self.quick_restart_bot,state="disabled");self.quick_restart_button.pack(side="left",padx=5,pady=5,fill="x",expand=True)
        self.force_new_part_button=CTkButton(bfr2,text="Force New YT Part",command=self.force_new_yt_part,state="disabled");self.force_new_part_button.pack(side="left",padx=5,pady=5,fill="x",expand=True)
        bfr3=CTkFrame(buttons_section, fg_color="transparent");bfr3.pack(fill="x",pady=3)
        self.test_yt_api_button=CTkButton(bfr3,text="Test YT API",command=self.test_youtube_api_connection);self.test_yt_api_button.pack(side="left",padx=5,pady=5,fill="x",expand=True)
        self.auth_yt_api_button=CTkButton(bfr3,text="Re-Auth YT API",command=self.authorize_youtube_api);self.auth_yt_api_button.pack(side="left",padx=5,pady=5,fill="x",expand=True)
        self.save_cfg_button=CTkButton(buttons_section,text="Save Configuration",command=self.save_gui_config);self.save_cfg_button.pack(fill="x",padx=5,pady=(10,5))

    def _update_youtube_info_display(self, video_id=None, part_num=None):
        self.dynamic_info_container.update_idletasks() # Update parent before packing children

        if bot_active and current_config.get("UTA_RESTREAMER_ENABLED") and current_config.get("UTA_YOUTUBE_API_ENABLED") and video_id and part_num:
            self.current_yt_video_id_var.set(video_id); self.current_yt_part_num_var.set(str(part_num))
            self.current_yt_watch_link=f"https://www.youtube.com/watch?v={video_id}"
            self.yt_watch_link_label.configure(text="[Watch on YouTube]");self.yt_watch_link_label.bind("<Button-1>",lambda e:self.open_youtube_link())
            if not self.yt_watch_link_label.winfo_ismapped(): self.yt_watch_link_label.pack(side="left",padx=(10,2), pady=0)
            
            if not self.yt_api_info_frame.winfo_ismapped():
                # Pack yt_api_info_frame into dynamic_info_container
                self.yt_api_info_frame.pack(fill="x", padx=0, pady=0, before=self.detailed_restream_status_frame)
        else:
            self.current_yt_video_id_var.set("N/A"); self.current_yt_part_num_var.set("N/A"); self.current_yt_watch_link=None
            if self.yt_watch_link_label.winfo_ismapped(): self.yt_watch_link_label.pack_forget()
            if self.yt_api_info_frame.winfo_ismapped(): self.yt_api_info_frame.pack_forget()
        # Call this here to ensure detailed status is also updated/re-packed correctly relative to yt_api_info_frame
        self._update_detailed_restream_status_display()


    def _update_detailed_restream_status_display(self, play_status=None, fails_str=None, cool_status=None):
        self.dynamic_info_container.update_idletasks()

        if play_status is not None: self.yt_playability_status_var.set(play_status)
        if fails_str is not None: self.consecutive_failures_var.set(fails_str)
        if cool_status is not None: self.cooldown_status_var.set(cool_status)
        
        should_show_details = bot_active and current_config.get("UTA_RESTREAMER_ENABLED", False)
        
        if should_show_details:
            if not self.detailed_restream_status_frame.winfo_ismapped():
                 self.detailed_restream_status_frame.pack(fill="x", padx=0, pady=(0,5)) # Pack into dynamic_info_container
        else:
            if self.detailed_restream_status_frame.winfo_ismapped():
                self.detailed_restream_status_frame.pack_forget()
    
    def open_youtube_link(self):
        if self.current_yt_watch_link: webbrowser.open_new_tab(self.current_yt_watch_link); self.log_display_frame.add_log(f"Opened YT link: {self.current_yt_watch_link}\n")

    def add_log_entry(self, message):
        if not hasattr(self,'log_display_frame') or not self.log_display_frame.winfo_exists(): return
        self.log_display_frame.add_log(message)
        yt_match = re.search(r"UTA YouTube: .* broadcast [a-zA-Z0-9_-]+ \(Video ID: ([a-zA-Z0-9_-]+)\) for Part (\d+)\.", message)
        if yt_match: self.after(0,self._update_youtube_info_display,yt_match.group(1),yt_match.group(2))
        playability_match = re.search(r"UTA_GUI_LOG: PlayabilityCheckStatus=([a-zA-Z0-9\s/()_-]+)", message)
        if playability_match: self.after(0, self._update_detailed_restream_status_display, play_status=playability_match.group(1).strip())
        failures_match = re.search(r"UTA_GUI_LOG: ConsecutiveFailures=(\d+)", message)
        if failures_match: self.after(0, self._update_detailed_restream_status_display, fails_str=f"{failures_match.group(1)}/{current_config.get('UTA_RESTREAM_MAX_CONSECUTIVE_FAILURES',3)}")
        cooldown_match = re.search(r"UTA_GUI_LOG: CooldownStatus=([a-zA-Z0-9_]+)", message)
        if cooldown_match: self.after(0, self._update_detailed_restream_status_display, cool_status=cooldown_match.group(1).strip())
        if any(x in message for x in ["Bot process exited", "UTA Restream: Target Twitch Channel not set"]):
             self.after(0, self._update_youtube_info_display)
             self.after(0, self._update_detailed_restream_status_display, play_status="N/A", fails_str=f"0/{current_config.get('UTA_RESTREAM_MAX_CONSECUTIVE_FAILURES',3)}", cool_status="Inactive")

    def test_youtube_api_connection(self):
        if not current_config.get("UTA_YOUTUBE_API_ENABLED"): messagebox.showinfo("API Disabled","YT API not enabled in config."); return
        if not os.path.exists(current_config.get("UTA_YOUTUBE_CLIENT_SECRET_FILE","client_secret.json")): messagebox.showerror("Client Secret Missing",f"YT client secret file ('{current_config.get('UTA_YOUTUBE_CLIENT_SECRET_FILE')}') not found."); return
        self.log_display_frame.add_log("Test YT API: Start bot. It logs API init success or guides OAuth if token invalid/missing.\n"); messagebox.showinfo("Test Initiated","Start bot to test. Bot logs API status or guides OAuth.")
    def authorize_youtube_api(self):
        if not current_config.get("UTA_YOUTUBE_API_ENABLED"): messagebox.showinfo("API Disabled","YT API not enabled."); return
        token_f = current_config.get("UTA_YOUTUBE_TOKEN_FILE","youtube_token.json")
        if os.path.exists(token_f):
            if messagebox.askyesno("Token Exists",f"'{token_f}' already exists. Deleting it will force re-authorization. Continue?"):
                try: os.remove(token_f); self.log_display_frame.add_log(f"Removed existing '{token_f}'. Start the bot to re-authorize.\n"); messagebox.showinfo("Token Removed",f"'{token_f}' removed. Start bot for OAuth.")
                except Exception as e: self.log_display_frame.add_log(f"Err removing token: {e}\n"); messagebox.showerror("Error",f"Could not remove '{token_f}': {e}")
            else: return
        else: self.log_display_frame.add_log(f"No existing '{token_f}' found. Start the bot to authorize.\n"); messagebox.showinfo("Authorize via Bot",f"'{token_f}' not found. Start bot for OAuth.")
        self.log_display_frame.add_log("The bot will initiate OAuth when it first needs to use the YouTube API (e.g., when a restream starts in API mode).\n")

    def update_dynamic_buttons_state(self):
        global bot_active
        is_restreamer_enabled = current_config.get("UTA_RESTREAMER_ENABLED", False)
        is_yt_api_mode = current_config.get("UTA_YOUTUBE_API_ENABLED", False)
        if bot_active:
            self.start_button.configure(state="disabled"); self.stop_button.configure(state="normal")
            if is_restreamer_enabled:
                self.quick_restart_button.configure(state="normal")
                if is_yt_api_mode: self.force_new_part_button.configure(state="normal")
                else: self.force_new_part_button.configure(state="disabled")
            else: self.quick_restart_button.configure(state="disabled"); self.force_new_part_button.configure(state="disabled")
        else:
            self.start_button.configure(state="normal"); self.stop_button.configure(state="disabled")
            self.quick_restart_button.configure(state="disabled"); self.force_new_part_button.configure(state="disabled")
        self._update_youtube_info_display(self.current_yt_video_id_var.get() if self.current_yt_video_id_var.get() != "N/A" else None, self.current_yt_part_num_var.get() if self.current_yt_part_num_var.get() != "N/A" else None)
        self._update_detailed_restream_status_display(play_status=self.yt_playability_status_var.get(), fails_str=self.consecutive_failures_var.get(), cool_status=self.cooldown_status_var.get())

    def setup_log_panel(self):
        log_outer_frame = CTkFrame(self.right_panel)
        log_outer_frame.pack(fill="both", expand=True, padx=0, pady=(5,0), side="bottom")
        log_header = CTkFrame(log_outer_frame, fg_color="transparent")
        log_header.pack(fill="x", padx=10, pady=(5,2))
        CTkLabel(log_header, text="Bot Logs", font=self.heading_font).pack(side="left")
        CTkButton(log_header, text="Clear Logs", width=80, command=self.clear_gui_logs).pack(side="right")
        self.log_display_frame = ScrollableLogFrame(log_outer_frame)
        self.log_display_frame.pack(fill="both", expand=True, padx=10, pady=(2,10))

    def browse_executable(self, string_var):
        filepath = filedialog.askopenfilename(title="Select Executable/Path", filetypes=(("All files", "*.*"),))
        if filepath: string_var.set(filepath)

    def save_gui_config(self):
        global current_config; gui_logger.info("Save GUI Config pressed."); new_cfg = {}
        for key, var_obj in self.vars.items():
            try:
                val=var_obj.get(); orig_default_val=DEFAULT_CONFIG.get(key); orig_type=type(orig_default_val); is_null_in_default=(orig_default_val is None)
                if isinstance(var_obj,ctk.BooleanVar): new_cfg[key]=bool(val)
                elif isinstance(var_obj,ctk.StringVar):
                    if val=="" and is_null_in_default: new_cfg[key]=None
                    elif orig_type==int: new_cfg[key]=int(val) if val else (None if is_null_in_default else 0)
                    elif orig_type==float: new_cfg[key]=float(val) if val else (None if is_null_in_default else 0.0)
                    else: new_cfg[key]=val
                else: new_cfg[key]=val
            except ValueError as ve: gui_logger.warning(f"Convert err for {key} ('{val}'): {ve}"); new_cfg[key]=None if val=="" and is_null_in_default else str(val)
            except Exception as e: gui_logger.error(f"Err getting val for {key}: {e}"); new_cfg[key]=current_config.get(key)
        updated_cfg=DEFAULT_CONFIG.copy(); updated_cfg.update(current_config); updated_cfg.update(new_cfg); current_config=updated_cfg
        if save_config_to_file(current_config): messagebox.showinfo("Config Saved",f"Settings saved to {CONFIG_FILE}"); self.config_changed_by_user=False; self.update_dynamic_buttons_state()
        else: messagebox.showerror("Error","Failed to save config.")

    def start_bot_process(self, is_restarting=False):
        global bot_process, bot_active;
        if bot_active and not is_restarting: messagebox.showwarning("Bot Running","Bot already running."); return
        if self.config_changed_by_user and not is_restarting:
            if messagebox.askyesno("Save Config","Unsaved changes. Save before starting?"):
                self.save_gui_config()
                if self.config_changed_by_user: gui_logger.error("Config save failed, aborting start."); return
            else: load_config_from_file(); self.populate_fields_from_config(); self.config_changed_by_user=False
        log_act="Restarting" if is_restarting else "Starting"; gui_logger.info(f"{log_act} UTA.py..."); self.log_display_frame.add_log(f"Attempting {log_act.lower()} UTA.py...\n")
        self._update_detailed_restream_status_display(play_status="N/A", fails_str=f"0/{current_config.get('UTA_RESTREAM_MAX_CONSECUTIVE_FAILURES',3)}", cool_status="Inactive")
        try:
            scr_path=os.path.join(os.path.dirname(os.path.abspath(__file__)),"UTA.py")
            if not os.path.exists(scr_path): self.log_display_frame.add_log(f"ERR: UTA.py not found: {scr_path}\n"); messagebox.showerror("Error",f"UTA.py not found: {scr_path}"); return
            bot_process=subprocess.Popen([sys.executable,"-u",scr_path],stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,creationflags=subprocess.CREATE_NO_WINDOW if os.name=='nt' else 0, encoding='utf-8', errors='replace')
            bot_active=True; self.status_label.configure(text="Active",text_color=self.status_active_color); self.update_dynamic_buttons_state()
            threading.Thread(target=self.read_bot_output,daemon=True).start()
        except Exception as e: gui_logger.error(f"Failed to start UTA.py: {e}",exc_info=True); self.log_display_frame.add_log(f"ERR starting bot: {e}\n"); messagebox.showerror("Start Error",f"Failed to start: {e}"); bot_active=False; self.update_dynamic_buttons_state()

    def read_bot_output(self):
        global bot_process;
        if not bot_process or not bot_process.stdout: return
        try:
            for line in iter(bot_process.stdout.readline,''):
                if line and self.winfo_exists(): self.after(0,self.add_log_entry,line)
        except Exception as e: gui_logger.error(f"Err reading bot output: {e}")
        finally:
            if bot_process:
                if bot_process.stdout and not bot_process.stdout.closed: bot_process.stdout.close()
                ret_code=bot_process.wait()
                if self.winfo_exists(): self.after(0,self.handle_bot_exit,ret_code)

    def handle_bot_exit(self,return_code):
        global bot_active,bot_process; prev_active_state=bot_active; bot_active=False; bot_process=None;
        self._update_youtube_info_display()
        self._update_detailed_restream_status_display(play_status="N/A", fails_str=f"0/{current_config.get('UTA_RESTREAM_MAX_CONSECUTIVE_FAILURES',3)}", cool_status="Inactive")
        if self.is_quick_restarting: self.log_display_frame.add_log("Quick restart: Bot stopped, starting again...\n"); self.is_quick_restarting=False; self.after(500,lambda: self.start_bot_process(is_restarting=True))
        elif prev_active_state:
            rc_ok = [0,1,None] + ([-signal.SIGINT.value] if os.name!='nt' else [])
            if return_code in rc_ok or (os.name == 'nt' and return_code == 1):
                self.status_label.configure(text="Inactive",text_color=self.status_inactive_color); self.log_display_frame.add_log(f"Bot process exited (Code: {return_code}).\n")
            else: self.status_label.configure(text="Error/Exited",text_color=self.status_error_color); self.log_display_frame.add_log(f"Bot process exited with error (Code: {return_code}).\n")
            gui_logger.info(f"UTA.py exited (Code: {return_code})")
        self.update_dynamic_buttons_state()

    def stop_bot_process(self,called_by_gui=False):
        global bot_process,bot_active;
        if not bot_active or not bot_process:
            if called_by_gui: gui_logger.info("Stop cmd: Bot not running.")
            self.handle_bot_exit(bot_process.returncode if bot_process and hasattr(bot_process,'returncode') else -1); return False
        gui_logger.info("Stopping UTA.py..."); self.log_display_frame.add_log("Stopping UTA.py...\n"); self.status_label.configure(text="Stopping...",text_color=self.status_stopping_color); self.stop_button.configure(state="disabled"); self.quick_restart_button.configure(state="disabled"); self.force_new_part_button.configure(state="disabled")
        proc_to_stop=bot_process
        try:
            if os.name=='nt': proc_to_stop.terminate()
            else: proc_to_stop.send_signal(signal.SIGINT)
            
            def chk_kill(p,t=10):
                try: p.wait(t)
                except subprocess.TimeoutExpired:
                    if p.poll() is None: 
                        gui_logger.warning(f"Bot did not terminate/SIGINT gracefully after {t}s, killing.");
                        if self.winfo_exists(): self.log_display_frame.add_log(f"Bot did not stop gracefully after {t}s, forcing kill...\n")
                        try: p.kill(); p.wait(5)
                        except Exception as kill_e: gui_logger.error(f"Error during kill: {kill_e}")
            threading.Thread(target=chk_kill,args=(proc_to_stop,)).start(); 
            self.log_display_frame.add_log("Bot stop signal sent.\n")
            return True
        except Exception as e:
            gui_logger.error(f"Err stopping bot: {e}",exc_info=True);
            if self.winfo_exists(): self.log_display_frame.add_log(f"ERR stopping: {e}\n")
            if proc_to_stop and proc_to_stop.poll() is None:
                try:
                    proc_to_stop.kill()
                    proc_to_stop.wait(2)
                except Exception as final_kill_error:
                    gui_logger.error(f"Error during emergency kill in stop_bot_process: {final_kill_error}")
            self.handle_bot_exit(-1); return False

    def stop_bot_process_command(self): self.is_quick_restarting=False; self.stop_bot_process(called_by_gui=True)
    def quick_restart_bot(self):
        global bot_active;
        if not bot_active: messagebox.showinfo("Bot Not Running","Cannot restart, bot not running."); return
        if self.is_quick_restarting: return
        gui_logger.info("Quick Bot Restart."); self.log_display_frame.add_log("Quick Restart: Stopping...\n"); self.is_quick_restarting=True
        if not self.stop_bot_process(called_by_gui=True): self.is_quick_restarting=False; self.log_display_frame.add_log("Quick Restart: Fail to stop, aborted.\n"); self.update_dynamic_buttons_state()
    def force_new_yt_part(self):
        if not current_config.get("UTA_YOUTUBE_API_ENABLED",False): messagebox.showinfo("API Mode Disabled","Feature needs YT API Mode."); return
        global bot_active;
        if not bot_active: messagebox.showinfo("Bot Not Running","Cannot force new part, bot not running."); return
        if self.is_quick_restarting: return
        gui_logger.info("Force New YT Part (via bot restart)."); self.log_display_frame.add_log("Forcing New YT Part (restarts bot)...\n"); self.is_quick_restarting=True
        if not self.stop_bot_process(called_by_gui=True): self.is_quick_restarting=False; self.log_display_frame.add_log("Force New YT Part: Fail to stop, aborted.\n"); self.update_dynamic_buttons_state()
    def clear_gui_logs(self): self.log_display_frame.clear_logs()
    def on_close(self):
        global bot_active;
        if bot_active:
            if messagebox.askyesno("Quit","Bot running. Quit and stop bot?"): self.is_quick_restarting=False; self.stop_bot_process(called_by_gui=True); self.after(2000,self.destroy_app)
            else: return
        elif self.config_changed_by_user:
            if messagebox.askyesno("Unsaved Changes","Unsaved config changes. Quit anyway?"): self.destroy_app()
            else: return
        else: self.destroy_app()
    def destroy_app(self):
        global bot_process;
        if bot_process and bot_process.poll() is None:
            gui_logger.info("GUI destroying, kill bot process.");
            try: bot_process.kill();bot_process.wait(1)
            except Exception as e: gui_logger.error(f"Err final kill: {e}")
        self.destroy()

if __name__ == "__main__":
    if not os.path.exists(CONFIG_FILE):
        temp_root = ctk.CTk(); temp_root.withdraw()
        if messagebox.askyesno("Config Not Found",f"{CONFIG_FILE} not found. Create with defaults?"):
            save_config_to_file(DEFAULT_CONFIG); current_config=DEFAULT_CONFIG.copy()
        else: gui_logger.error(f"{CONFIG_FILE} not found, user chose no create. Exit."); sys.exit(1)
        temp_root.destroy()
    # load_config_from_file() is called globally at the start, no need to call it again here if it's already done.
    
    app = UtaBotGui()
    try: app.mainloop()
    except KeyboardInterrupt: gui_logger.info("GUI KbdInter. Close."); app.on_close()
    except Exception as e: gui_logger.critical(f"Unhandled GUI mainloop err: {e}",exc_info=True)
    finally:
        if bot_process and bot_process.poll() is None:
            gui_logger.warning("GUI mainloop exit unclean, kill bot.");
            try: bot_process.kill();bot_process.wait(2)
            except Exception as final_kill_e: gui_logger.error(f"Err final kill main: {final_kill_e}")