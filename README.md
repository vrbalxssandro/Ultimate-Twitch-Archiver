# UTA (Ultimate Twitch Archiver)

This project provides a comprehensive Twitch.tv automation tool with a CustomTkinter-based GUI control panel and a feature-rich Discord bot. It's designed to manage various aspects of a Twitch channel, including follower tracking, stream restreaming, clip notifications, chat monitoring, and data analytics.

## Overview

The system consists of two main parts:
1.  **GUI Control Panel (`gui_uta.py`)**: A user-friendly interface to configure all settings, start/stop the bot, view logs, and monitor real-time status updates (e.g., YouTube restream details).
2.  **Discord Bot (`uta_bot/`)**: The backend bot that performs all the automated tasks, interacts with Discord, Twitch API, and YouTube API, and logs data for analytics.

## Features

### 📊 GUI Control Panel
*   **Centralized Configuration**: Manage all bot settings through a tabbed interface, with changes saved to `config.json`.
*   **Bot Process Management**: Start, stop, and quick-restart the Discord bot process.
*   **Live Log Viewing**: Monitor bot activity and logs directly within the GUI.
*   **Dynamic Status Display**:
    *   Real-time YouTube Video ID and Part Number for restreaming.
    *   YouTube VOD playability status.
    *   Restreamer consecutive failures and cooldown status.
*   **Restreamer Controls**:
    *   Force a new YouTube VOD part (API mode).
    *   (Via bot restart) Restart FFmpeg/Streamlink pipe.
*   **YouTube API Management**: Test API connection and re-authorize OAuth credentials.
*   **File Browsing**: Easily locate executables like Streamlink and FFmpeg.

### 🤖 Core Discord Bot
*   **Modular Cog System**: Features are organized into extendable cogs.
*   **Command Handling**: Responds to commands in Discord (configurable command channel).
*   **Data Logging**: Persistently logs various metrics to binary files for historical analysis.

### 📈 Follower Counter (FCTD)
*   Tracks Twitch follower counts for a specified channel.
*   Automatically updates a Discord channel name with the current follower count.
*   Logs follower data to `follower_counts.bin`.
*   **Commands**:
    *   `!followers [period]`: Shows follower gain/loss.
    *   `!follrate [period]`: Calculates follower growth rate.
    *   `!daystats [YYYY-MM-DD]`: Shows follower stats for a specific day.

### ⚙️ Ultimate Twitch Archiver (UTA)
A suite of tools for advanced Twitch channel automation:

*   **🎬 Clip Monitor**:
    *   Monitors a Twitch channel for new clips.
    *   Posts new clips to a configured Discord webhook.

*   **📡 Restreamer (Twitch to YouTube)**:
    *   **Live Restreaming**: Captures Twitch live stream using Streamlink and pipes it to FFmpeg for restreaming to YouTube.
    *   **Modes**:
        *   **YouTube API Mode**: Dynamically creates YouTube live broadcasts, binds streams, and transitions states. Recommended for full features.
        *   **Legacy RTMP Mode**: Streams to a pre-configured YouTube RTMP endpoint and stream key.
    *   **Dynamic VODs**:
        *   Customizable YouTube VOD titles and descriptions using templates (e.g., including Twitch title, game, date, part number).
        *   Automatic VOD part rolling based on a configured schedule (YouTube API mode).
    *   **Auto Chapters**: Generates YouTube video chapters based on game changes during the stream (requires stream activity logging & YouTube API mode).
    *   **Playability Checks**: Verifies if the YouTube VOD is playable after starting the restream (YouTube API mode).
    *   **Reliability**: Handles consecutive failures and cooldowns.
    *   Logs stream VOD durations to `stream_durations.bin`.

*   **📢 Stream Status Notifications & Activity Logging**:
    *   Sends Discord notifications (via webhook or channel message) for:
        *   Stream going LIVE.
        *   Stream going OFFLINE (with a session summary).
        *   Game changes.
        *   Title changes.
        *   Tag changes.
    *   Logs detailed stream activity (start, end, game, title, tags, YouTube video ID if applicable) to `stream_activity.bin`.
    *   Optionally logs viewer counts at regular intervals to `viewer_counts.bin`.

*   **💬 Twitch Chat Monitor & Mirror**:
    *   Connects to the target Twitch channel's chat using TwitchIO.
    *   Logs chat activity (message count, unique chatters per interval) to `chat_activity.bin`.
    *   Mirrors Twitch chat messages to a specified Discord channel.
    *   **Command**: `!chatstats [period|live]`: Shows chat activity metrics.

### 📜 Informational & Analytics Commands
*   `!uptime`: Bot's current session uptime.
*   `!runtime [period]`: Bot's total logged runtime over a past period (from `bot_sessions.bin`).
*   `!twitchinfo [username]`: Displays public Twitch channel information.
*   `!streamtime [period]`: Shows total stream time logged for the UTA target channel.
*   `!gamestats "<Game Name>" [period]`: Provides statistics for a specific game played.

### 👑 Admin & Control Commands (Bot Owner Only)
*   `!reloadconfig`: Reloads `config.json` dynamically, restarting services if necessary.
*   `!readdata [log_type]`: Dumps raw data from specified binary log files.
*   `!utastatus`: Shows the current status of all UTA modules and related configurations.
*   `!utarestartffmpeg`: Manually requests the restreamer to restart the FFmpeg/Streamlink pipe.
*   `!utastartnewpart`: Manually requests the restreamer to start a new YouTube VOD part (API mode only).
*   `!utaytstatus`: Shows current YouTube restream status if in API mode.
*   `!deephealthcheck`: Performs a comprehensive diagnostic check of bot functions and configurations.
*   `!commands` (or `!help`): Lists available commands for the user.

### 📅 Milestones & Historical Data
*   **Milestones Cog (`!milestones [category|all]`)**:
    *   Tracks progress towards predefined channel goals (e.g., X followers, Y hours streamed, Z peak viewers).
    *   Displays completed and upcoming milestones.
*   **Time Capsule Cog (`!onthisday [YYYY-MM-DD]`)**:
    *   Shows a "snapshot" of channel activity (follower changes, stream time, games, viewers) for the current day in previous years, or a specified historical date.

### 📈 Plotting Commands (Requires Matplotlib)
*   `!plotfollowers [period|all]`: Generates a plot of follower count over time.
*   `!plotstreamdurations [period|all]`: Generates a histogram of stream/VOD part durations.
*   (Plotting capabilities are also integrated into `!gamestats` for viewer distribution histograms).

## Requirements

*   **Python**: 3.9 or higher.
*   **pip**: For installing Python packages.
*   **External Applications** (for Restreamer):
    *   **Streamlink**: Latest version recommended. ([Installation Guide](https://streamlink.github.io/install.html))
    *   **FFmpeg**: Recent version. ([Installation Guide](https://ffmpeg.org/download.html))
    *   *Ensure Streamlink and FFmpeg are either in your system's PATH or their paths are correctly specified in `config.json` via the GUI.*
*   **Python Libraries**:
    *   `discord.py`
    *   `requests`
    *   `customtkinter` (GUI handles its installation)
    *   `Pillow` (GUI handles its installation)
    *   `twitchio` (Optional, for Twitch Chat Monitor feature. Install with `pip install twitchio`)
    *   `matplotlib` (Optional, for plotting features. Install with `pip install matplotlib`)
    *   `google-api-python-client`, `google-auth-oauthlib`, `google-auth-httplib2` (Optional, for YouTube API mode in Restreamer. Install with `pip install google-api-python-client google-auth-oauthlib google-auth-httplib2`)
    *   `streamlink` (as a Python library, for YouTube VOD playability checks. Install with `pip install streamlink`)

    You can typically install most of these with:
    ```bash
    pip install discord.py requests twitchio matplotlib google-api-python-client google-auth-oauthlib google-auth-httplib2 streamlink
    ```
    The GUI will attempt to install `customtkinter` and `Pillow` if they are missing.

## Setup & Configuration

1.  **Clone the Repository**:
    ```bash
    git clone <repository_url>
    cd <repository_directory>
    ```

2.  **Install Dependencies**:
    *   Ensure Python and pip are installed.
    *   Install the required Python libraries listed above.
    *   Install Streamlink and FFmpeg if you plan to use the restreamer.

3.  **Initial Configuration (`config.json`)**:
    *   The primary way to configure the bot is by running the GUI (`python gui_uta.py`).
    *   If `config.json` does not exist when you first run `gui_uta.py`, it will be created with default placeholder values.
    *   **Open the GUI and fill in the necessary fields.** Key fields include:
        *   **General Tab**:
            *   `DISCORD_TOKEN`: Your Discord bot's token.
            *   `DISCORD_BOT_OWNER_ID`: Your Discord user ID for owner-only commands.
            *   `TWITCH_CLIENT_ID` & `TWITCH_CLIENT_SECRET`: Credentials for your Twitch Application (see below).
        *   **Follower Counter Tab**:
            *   `FCTD_TWITCH_USERNAME`: The Twitch username to track followers for.
            *   `FCTD_TARGET_CHANNEL_ID`: The Discord channel ID whose name will be updated with the follower count.
        *   **UTA General Tab**:
            *   `UTA_ENABLED`: Master switch for all UTA features.
            *   `UTA_TWITCH_CHANNEL_NAME`: The Twitch username for UTA features (clips, restreaming, status).
        *   **UTA Restreamer Tab**:
            *   `UTA_RESTREAMER_ENABLED`: Enable/disable the restreamer.
            *   **YouTube API Mode (Recommended)**:
                *   `UTA_YOUTUBE_API_ENABLED`: Set to `True`.
                *   `UTA_YOUTUBE_CLIENT_SECRET_FILE`: Path to your `client_secret.json` from Google Cloud (see below).
                *   `UTA_YOUTUBE_TOKEN_FILE`: Path where the OAuth token will be stored (e.g., `youtube_token.json`). The GUI/bot will guide you through authorization.
            *   **Legacy RTMP Mode**:
                *   `UTA_YOUTUBE_API_ENABLED`: Set to `False`.
                *   `UTA_YOUTUBE_RTMP_URL_BASE`: Your YouTube RTMP base URL.
                *   `UTA_YOUTUBE_STREAM_KEY`: Your YouTube stream key.
        *   **UTA Clip Monitor / Status Monitor / Chat Monitor Tabs**: Configure webhooks, channel IDs, and feature-specific settings as needed.
        *   **Paths Tab**:
            *   `UTA_STREAMLINK_PATH`: Path to Streamlink executable (e.g., `streamlink` or `/usr/local/bin/streamlink`).
            *   `UTA_FFMPEG_PATH`: Path to FFmpeg executable (e.g., `ffmpeg` or `/usr/local/bin/ffmpeg`).

4.  **Twitch Application Setup**:
    *   Go to the [Twitch Developer Console](https://dev.twitch.tv/console).
    *   Register a new application.
    *   Choose "Chat Bot" or "Server-to-Server App" depending on your primary use (for API access, "Server-to-Server" or an app with user auth if needed). For this bot, a general app type is fine.
    *   Set an OAuth Redirect URL (e.g., `http://localhost` - it's often not used directly by this bot's server-to-server auth but might be required by Twitch).
    *   You will get a **Client ID** and can generate a **Client Secret**. Use these in `config.json`.

5.  **YouTube API Setup (for Restreamer API Mode)**:
    *   Go to the [Google Cloud Console](https://console.cloud.google.com/).
    *   Create a new project or select an existing one.
    *   Enable the "YouTube Data API v3".
    *   Create OAuth 2.0 credentials for a "Desktop app".
    *   Download the credentials JSON file. Rename it to `client_secret.json` (or as specified in `UTA_YOUTUBE_CLIENT_SECRET_FILE`) and place it in the bot's root directory.
    *   When you first start the bot with YouTube API mode enabled (or use the "Re-Auth YT API" button in the GUI), you'll be prompted to authorize the application via your web browser. The resulting token will be saved to the file specified by `UTA_YOUTUBE_TOKEN_FILE`.

6.  **Data Files**: The bot will create `.bin` files (e.g., `follower_counts.bin`, `stream_activity.bin`) in its working directory to store historical data. These are binary files and not human-readable directly but are used by the bot for analytics commands.

## Running the Bot

The primary way to run and manage the bot is through the GUI:

```bash
python gui_uta.py
```
