import logging
import os
import asyncio
import time
import random

from uta_bot import config_manager

logger = logging.getLogger(__name__)

def get_youtube_service(force_reinitialize=False):
    if not config_manager.GOOGLE_API_AVAILABLE:
        logger.error("UTA YouTube: Google API client libraries not available. YouTube API features disabled.")
        config_manager.uta_yt_service = None
        return None

    if config_manager.uta_yt_service and not force_reinitialize:
        return config_manager.uta_yt_service

    creds = None
    if os.path.exists(config_manager.UTA_YOUTUBE_TOKEN_FILE) and not force_reinitialize:
        try:
            creds = config_manager.GoogleCredentials.from_authorized_user_file(
                config_manager.UTA_YOUTUBE_TOKEN_FILE, config_manager.UTA_YOUTUBE_API_SCOPES
            )
        except Exception as e:
            logger.error(f"UTA YouTube: Error loading token file '{config_manager.UTA_YOUTUBE_TOKEN_FILE}': {e}. Will attempt re-auth.")
            creds = None

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            logger.info("UTA YouTube: Refreshing expired YouTube API token.")
            try:
                creds.refresh(config_manager.GoogleAuthRequest())
            except Exception as e:
                logger.error(f"UTA YouTube: Failed to refresh YouTube API token: {e}. User interaction may be required.")
                creds = None

        if not creds:
            if not os.path.exists(config_manager.UTA_YOUTUBE_CLIENT_SECRET_FILE):
                logger.error(f"UTA YouTube: Client secret file '{config_manager.UTA_YOUTUBE_CLIENT_SECRET_FILE}' not found. Cannot authenticate.")
                config_manager.uta_yt_service = None
                return None
            try:
                logger.info("UTA YouTube: Initiating new YouTube API OAuth flow. Please follow browser instructions.")
                flow = config_manager.InstalledAppFlow.from_client_secrets_file(
                    config_manager.UTA_YOUTUBE_CLIENT_SECRET_FILE, config_manager.UTA_YOUTUBE_API_SCOPES
                )
                creds = flow.run_local_server(port=0)
                logger.info("UTA YouTube: OAuth flow completed successfully through local server.")
            except Exception as e:
                logger.error(f"UTA YouTube: OAuth flow failed: {e}", exc_info=True)
                logger.error("UTA YouTube: If running on a headless server or cannot open a browser, "
                             f"you may need to generate '{config_manager.UTA_YOUTUBE_TOKEN_FILE}' "
                             "manually once on a machine with a browser, then copy it to the server.")
                config_manager.uta_yt_service = None
                return None

        if creds:
            try:
                with open(config_manager.UTA_YOUTUBE_TOKEN_FILE, 'w') as token_file:
                    token_file.write(creds.to_json())
                logger.info(f"UTA YouTube: Token saved to {config_manager.UTA_YOUTUBE_TOKEN_FILE}")
            except Exception as e:
                logger.error(f"UTA YouTube: Error saving token file '{config_manager.UTA_YOUTUBE_TOKEN_FILE}': {e}")

    if creds and creds.valid:
        try:
            config_manager.uta_yt_service = config_manager.google_build('youtube', 'v3', credentials=creds, cache_discovery=False)
            logger.info("UTA YouTube: YouTube API service initialized successfully.")
            return config_manager.uta_yt_service
        except Exception as e:
            logger.error(f"UTA YouTube: Failed to build YouTube service object: {e}", exc_info=True)
            config_manager.uta_yt_service = None
            return None
    else:
        logger.error("UTA YouTube: Failed to obtain valid credentials for YouTube API after auth attempts.")
        config_manager.uta_yt_service = None
        return None

async def create_youtube_live_stream_resource(service, twitch_username: str):
    if not service: return None, None, None

    stream_title = f"UTA Restream Endpoint for {twitch_username} - {int(time.time())}-{random.randint(1000,9999)}"

    try:
        request_body = {
            "snippet": {
                "title": stream_title,
                "description": "Reusable live stream resource for UTA bot archival"
            },
            "cdn": {
                "frameRate": "variable",
                "ingestionType": "rtmp",
                "resolution": "variable"
            },
            "status": {"streamStatus": "ready"}
        }
        request = service.liveStreams().insert(part="snippet,cdn,status", body=request_body)
        response = await asyncio.to_thread(request.execute)

        stream_id = response['id']
        ingestion_info = response['cdn']['ingestionInfo']
        rtmp_url = ingestion_info['ingestionAddress']
        stream_key = ingestion_info['streamName']

        logger.info(f"UTA YouTube: Successfully created liveStream resource ID: {stream_id} for {twitch_username}")
        return stream_id, rtmp_url, stream_key
    except config_manager.GoogleHttpError as e:
        logger.error(f"UTA YouTube: API error creating liveStream resource: {e.content.decode() if e.content else e}", exc_info=True)
    except Exception as e:
        logger.error(f"UTA YouTube: Unexpected error creating liveStream resource: {e}", exc_info=True)
    return None, None, None

async def create_youtube_broadcast(service, bound_live_stream_id: str, title: str, description: str,
                                   privacy_status: str, start_time_iso: str):
    if not service: return None

    try:
        request_body = {
            "snippet": {
                "title": title,
                "description": description,
                "scheduledStartTime": start_time_iso,
            },
            "status": {
                "privacyStatus": privacy_status,
                "selfDeclaredMadeForKids": False,
            },
            "contentDetails": {
                "enableAutoStart": True,
                "enableAutoStop": True,
                "latencyPreference": "ultraLow",
                "enableDvr": True,
            }
        }
        insert_request = service.liveBroadcasts().insert(part="snippet,status,contentDetails", body=request_body)
        response = await asyncio.to_thread(insert_request.execute)
        broadcast_id = response['id']

        bind_request = service.liveBroadcasts().bind(
            id=broadcast_id,
            part="id,snippet,contentDetails,status",
            streamId=bound_live_stream_id
        )
        await asyncio.to_thread(bind_request.execute)

        logger.info(f"UTA YouTube: Successfully created and bound liveBroadcast ID: {broadcast_id} (Title: {title}) to stream ID: {bound_live_stream_id}")
        return broadcast_id
    except config_manager.GoogleHttpError as e:
        logger.error(f"UTA YouTube: API error creating/binding liveBroadcast: {e.content.decode() if e.content else e}", exc_info=True)
    except Exception as e:
        logger.error(f"UTA YouTube: Unexpected error creating/binding liveBroadcast: {e}", exc_info=True)
    return None

async def transition_youtube_broadcast(service, broadcast_id: str, status: str):
    if not service or not broadcast_id: return False
    try:
        request = service.liveBroadcasts().transition(
            broadcastStatus=status,
            id=broadcast_id,
            part="id,snippet,contentDetails,status"
        )
        await asyncio.to_thread(request.execute)
        logger.info(f"UTA YouTube: Successfully transitioned broadcast {broadcast_id} to status '{status}'.")
        return True
    except config_manager.GoogleHttpError as e:
        logger.error(f"UTA YouTube: API error transitioning broadcast {broadcast_id} to '{status}': {e.content.decode() if e.content else e}", exc_info=True)
    except Exception as e:
        logger.error(f"UTA YouTube: Unexpected error transitioning broadcast {broadcast_id} to '{status}': {e}", exc_info=True)
    return False

async def get_youtube_video_details(service, video_id: str):
    """Fetches video details, primarily for the snippet which includes title, description, categoryId."""
    if not service or not video_id: return None
    try:
        request = service.videos().list(
            part="snippet", # Fetch the whole snippet
            id=video_id
        )
        response = await asyncio.to_thread(request.execute)
        if response and response.get("items"):
            return response["items"][0]
        logger.warning(f"UTA YouTube Get Details: Video details not found for ID {video_id}. API response: {response}")
        return None
    except config_manager.GoogleHttpError as e:
        logger.error(f"UTA YouTube Get Details: API error getting video details for {video_id}: {e.content.decode() if e.content else e}", exc_info=True)
    except Exception as e:
        logger.error(f"UTA YouTube Get Details: Failed to get video details for {video_id}: {e}", exc_info=True)
    return None


async def update_youtube_broadcast_metadata(service, broadcast_id_or_video_id: str,
                                            new_title: str = None,
                                            new_description: str = None,
                                            tags: list[str] = None,
                                            category_id: str = None
                                            ):
    if not service or not broadcast_id_or_video_id:
        logger.error("UTA YouTube Update Meta: Service or video ID not provided.")
        return False

    request_body_for_log = {} # For logging in case of error

    try:
        # Fetch existing snippet details first to ensure required fields are present
        existing_video_details = await get_youtube_video_details(service, broadcast_id_or_video_id)
        if not existing_video_details or 'snippet' not in existing_video_details:
            logger.error(f"UTA YouTube Update Meta: Could not fetch existing details for video {broadcast_id_or_video_id} before update.")
            return False

        existing_snippet = existing_video_details['snippet']

        # Prepare the snippet for the update request
        # Start with essential fields from existing snippet if not overridden by new values
        snippet_update_payload = {
            "title": new_title if new_title is not None else existing_snippet.get("title"),
            "description": new_description if new_description is not None else existing_snippet.get("description", ""),
            "tags": tags if tags is not None else existing_snippet.get("tags", []),
            "categoryId": category_id if category_id is not None else existing_snippet.get("categoryId")
        }

        # Determine if any actual changes are being made
        changed_something = False
        if new_title is not None and new_title != existing_snippet.get("title"):
            changed_something = True
        if new_description is not None and new_description != existing_snippet.get("description", ""): # Compare with existing or empty
            changed_something = True
        if tags is not None and set(tags) != set(existing_snippet.get("tags", [])):
            changed_something = True
        if category_id is not None and category_id != existing_snippet.get("categoryId"):
            changed_something = True

        if not changed_something:
            logger.info(f"UTA YouTube Update Meta: No actual metadata changes detected for video {broadcast_id_or_video_id}. Skipping API call.")
            return True

        # Validate title: It cannot be empty if we are sending the snippet update.
        if not snippet_update_payload.get("title"): # Check if title is None or empty string
             logger.error(f"UTA YouTube Update Meta: Video title cannot be empty for video {broadcast_id_or_video_id}. Original: '{existing_snippet.get('title')}', New (if any): '{new_title}'. Update aborted.")
             return False # Abort if title would be empty

        # Ensure categoryId is present if it was there originally, or if a new one is provided.
        # If it was never there and not provided now, it might be okay for YouTube to default it.
        if not snippet_update_payload.get("categoryId") and existing_snippet.get("categoryId"):
            logger.warning(f"UTA YouTube Update Meta: categoryId was present ('{existing_snippet.get('categoryId')}') but is now missing in payload for video {broadcast_id_or_video_id}. This might be unintended.")
            # Depending on API strictness, you might need to ensure it's always populated if it existed.
            # For now, we allow it to be potentially unset if `category_id` arg is `None` and it was not in `existing_snippet`.

        request_body_for_log = { # Assign to outer scope variable for logging in HttpError
            "id": broadcast_id_or_video_id,
            "snippet": snippet_update_payload
        }

        logger.debug(f"UTA YouTube Update Meta: Request body for video {broadcast_id_or_video_id}: {request_body_for_log}")

        request = service.videos().update(part="snippet", body=request_body_for_log)
        await asyncio.to_thread(request.execute)
        logger.info(f"UTA YouTube Update Meta: Successfully updated snippet metadata for video/broadcast {broadcast_id_or_video_id}.")
        return True
    except config_manager.GoogleHttpError as e:
        logger.error(f"UTA YouTube Update Meta: API error updating metadata for {broadcast_id_or_video_id}. Sent body: {request_body_for_log}. Error: {e.content.decode() if e.content else e}", exc_info=True)
    except Exception as e:
        logger.error(f"UTA YouTube Update Meta: Unexpected error updating metadata for {broadcast_id_or_video_id}: {e}", exc_info=True)
    return False


async def add_video_to_youtube_playlist(service, video_id: str, playlist_id: str):
    if not service or not video_id or not playlist_id: return False
    try:
        request_body = {
            "snippet": {
                "playlistId": playlist_id,
                "resourceId": {
                    "kind": "youtube#video",
                    "videoId": video_id
                }
            }
        }
        request = service.playlistItems().insert(part="snippet", body=request_body)
        await asyncio.to_thread(request.execute)
        logger.info(f"UTA YouTube: Successfully added video {video_id} to playlist {playlist_id}.")
        return True
    except config_manager.GoogleHttpError as e:
        if hasattr(e, 'resp') and e.resp.status == 409 and "playlistItemNotUnique" in str(e.content):
            logger.info(f"UTA YouTube: Video {video_id} is already in playlist {playlist_id}.")
            return True
        logger.error(f"UTA YouTube: API error adding video {video_id} to playlist {playlist_id}: {e.content.decode() if e.content else e}", exc_info=True)
    except Exception as e:
        logger.error(f"UTA YouTube: Unexpected error adding video {video_id} to playlist {playlist_id}: {e}", exc_info=True)
    return False

async def set_youtube_video_privacy(service, video_id: str, privacy_status: str):
    if not service or not video_id: return False
    allowed_statuses = ["public", "private", "unlisted"]
    if privacy_status.lower() not in allowed_statuses:
        logger.error(f"UTA YouTube: Invalid privacy status '{privacy_status}'. Must be one of {allowed_statuses}.")
        return False

    try:
        request_body = {
            "id": video_id,
            "status": {
                "privacyStatus": privacy_status.lower()
            }
        }
        request = service.videos().update(part="status", body=request_body)
        await asyncio.to_thread(request.execute)
        logger.info(f"UTA YouTube: Successfully set privacy of video {video_id} to '{privacy_status}'.")
        return True
    except config_manager.GoogleHttpError as e:
        logger.error(f"UTA YouTube: API error setting privacy for video {video_id}: {e.content.decode() if e.content else e}", exc_info=True)
    except Exception as e:
        logger.error(f"UTA YouTube: Unexpected error setting privacy for video {video_id}: {e}", exc_info=True)
    return False