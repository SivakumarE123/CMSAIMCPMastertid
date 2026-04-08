import os
import base64
import json
import requests
import logging
from datetime import datetime, timedelta
import tempfile
from concurrent.futures import ThreadPoolExecutor
from cryptography.fernet import Fernet

from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import re

from urllib.parse import urlparse, parse_qs
import xml.etree.ElementTree as ET

# ===== INIT =====
load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Audio/video extensions for blob container filtering
_AUDIO_VIDEO_EXTENSIONS = {
    ".mp4", ".mov", ".mkv", ".wav", ".mp3", ".flac", ".ogg",
    ".m4a", ".wma", ".aac", ".webm", ".avi",
}

# MIME types for Google Drive folder filtering
_DRIVE_AUDIO_VIDEO_MIMES = {
    "audio/mpeg", "audio/wav", "audio/x-wav", "audio/mp4", "audio/ogg",
    "audio/flac", "audio/x-flac", "audio/aac", "audio/webm", "audio/x-m4a",
    "video/mp4", "video/quicktime", "video/x-matroska", "video/webm",
    "video/x-msvideo", "video/avi",
}

# ===== ENV =====
ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
ACCOUNT_KEY = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
CONTAINER = os.getenv("AZURE_BLOB_CONTAINER")

SPEECH_KEY = os.getenv("AZURE_SPEECH_KEY")
SPEECH_REGION = os.getenv("AZURE_SPEECH_REGION")
API_VERSION = os.getenv("AZURE_SPEECH_API_VERSION")

# ===== ENCRYPTION KEY =====
FERNET_KEY = os.getenv("FERNET_KEY")
if FERNET_KEY:
    _cipher = Fernet(FERNET_KEY.encode())
else:
    _cipher = None
    logger.warning("FERNET_KEY not set — secrets will not be encrypted")


def encrypt_secret(plain_text: str) -> str:
    if not _cipher:
        raise ValueError("FERNET_KEY not configured")
    return _cipher.encrypt(plain_text.encode()).decode()


def decrypt_secret(encrypted_text: str) -> str:
    if not _cipher:
        raise ValueError("FERNET_KEY not configured")
    return _cipher.decrypt(encrypted_text.encode()).decode()


# ===== BLOB CLIENT =====
blob_service_client = BlobServiceClient(
    f"https://{ACCOUNT_NAME}.blob.core.windows.net",
    credential=ACCOUNT_KEY
)


# ===== HELPERS =====

def generate_sas_url(blob_name):
    sas_token = generate_blob_sas(
        account_name=ACCOUNT_NAME,
        container_name=CONTAINER,
        blob_name=blob_name,
        account_key=ACCOUNT_KEY,
        permission=BlobSasPermissions(read=True),
        expiry=datetime.utcnow() + timedelta(hours=2)
    )
    return f"https://{ACCOUNT_NAME}.blob.core.windows.net/{CONTAINER}/{blob_name}?{sas_token}"


def upload_file_to_blob(file_path):
    blob_name = os.path.basename(file_path)
    blob_client = blob_service_client.get_blob_client(CONTAINER, blob_name)

    with open(file_path, "rb") as f:
        blob_client.upload_blob(f, overwrite=True)

    logger.info(f"Uploaded to blob: {blob_name}")
    return generate_sas_url(blob_name)


# ===== CONTAINER / FOLDER HELPERS =====

def is_container_url(url: str) -> bool:
    """Detect if a blob URL points to a container (no blob path after container name)."""
    try:
        parsed = urlparse(url)
        # Path like /containername or /containername/ (no blob filename)
        path_parts = [p for p in parsed.path.strip("/").split("/") if p]
        if len(path_parts) <= 1:
            # Also check SAS sr=c (container scope)
            qs = parse_qs(parsed.query)
            sr = qs.get("sr", [""])[0]
            if sr == "c" or len(path_parts) <= 1:
                return True
        return False
    except Exception:
        return False


def list_container_blobs(container_url: str) -> list:
    """List audio/video blobs in a container using the Azure Blob REST API.
    The SAS token must include list (l) and read (r) permissions.
    Returns list of full blob URLs with the same SAS token.
    """
    parsed = urlparse(container_url)
    base_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path.rstrip('/')}"
    sas_token = parsed.query  # everything after ?

    # List blobs via REST: ?restype=container&comp=list&<sas>
    list_url = f"{base_url}?restype=container&comp=list&{sas_token}"
    logger.info(f"Listing blobs: {list_url[:120]}...")

    resp = requests.get(list_url, timeout=30)
    if resp.status_code != 200:
        raise Exception(
            f"Failed to list blobs (HTTP {resp.status_code}). "
            f"Ensure the SAS token has List (l) permission. Response: {resp.text[:300]}"
        )

    # Parse XML response
    root = ET.fromstring(resp.text)
    blobs = []
    for blob_elem in root.iter("Blob"):
        name_elem = blob_elem.find("Name")
        if name_elem is None or not name_elem.text:
            continue
        blob_name = name_elem.text
        ext = os.path.splitext(blob_name)[1].lower()
        if ext in _AUDIO_VIDEO_EXTENSIONS:
            blob_full_url = f"{base_url}/{blob_name}?{sas_token}"
            blobs.append({"name": blob_name, "url": blob_full_url})
            logger.info(f"  Found blob: {blob_name}")

    logger.info(f"Listed {len(blobs)} audio/video blob(s) from container")
    return blobs


def is_drive_folder_url(url: str) -> bool:
    """Detect if a Google Drive URL points to a folder."""
    return "/folders/" in url or "mimeType=application/vnd.google-apps.folder" in url


def extract_folder_id(url: str) -> str:
    """Extract folder ID from a Google Drive folder URL."""
    match = re.search(r'/folders/([-\w]+)', url)
    if match:
        return match.group(1)
    match = re.search(r'id=([-\w]+)', url)
    if match:
        return match.group(1)
    raise ValueError(f"Cannot extract folder ID from: {url}")


def list_drive_folder_files(folder_url: str, creds_dict: dict) -> list:
    """List audio/video files in a Google Drive folder.
    Returns list of dicts: [{"name": str, "url": str, "file_id": str}]
    """
    folder_id = extract_folder_id(folder_url)
    creds = Credentials.from_authorized_user_info(creds_dict)
    service = build('drive', 'v3', credentials=creds)

    # Build MIME type query
    mime_clauses = " or ".join(f"mimeType='{m}'" for m in _DRIVE_AUDIO_VIDEO_MIMES)
    query = f"'{folder_id}' in parents and ({mime_clauses}) and trashed=false"

    files = []
    page_token = None
    while True:
        resp = service.files().list(
            q=query,
            fields="nextPageToken, files(id, name, mimeType)",
            pageSize=100,
            pageToken=page_token,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        ).execute()
        for f in resp.get("files", []):
            files.append({
                "name": f["name"],
                "file_id": f["id"],
                "url": f"https://drive.google.com/file/d/{f['id']}/view",
            })
            logger.info(f"  Found Drive file: {f['name']} ({f['mimeType']})")
        page_token = resp.get("nextPageToken")
        if not page_token:
            break

    logger.info(f"Listed {len(files)} audio/video file(s) from Drive folder {folder_id}")
    return files


# ===== SOURCE HANDLERS =====

def handle_base64_input(base64_data, filename="audio.wav"):
    ext = os.path.splitext(filename)[1] or ".wav"
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=ext)
    file_bytes = base64.b64decode(base64_data)
    temp_file.write(file_bytes)
    temp_file.close()
    return upload_file_to_blob(temp_file.name)


def handle_blob_url_input(blob_url):
    if not blob_url or not blob_url.startswith("https://"):
        raise ValueError("Invalid blob URL")
    logger.info(f"Using existing blob URL: {blob_url[:80]}...")
    return blob_url


def download_drive_file(drive_url, creds_dict):
    match = re.search(r'/d/(.*?)/|id=([\w-]+)', drive_url)
    if not match:
        raise ValueError("Invalid Google Drive URL — expected /d/<id>/ or id=<id>")
    file_id = match.group(1) or match.group(2)

    creds = Credentials.from_authorized_user_info(creds_dict)
    service = build('drive', 'v3', credentials=creds)

    request = service.files().get_media(fileId=file_id, supportsAllDrives=True)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)

    done = False
    while not done:
        _, done = downloader.next_chunk()

    fh.seek(0)
    return fh


def handle_gdrive_input(drive_url, creds_json_encrypted):
    creds_json = decrypt_secret(creds_json_encrypted)
    creds_dict = json.loads(creds_json)

    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".mp4")
    file_stream = download_drive_file(drive_url, creds_dict)
    temp_file.write(file_stream.read())
    temp_file.close()

    logger.info("Downloaded file from Google Drive")
    return upload_file_to_blob(temp_file.name)


# ===== RESOLVE SINGLE SOURCE TO BLOB URL =====

def resolve_source(item):
    """Resolve one source item to a blob URL.
    item: {"source_type": "file_upload"|"blob_url"|"gdrive",
           "data": "...", "filename": "...", "creds_encrypted": "..."}
    Returns: {"name": str, "blob_url": str, "error": str|None}
    """
    source_type = item.get("source_type", "")
    data = item.get("data", "")
    filename = item.get("filename", "file")
    creds_encrypted = item.get("creds_encrypted", "")

    try:
        if source_type == "file_upload":
            blob_url = handle_base64_input(data, filename)
        elif source_type == "blob_url":
            blob_url = handle_blob_url_input(data)
        elif source_type == "gdrive":
            blob_url = handle_gdrive_input(data, creds_encrypted)
        else:
            raise ValueError(f"Invalid source type: {source_type}")

        return {"name": filename, "blob_url": blob_url, "error": None}

    except Exception as e:
        logger.error(f"Failed to resolve {filename}: {e}")
        return {"name": filename, "blob_url": None, "error": str(e)}


# ===== SPEECH API =====

def submit_transcription(content_urls):
    url = f"https://{SPEECH_REGION}.api.cognitive.microsoft.com/speechtotext/transcriptions:submit?api-version={API_VERSION}"

    headers = {
        "Ocp-Apim-Subscription-Key": SPEECH_KEY,
        "Content-Type": "application/json"
    }

    payload = {
        "contentUrls": content_urls,
        "locale": "en-US",
        "displayName": f"Batch Transcription ({len(content_urls)} files)",
        "properties": {
            "wordLevelTimestampsEnabled": True,
            "timeToLiveHours": 24
        }
    }

    response = requests.post(url, headers=headers, json=payload)

    if response.status_code not in [200, 201, 202]:
        logger.error(f"Speech API error ({response.status_code}): {response.text}")
        raise Exception(f"Speech API failed ({response.status_code}): {response.text}")

    job_url = response.headers.get("Location", "")
    job_url = job_url.replace("transcriptions:submit/", "transcriptions/")
    logger.info(f"Speech job created: {job_url}")
    return job_url


# ===== BATCH ENTRY: MULTIPLE SOURCES =====

def process_batch_input(sources_json):
    """
    sources_json: JSON string of list of source items:
    [{"source_type": "file_upload", "data": "...", "filename": "a.mp4"},
     {"source_type": "blob_url", "data": "https://..."},
     {"source_type": "gdrive", "data": "https://drive...", "creds_encrypted": "..."}]

    Returns: {"files": [...], "speech_job_url": str, "total": int, "uploaded": int, "failed": int}
    """
    sources = json.loads(sources_json)
    if not sources:
        raise ValueError("No sources provided")

    # ---- AUTO-EXPAND containers / folders into individual files ----
    expanded = []
    for item in sources:
        src_type = item.get("source_type", "")
        data = item.get("data", "")
        creds_enc = item.get("creds_encrypted", "")

        # Azure Blob: detect container URL and expand
        if src_type == "blob_url" and is_container_url(data):
            logger.info(f"Expanding blob container URL: {data[:80]}...")
            try:
                blobs = list_container_blobs(data)
                if not blobs:
                    logger.warning("Container listed 0 audio/video files")
                    expanded.append({
                        "source_type": "blob_url",
                        "data": data,
                        "filename": "container (empty)",
                        "creds_encrypted": "",
                    })
                else:
                    for b in blobs:
                        expanded.append({
                            "source_type": "blob_url",
                            "data": b["url"],
                            "filename": b["name"],
                            "creds_encrypted": "",
                        })
                    logger.info(f"Expanded container into {len(blobs)} blob source(s)")
            except Exception as e:
                logger.error(f"Container expansion failed: {e}")
                expanded.append({
                    "source_type": "blob_url",
                    "data": data,
                    "filename": item.get("filename", "container"),
                    "creds_encrypted": "",
                    "_expand_error": str(e),
                })
            continue

        # Google Drive: detect folder URL and expand
        if src_type == "gdrive" and is_drive_folder_url(data):
            logger.info(f"Expanding Drive folder URL: {data[:80]}...")
            try:
                creds_json = decrypt_secret(creds_enc)
                creds_dict = json.loads(creds_json)
                drive_files = list_drive_folder_files(data, creds_dict)
                if not drive_files:
                    logger.warning("Drive folder listed 0 audio/video files")
                    expanded.append({
                        "source_type": "gdrive",
                        "data": data,
                        "filename": "folder (empty)",
                        "creds_encrypted": creds_enc,
                    })
                else:
                    for df in drive_files:
                        expanded.append({
                            "source_type": "gdrive",
                            "data": df["url"],
                            "filename": df["name"],
                            "creds_encrypted": creds_enc,
                        })
                    logger.info(f"Expanded Drive folder into {len(drive_files)} file source(s)")
            except Exception as e:
                logger.error(f"Drive folder expansion failed: {e}")
                expanded.append({
                    "source_type": "gdrive",
                    "data": data,
                    "filename": item.get("filename", "folder"),
                    "creds_encrypted": creds_enc,
                    "_expand_error": str(e),
                })
            continue

        # Not a container/folder — keep as-is
        expanded.append(item)

    sources = expanded
    logger.info(f"After expansion: {len(sources)} source(s)")

    # Parallel upload/resolve
    results = []
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = [pool.submit(resolve_source, item) for item in sources]
        for f in futures:
            results.append(f.result())

    # Separate success / failed
    blob_urls = []
    files_info = []
    for r in results:
        files_info.append({
            "name": r["name"],
            "blob_url": r.get("blob_url", ""),
            "status": "uploaded" if r["blob_url"] else "failed",
            "error": r.get("error", "")
        })
        if r["blob_url"]:
            blob_urls.append(r["blob_url"])

    uploaded = len(blob_urls)
    failed = len(results) - uploaded

    if not blob_urls:
        raise Exception("All files failed to upload")

    # Submit single batch job with all URLs
    job_url = submit_transcription(blob_urls)

    return {
        "files": files_info,
        "speech_job_url": job_url,
        "total": len(results),
        "uploaded": uploaded,
        "failed": failed
    }


# ===== BATCH STATUS + PER-FILE RESULTS =====

def get_batch_transcription_result(job_url):
    """Fetch batch status. Returns per-file transcription when done."""
    headers = {
        "Ocp-Apim-Subscription-Key": SPEECH_KEY
    }

    response = requests.get(job_url, headers=headers)
    if response.status_code != 200:
        raise Exception(f"Status check failed ({response.status_code}): {response.text}")

    job_data = response.json()
    status = job_data.get("status", "Unknown")

    if status != "Succeeded":
        return {"status": status, "files": [], "total_text": None}

    files_url = job_data.get("links", {}).get("files", "")
    if not files_url:
        return {"status": status, "files": [], "total_text": None}

    files_resp = requests.get(files_url, headers=headers)
    if files_resp.status_code != 200:
        raise Exception(f"Files fetch failed ({files_resp.status_code}): {files_resp.text}")

    # Collect per-file transcriptions
    file_results = []
    all_text = []

    for f in files_resp.json().get("values", []):
        if f.get("kind") == "Transcription":
            file_name = f.get("name", "unknown")
            content_url = f.get("links", {}).get("contentUrl", "")
            file_text = ""

            if content_url:
                content_resp = requests.get(content_url, headers=headers)
                if content_resp.status_code == 200:
                    content = content_resp.json()
                    source_url = content.get("source", "")
                    phrases = content.get("combinedRecognizedPhrases", [])
                    if phrases:
                        file_text += phrases[0].get("display", "") + "\n"

            file_text = file_text.strip()
            file_results.append({
                "name": file_name,
                "text": file_text,
                "status": "completed" if file_text else "no_speech"
            })
            if file_text:
                all_text.append(file_text)

    return {
        "status": status,
        "files": file_results,
        "total_text": "\n\n---\n\n".join(all_text),
        "completed_count": len([f for f in file_results if f["status"] == "completed"]),
        "total_count": len(file_results)
    }
