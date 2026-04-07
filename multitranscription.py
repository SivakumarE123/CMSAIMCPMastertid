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

# ===== INIT =====
load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
                    for phrase in content.get("combinedRecognizedPhrases", []):
                        file_text += phrase.get("display", "") + "\n"

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
