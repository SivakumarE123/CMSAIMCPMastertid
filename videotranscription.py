import os
import base64
import uuid
import requests
import logging
from datetime import datetime, timedelta
import tempfile

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

# ===== CLIENT =====
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


# ===== UPLOAD FILE → BLOB =====
def upload_file_to_blob(file_path):
    blob_name = os.path.basename(file_path)

    blob_client = blob_service_client.get_blob_client(CONTAINER, blob_name)

    with open(file_path, "rb") as f:
        blob_client.upload_blob(f, overwrite=True)

    logger.info(f"Uploaded to blob: {blob_name}")
    return generate_sas_url(blob_name)


# ===== BASE64 HANDLER =====
def handle_base64_input(base64_data, filename="audio.wav"):
    ext = os.path.splitext(filename)[1] or ".wav"
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=ext)
    file_bytes = base64.b64decode(base64_data)

    temp_file.write(file_bytes)
    temp_file.close()

    return upload_file_to_blob(temp_file.name)


# ===== GOOGLE DRIVE HANDLER =====
def download_drive_file(drive_url, creds_dict):
    match = re.search(r'/d/(.*?)/|id=([\w-]+)', drive_url)
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


def handle_gdrive_input(drive_url, creds_dict):
    temp_video = tempfile.NamedTemporaryFile(delete=False, suffix=".mp4")

    file_stream = download_drive_file(drive_url, creds_dict)
    temp_video.write(file_stream.read())
    temp_video.close()

    logger.info("Downloaded file from Google Drive")

    return upload_file_to_blob(temp_video.name)


# ===== SPEECH API =====
def submit_transcription(audio_urls):
    url = f"https://{SPEECH_REGION}.api.cognitive.microsoft.com/speechtotext/transcriptions:submit?api-version={API_VERSION}"

    headers = {
        "Ocp-Apim-Subscription-Key": SPEECH_KEY,
        "Content-Type": "application/json"
    }

    payload = {
        "contentUrls": audio_urls,
        "locale": "en-US",
        "displayName": "Batch Transcription Job",
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
    # Fix: status URL should not contain :submit
    job_url = job_url.replace("transcriptions:submit/", "transcriptions/")
    logger.info(f"Speech job created: {job_url}")
    return job_url


# ===== MAIN FUNCTION =====
def process_input(input_type, data, creds_dict=None, filename="audio.wav"):
    """
    input_type: 'base64' or 'gdrive'
    data: base64 string OR Google Drive URL
    """

    if input_type == "base64":
        blob_url = handle_base64_input(data, filename)

    elif input_type == "gdrive":
        if not creds_dict:
            raise ValueError("Google Drive creds required")
        blob_url = handle_gdrive_input(data, creds_dict)

    else:
        raise ValueError("Invalid input type")

    job_url = submit_transcription([blob_url])

    return {
        "blob_url": blob_url,
        "speech_job_url": job_url
    }


# ===== OPTIONAL STATUS =====
def get_transcription_status(job_url):
    headers = {
        "Ocp-Apim-Subscription-Key": SPEECH_KEY
    }

    response = requests.get(job_url, headers=headers)

    if response.status_code != 200:
        raise Exception(response.text)

    return response.json()


def get_transcription_result(job_url):
    """Fetch transcription status, and if succeeded, fetch the actual transcribed text."""
    headers = {
        "Ocp-Apim-Subscription-Key": SPEECH_KEY
    }

    # Get job status
    response = requests.get(job_url, headers=headers)
    if response.status_code != 200:
        raise Exception(f"Status check failed ({response.status_code}): {response.text}")

    job_data = response.json()
    status = job_data.get("status", "Unknown")

    if status != "Succeeded":
        return {"status": status, "text": None}

    # Fetch files list
    files_url = job_data.get("links", {}).get("files", "")
    if not files_url:
        return {"status": status, "text": None}

    files_resp = requests.get(files_url, headers=headers)
    if files_resp.status_code != 200:
        raise Exception(f"Files fetch failed ({files_resp.status_code}): {files_resp.text}")

    # Find the transcription content
    transcription_text = ""
    for f in files_resp.json().get("values", []):
        if f.get("kind") == "Transcription":
            content_url = f.get("links", {}).get("contentUrl", "")
            if content_url:
                content_resp = requests.get(content_url, headers=headers)
                if content_resp.status_code == 200:
                    content = content_resp.json()
                    for phrase in content.get("combinedRecognizedPhrases", []):
                        transcription_text += phrase.get("display", "") + "\n"

    return {"status": status, "text": transcription_text.strip()}
