import os
import asyncio
import base64
import logging
import time
import httpx
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# CONFIGURATION
# ============================================================

MISTRAL_ENDPOINT = os.getenv("AZUREAI_ENDPOINT", "")
MISTRAL_KEY = os.getenv("AZUREAI_API_KEY", "")
MISTRAL_MODEL = os.getenv("MODEL_NAME", "mistral-document-ai-2512-2")

REQUEST_TIMEOUT = 60
MAX_RETRIES = 3

# Optional: Blob URL mode (enterprise)
USE_BLOB = os.getenv("USE_BLOB", "false").lower() == "true"

# ============================================================
# LOGGER
# ============================================================

logger = logging.getLogger("mistral_ocr")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)

# ============================================================
# HTTP CLIENT (REUSE)
# ============================================================

client = httpx.AsyncClient(
    timeout=httpx.Timeout(REQUEST_TIMEOUT),
    limits=httpx.Limits(max_connections=200, max_keepalive_connections=50)
)

# ============================================================
# CORE API CALL
# ============================================================

async def call_mistral_api(payload: dict):

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {MISTRAL_KEY}"
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            start = time.time()

            response = await client.post(
                MISTRAL_ENDPOINT,
                headers=headers,
                json=payload
            )

            latency = round((time.time() - start) * 1000, 2)

            logger.info(f"OCR API success | latency={latency}ms")

            # 🔥 DEBUG (KEEP THIS)
            print("🔍 STATUS:", response.status_code)
            print("🔍 RESPONSE:", response.text[:500])

            response.raise_for_status()

            return response.json()

        except httpx.HTTPStatusError as e:
            logger.error(
                f"HTTP error | status={e.response.status_code} | body={e.response.text}"
            )

        except Exception as e:
            logger.warning(f"Retry {attempt}/{MAX_RETRIES} failed | error={str(e)}")

        await asyncio.sleep(2 ** attempt)

    logger.error("OCR API failed after retries")

    return {"error": "Mistral API failed after retries"}


# ============================================================
# MAIN OCR FUNCTION
# ============================================================

async def process_mistral_ocr(file_base64: str, mime_type: str, blob_url: str = None):

    if not MISTRAL_KEY or not MISTRAL_ENDPOINT:
        return {"error": "Missing credentials"}

    try:
        # ----------------------------------------------------
        # 1. VALIDATE BASE64
        # ----------------------------------------------------
        try:
            file_bytes = base64.b64decode(file_base64)
        except Exception:
            return {"error": "Invalid base64 input"}

        size_mb = round(len(file_bytes) / (1024 * 1024), 2)

        logger.info(f"OCR request | size={size_mb}MB | mime={mime_type}")

        if size_mb > 30:
            return {"error": "File too large (>30MB)"}

        # ----------------------------------------------------
        # 2. BUILD PAYLOAD (FIXED)
        # ----------------------------------------------------

        if USE_BLOB and blob_url:
            # ✅ ENTERPRISE MODE (FASTEST)
            payload = {
                "model": MISTRAL_MODEL,
                "document": {
                    "type": "document_url",
                    "document_url": blob_url
                }
            }
        else:
            # Base64 via data URI (matching working format)
            payload = {
                "model": MISTRAL_MODEL,
                "document": {
                    "type": "document_url",
                    "document_url": f"data:{mime_type};base64,{file_base64}"
                }
            }

        # ----------------------------------------------------
        # 3. CALL API
        # ----------------------------------------------------
        result = await call_mistral_api(payload)

        if "error" in result:
            return result

        logger.info("OCR success")

        return {
            "status": "success",
            "data": result
        }

    except Exception as e:
        logger.exception("Unexpected OCR failure")
        return {"error": str(e)}