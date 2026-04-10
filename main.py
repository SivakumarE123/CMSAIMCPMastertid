#from auth import TID_ISSUER, TID_AUDIENCE, TID_SCOPE, _load_oidc_config
        # -------------------------------------------------------------
# Tool: protect_multi
# -------------------------------------------------------------
# Detects PII entities in the input text and replaces them
# with anonymized placeholders. Optionally accepts a custom
# deny list to flag additional terms beyond standard PII.
# -------------------------------------------------------------

# Configure TID (Trimble ID) JWT verification
# Discover JWKS via OIDC to avoid hardcoding paths

# if not TID_AUDIENCE:
#     raise RuntimeError("Missing required environment variable: TID_AUDIENCE")
# tid_cfg = _load_oidc_config()
# auth_provider = JWTVerifier(
#     jwks_uri=tid_cfg["jwks_uri"],
#     issuer=tid_cfg.get("issuer", TID_ISSUER),
#     audience=TID_AUDIENCE.split(",") if "," in TID_AUDIENCE else [TID_AUDIENCE],
#     required_scopes=TID_SCOPE.split() if TID_SCOPE else [],
# )

# # mcp = FastMCP(name="ai-search-mcp", auth=auth_provider)

# # Build tool description dynamically so callers know valid product values
# _TOOL_DESC = (
#     "Perform an Azure AI Search call with progress notifications.\n"
#     f"Available products: {', '.join(AVAILABLE_PRODUCTS)}.\n"
#     "Pass one of these product names to select the correct search index."
# )
# ============================================================
# 🔐 LOCK (PREVENT DB STORM)
# ============================================================


# ============================================================
# MCP Server for PII Protection and Document OCR (WITH AUTH)
# ============================================================

import os
from fastmcp import FastMCP, Context
from denylist import apply_multiple_deny_lists
from mistral import process_mistral_ocr
from videotranscription import process_input as process_video_input, get_transcription_status, get_transcription_result
from multitranscription import process_batch_input, get_batch_transcription_result, encrypt_secret
import json
import asyncio
from dotenv import load_dotenv

from cachetools import TTLCache
from cosmosservice import get_user_permissions
from cosmosservice import upsert_user as cosmos_upsert_user
from cosmosservice import list_all_users as cosmos_list_all_users
from cosmosservice import get_all_products as cosmos_get_all_products
from cosmosservice import delete_user as cosmos_delete_user

load_dotenv()

mcp = FastMCP("Multitool")

# ============================================================
# L1 CACHE
# ============================================================

CACHE = TTLCache(maxsize=10000, ttl=30)

def get_cache(key):
    return CACHE.get(key)

def set_cache(key, value):
    CACHE[key] = value


# ============================================================
# LOCK (PREVENT DB STORM)
# ============================================================

LOCKS = {}


# ============================================================
# USER CONTEXT (CACHE + COSMOS)
# ============================================================

async def get_user_context(ctx: Context, email: str = None):

    # Use passed email, fallback to env variable
    if not email:
        email = os.getenv("DEFAULT_USER_EMAIL", "")

    # 1️⃣ L1 cache
    user = get_cache(email)
    if user:
        return user, email

    # 2️⃣ Lock per user
    if email not in LOCKS:
        LOCKS[email] = asyncio.Lock()

    async with LOCKS[email]:

        # double check
        user = get_cache(email)
        if user:
            return user, email

        # 3️⃣ Cosmos (SYNC → THREAD)
        user = await asyncio.to_thread(get_user_permissions, email)

        if not user:
            return None, email

        # 4️⃣ Cache
        set_cache(email, user)

        return user, email


def invalidate_cache(email: str):
    """Remove a user from cache so next call re-fetches from Cosmos."""
    CACHE.pop(email, None)


async def get_user_context_fresh(email: str):
    """Force re-fetch from Cosmos, bypassing cache."""
    invalidate_cache(email)
    user = await asyncio.to_thread(get_user_permissions, email)
    if user:
        set_cache(email, user)
    return user


# ============================================================
# AUTH CHECK
# ============================================================

def check_access(user, product):
    return product in user.get("products", [])


# ============================================================
# Tool: protect_multi
# ============================================================

@mcp.tool()
async def protect_multi(text: str, deny_lists: str, email: str = "", *, ctx: Context) -> dict:

    user, email = await get_user_context(ctx, email)

    if not user or not check_access(user, "pii"):
        # Re-fetch from Cosmos in case cache was stale
        user = await get_user_context_fresh(email)
        if not user or not check_access(user, "pii"):
            return {"status": "unauthorized", "error": "Access Denied: You do not have permission to use PII Protection"}

    deny_dict = json.loads(deny_lists)

    result = apply_multiple_deny_lists(
        text=text,
        deny_lists=deny_dict
    )

    return {
        "original": text,
        "anonymized": result
    }


# ============================================================
# Tool: mistral_ocr
# ============================================================

# @mcp.tool()
# async def mistral_ocr(file_base64: str, mime_type: str, ctx: Context) -> dict:

#     user = await get_user_context(ctx)

#     if not user or not check_access(user, "ocr"):
#         return {"error": "Unauthorized"}

#     result = await process_mistral_ocr(
#         file_base64=file_base64,
#         mime_type=mime_type
#     )

#     return {
#         "status": "success" if "error" not in result else "failed",
#         "data": result
#     }

@mcp.tool()
async def mistral_ocr(file_base64: str, mime_type: str, email: str = "", *, ctx: Context):

    user, email = await get_user_context(ctx, email)

    if not user or not check_access(user, "ocr"):
        # Re-fetch from Cosmos in case cache was stale
        user = await get_user_context_fresh(email)
        if not user or not check_access(user, "ocr"):
            return {"status": "unauthorized", "error": "Access Denied: You do not have permission to use OCR"}

    result = await process_mistral_ocr(file_base64, mime_type)

    if "error" in result:
        return {
            "status": "failed",
            "error": result["error"]
        }

    return result
# ============================================================
# OPTIONAL AUTH TOOL
# ============================================================

@mcp.tool("authorize_user")
async def authorize_user(product: str, ctx: Context):

    user, email = await get_user_context(ctx)

    if not user:
        return {"error": "User not found"}

    return {
        "user": user["email"],
        "product": product,
        "authorized": check_access(user, product)
    }


# ============================================================
# Tool: video_transcribe
# ============================================================

@mcp.tool()
async def video_transcribe(file_base64: str, filename: str = "audio.wav", email: str = "", *, ctx: Context) -> dict:

    user, email = await get_user_context(ctx, email)

    if not user or not check_access(user, "transcription"):
        user = await get_user_context_fresh(email)
        if not user or not check_access(user, "transcription"):
            return {"status": "unauthorized", "error": "Access Denied: You do not have permission to use Video Transcription"}

    try:
        result = await asyncio.to_thread(process_video_input, "base64", file_base64, None, filename)
        return {
            "status": "success",
            "filename": filename,
            "blob_url": result.get("blob_url", ""),
            "speech_job_url": result.get("speech_job_url", "")
        }
    except Exception as e:
        return {"status": "failed", "error": str(e)}


@mcp.tool()
async def transcription_status(job_url: str, email: str = "", *, ctx: Context) -> dict:

    user, email = await get_user_context(ctx, email)

    if not user or not check_access(user, "transcription"):
        user = await get_user_context_fresh(email)
        if not user or not check_access(user, "transcription"):
            return {"status": "unauthorized", "error": "Access Denied"}

    try:
        result = await asyncio.to_thread(get_transcription_result, job_url)
        return {"status": "success", "data": result}
    except Exception as e:
        return {"status": "failed", "error": str(e)}


# ============================================================
# Tool: multi_transcribe (Batch: multiple files from mixed sources)
# ============================================================

@mcp.tool()
async def multi_transcribe(sources_json: str, email: str = "", *, ctx: Context) -> dict:
    """Submit multiple files for batch transcription.
    sources_json: JSON array of source items, each with source_type, data, filename, creds_encrypted.
    """

    user, email = await get_user_context(ctx, email)

    if not user or not check_access(user, "transcription"):
        user = await get_user_context_fresh(email)
        if not user or not check_access(user, "transcription"):
            return {"status": "unauthorized", "error": "Access Denied: You do not have permission to use Transcription"}

    try:
        result = await asyncio.to_thread(process_batch_input, sources_json)
        return {
            "status": "success",
            "files": result.get("files", []),
            "speech_job_url": result.get("speech_job_url", ""),
            "total": result.get("total", 0),
            "uploaded": result.get("uploaded", 0),
            "failed": result.get("failed", 0)
        }
    except Exception as e:
        return {"status": "failed", "error": str(e)}


@mcp.tool()
async def multi_transcription_status(job_url: str, email: str = "", *, ctx: Context) -> dict:
    """Check batch transcription status with per-file results."""

    user, email = await get_user_context(ctx, email)

    if not user or not check_access(user, "transcription"):
        user = await get_user_context_fresh(email)
        if not user or not check_access(user, "transcription"):
            return {"status": "unauthorized", "error": "Access Denied"}

    try:
        result = await asyncio.to_thread(get_batch_transcription_result, job_url)
        return {"status": "success", "data": result}
    except Exception as e:
        return {"status": "failed", "error": str(e)}


@mcp.tool()
async def encrypt_user_secret(plain_text: str, email: str = "", *, ctx: Context) -> dict:
    """Encrypt a secret (e.g. Google creds JSON) so it can be safely passed to multi_transcribe."""

    user, email = await get_user_context(ctx, email)

    if not user or not check_access(user, "transcription"):
        user = await get_user_context_fresh(email)
        if not user or not check_access(user, "transcription"):
            return {"status": "unauthorized", "error": "Access Denied"}

    try:
        encrypted = encrypt_secret(plain_text)
        return {"status": "success", "encrypted": encrypted}
    except Exception as e:
        return {"status": "failed", "error": str(e)}


# ============================================================
# Tool: get_user_permissions (for app.py to fetch on login)
# ============================================================

@mcp.tool()
async def get_permissions(email: str = "", *, ctx: Context) -> dict:
    """Get user permissions from Cosmos DB."""
    user, email = await get_user_context(ctx, email)
    if not user:
        return {"status": "not_found", "email": email, "products": []}
    return {"status": "success", "email": email, "products": user.get("products", [])}


# ============================================================
# Admin Tools (require "admin" product)
# ============================================================

@mcp.tool()
async def admin_list_users(email: str = "", *, ctx: Context) -> dict:
    """List all users and their permissions. Requires admin role."""
    user, email = await get_user_context(ctx, email)
    if not user or not check_access(user, "admin"):
        user = await get_user_context_fresh(email)
        if not user or not check_access(user, "admin"):
            return {"status": "unauthorized", "error": "Admin access required"}
    try:
        users = await asyncio.to_thread(cosmos_list_all_users)
        return {"status": "success", "users": users}
    except Exception as e:
        return {"status": "failed", "error": str(e)}


@mcp.tool()
async def admin_get_products(email: str = "", *, ctx: Context) -> dict:
    """Get all distinct product names across all users. Requires admin role."""
    user, email = await get_user_context(ctx, email)
    if not user or not check_access(user, "admin"):
        user = await get_user_context_fresh(email)
        if not user or not check_access(user, "admin"):
            return {"status": "unauthorized", "error": "Admin access required"}
    try:
        products = await asyncio.to_thread(cosmos_get_all_products)
        return {"status": "success", "products": products}
    except Exception as e:
        return {"status": "failed", "error": str(e)}


@mcp.tool()
async def admin_upsert_user(target_email: str, products_json: str, email: str = "", *, ctx: Context) -> dict:
    """Create or update a user's product permissions. Requires admin role.
    products_json: JSON array of product names, e.g. '["pii","ocr"]'
    """
    user, email = await get_user_context(ctx, email)
    if not user or not check_access(user, "admin"):
        user = await get_user_context_fresh(email)
        if not user or not check_access(user, "admin"):
            return {"status": "unauthorized", "error": "Admin access required"}
    try:
        products = json.loads(products_json)
        result = await asyncio.to_thread(cosmos_upsert_user, target_email, products)
        # Invalidate cache for the target user
        invalidate_cache(target_email)
        return {"status": "success", "result": result.get("status", ""), "email": target_email, "products": products}
    except Exception as e:
        return {"status": "failed", "error": str(e)}


@mcp.tool()
async def admin_delete_user(target_email: str, email: str = "", *, ctx: Context) -> dict:
    """Delete a user from Cosmos DB. Requires admin role."""
    user, email = await get_user_context(ctx, email)
    if not user or not check_access(user, "admin"):
        user = await get_user_context_fresh(email)
        if not user or not check_access(user, "admin"):
            return {"status": "unauthorized", "error": "Admin access required"}
    try:
        result = await asyncio.to_thread(cosmos_delete_user, target_email)
        invalidate_cache(target_email)
        return {"status": "success", "result": result.get("status", ""), "email": target_email}
    except Exception as e:
        return {"status": "failed", "error": str(e)}


# ============================================================
# RUN
# ============================================================

if __name__ == "__main__":
    mcp.run(
        transport="streamable-http",
        host="0.0.0.0",
        port=int(os.environ.get("PORT", "8090")),
        path="/mcp",
        log_level="info"
    )
