# ============================================================
# Streamlit UI for PII Protection and Mistral OCR (FINAL)
# ============================================================

import streamlit as st
import os
from docx import Document
import json
import base64
import traceback
from io import BytesIO
from PIL import Image
from mcp.client.streamable_http import streamablehttp_client
from mcp import ClientSession
import asyncio
from dotenv import load_dotenv
import re
import requests as http_requests
from urllib.parse import urlencode

load_dotenv()

MCP_URL = os.getenv("MCP_URL", "")

# TID OAuth Configuration
TID_CLIENT_ID = os.getenv("TID_CLIENT_ID")
TID_CLIENT_SECRET = os.getenv("TID_CLIENT_SECRET")
TID_AUTH_URL = os.getenv("TID_AUTH_URL")
TID_TOKEN_URL = os.getenv("TID_TOKEN_URL")
TID_USERINFO_URL = os.getenv("TID_USERINFO_URL")
TID_REDIRECT_URI = os.getenv("TID_REDIRECT_URI")
TID_OAUTH_SCOPE = os.getenv("TID_OAUTH_SCOPE")
TID_LOGOUT_URL = os.getenv("TID_LOGOUT_URL")

st.set_page_config(page_title="PII Protection & OCR", layout="wide")
st.title("🔐 PII Protection & OCR Tool")


# ============================================================
# TID AUTHENTICATION HELPERS
# ============================================================
def get_tid_auth_url():
    params = urlencode({
        "scope": TID_OAUTH_SCOPE,
        "response_type": "code",
        "redirect_uri": TID_REDIRECT_URI,
        "client_id": TID_CLIENT_ID,
    })
    return f"{TID_AUTH_URL}?{params}"


def exchange_code_for_token(code):
    resp = http_requests.post(TID_TOKEN_URL, data={
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": TID_REDIRECT_URI,
        "client_id": TID_CLIENT_ID,
        "client_secret": TID_CLIENT_SECRET,
    }, headers={"Content-Type": "application/x-www-form-urlencoded"}, timeout=30)
    resp.raise_for_status()
    return resp.json()


def get_tid_user_info(access_token):
    resp = http_requests.get(TID_USERINFO_URL, headers={
        "Authorization": f"Bearer {access_token}"
    }, timeout=30)
    resp.raise_for_status()
    return resp.json()


def decode_id_token_email(id_token):
    """Extract email from ID token payload (token already obtained server-side)."""
    try:
        payload_part = id_token.split(".")[1]
        # Add padding for base64
        payload_part += "=" * (4 - len(payload_part) % 4)
        payload = json.loads(base64.b64decode(payload_part).decode("utf-8"))
        return payload.get("email", "")
    except Exception:
        return ""


# ============================================================
# MCP CALL
# ============================================================
def call_mcp_tool(tool_name: str, arguments: dict) -> dict:
    async def _call():
        async with streamablehttp_client(MCP_URL) as (read_stream, write_stream, _):
            async with ClientSession(read_stream, write_stream) as session:
                await session.initialize()
                result = await session.call_tool(tool_name, arguments)

                if result.isError:
                    error_msg = ""
                    for content in result.content:
                        if hasattr(content, "text"):
                            error_msg += content.text
                    raise Exception(f"MCP tool error: {error_msg}")

                for content in result.content:
                    if hasattr(content, "text"):
                        return json.loads(content.text)

                return {}

    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(_call())
    finally:
        loop.close()


# ============================================================
# HELPERS
# ============================================================
def extract_text(file):
    if file.name.endswith(".txt"):
        return file.read().decode("utf-8")

    elif file.name.endswith(".docx"):
        doc = Document(file)
        return "\n".join([para.text for para in doc.paragraphs])

    return ""


def compress_image(image_bytes):
    img = Image.open(BytesIO(image_bytes))
    if img.mode in ("RGBA", "P"):
        img = img.convert("RGB")

    output = BytesIO()
    img.save(output, format="JPEG", quality=80)
    return output.getvalue()


def markdown_to_text(md):
    """Convert markdown links to readable text"""
    return re.sub(r'\[([^\]]+)\]\(([^)]+)\)', r'\1 (\2)', md)


# ============================================================
# SESSION STATE
# ============================================================
if "ocr_result" not in st.session_state:
    st.session_state.ocr_result = None

if "pii_result" not in st.session_state:
    st.session_state.pii_result = None

if "pii_uploader_key" not in st.session_state:
    st.session_state.pii_uploader_key = 0

if "ocr_uploader_key" not in st.session_state:
    st.session_state.ocr_uploader_key = 0

if "authenticated" not in st.session_state:
    st.session_state.authenticated = False

if "user_email" not in st.session_state:
    st.session_state.user_email = None

if "user_name" not in st.session_state:
    st.session_state.user_name = None


# ============================================================
# TID AUTHENTICATION FLOW
# ============================================================
query_params = st.query_params

if "code" in query_params and not st.session_state.authenticated:
    try:
        token_data = exchange_code_for_token(query_params["code"])
        if "access_token" in token_data:
            user_info = get_tid_user_info(token_data["access_token"])
            st.session_state.authenticated = True
            st.session_state.user_email = user_info.get("email", "")
            st.session_state.user_name = user_info.get("name", "")
            st.query_params.clear()
            st.rerun()
        elif "id_token" in token_data:
            email = decode_id_token_email(token_data["id_token"])
            st.session_state.authenticated = True
            st.session_state.user_email = email
            st.query_params.clear()
            st.rerun()
        else:
            st.error("Authentication failed: No token received")
    except Exception as e:
        st.error(f"Authentication failed: {e}")

if not st.session_state.authenticated:
    # Auto-redirect to TID login page
    auth_url = get_tid_auth_url()
    st.markdown(
        f'<meta http-equiv="refresh" content="0;url={auth_url}">',
        unsafe_allow_html=True
    )
    st.info("Redirecting to Trimble Identity login...")
    st.stop()

# Sidebar: user info and sign out
with st.sidebar:
    st.markdown(f"👤 **{st.session_state.user_name or st.session_state.user_email}**")
    if st.button("🚪 Sign Out"):
        for key in list(st.session_state.keys()):
            del st.session_state[key]
        logout_url = f"{TID_LOGOUT_URL}?client_id={TID_CLIENT_ID}&post_logout_redirect_uri={TID_REDIRECT_URI}"
        st.markdown(
            f'<meta http-equiv="refresh" content="0;url={logout_url}">',
            unsafe_allow_html=True
        )
        st.stop()


# ============================================================
# TABS
# ============================================================
tab_pii, tab_ocr = st.tabs(["🛡️ PII Protection", "📄 Mistral OCR"])


# ============================================================
# TAB 1: PII
# ============================================================
with tab_pii:
    st.header("Detect & Anonymize PII")

    if st.button("🔄 Refresh", key="refresh_pii"):
        st.session_state.pii_result = None
        st.session_state.pii_uploader_key += 1
        st.rerun()

    pii_file = st.file_uploader("Upload .txt or .docx", type=["txt", "docx"], key=f"pii_upload_{st.session_state.pii_uploader_key}")

    use_deny = st.checkbox("Use Custom Deny List")

    deny_input = st.text_area(
        "Optional Deny List JSON",
        height=120,
        disabled=not use_deny
    )

    if st.button("🚀 Process PII"):

        if not pii_file:
            st.error("Upload a file")
            st.stop()

        text = extract_text(pii_file)

        if not text.strip():
            st.error("Empty file")
            st.stop()

        try:
            deny_dict = json.loads(deny_input) if use_deny and deny_input else {}

            result = call_mcp_tool("protect_multi", {
                "text": text,
                "deny_lists": json.dumps(deny_dict),
                "email": st.session_state.user_email
            })

            st.session_state.pii_result = result

        except Exception as e:
            st.error(str(e))
            st.code(traceback.format_exc())

    # DISPLAY
    if st.session_state.pii_result:
        result = st.session_state.pii_result

        if result.get("status") == "unauthorized":
            st.error("🔒 " + result.get("error", "Access Denied"))
        elif "error" in result and "original" not in result:
            st.error("❌ PII Processing Failed: " + result.get("error", "Unknown error"))
        else:
            col1, col2 = st.columns(2)

            with col1:
                st.subheader("📄 Original")
                st.text_area("", result.get("original", ""), height=400)

            with col2:
                st.subheader("🔒 Anonymized")
                st.text_area("", result.get("anonymized", ""), height=400)

            # DOWNLOADS
            st.download_button("📄 Download Text", result.get("anonymized", ""), "pii.txt")

            st.download_button(
                "📥 Download JSON",
                data=json.dumps(
                    {
                        "anonymized_text": result.get("anonymized", "")
                    },
                    indent=2
                ),
                file_name="pii.json",
                mime="application/json"
            )

# ============================================================
# TAB 2: OCR
# ============================================================
with tab_ocr:
    st.header("Extract Text from Documents")

    if st.button("🔄 Refresh", key="refresh_ocr"):
        st.session_state.ocr_result = None
        st.session_state.ocr_uploader_key += 1
        st.rerun()

    ocr_file = st.file_uploader(
        "Upload PDF / Image / TXT",
        type=["pdf", "png", "jpg", "jpeg", "txt"],
        key=f"ocr_upload_{st.session_state.ocr_uploader_key}"
    )

    if ocr_file:
        col1, col2 = st.columns([1, 2])

        with col1:
            if "image" in ocr_file.type:
                st.image(ocr_file, use_container_width=True)
            else:
                st.info(ocr_file.name)

        with col2:
            if st.button("🚀 Run OCR"):

                try:
                    file_bytes = ocr_file.getvalue()

                    ext = ocr_file.name.split(".")[-1].lower()
                    mime_map = {
                        "pdf": "application/pdf",
                        "txt": "text/plain",
                        "png": "image/png",
                        "jpg": "image/jpeg",
                        "jpeg": "image/jpeg"
                    }

                    mime_type = mime_map.get(ext)

                    if ext in ["png", "jpg", "jpeg"]:
                        file_bytes = compress_image(file_bytes)
                        mime_type = "image/jpeg"

                    b64 = base64.b64encode(file_bytes).decode()

                    result = call_mcp_tool("mistral_ocr", {
                        "file_base64": b64,
                        "mime_type": mime_type,
                        "email": st.session_state.user_email
                    })

                    st.session_state.ocr_result = result

                except Exception as e:
                    st.error(str(e))
                    st.code(traceback.format_exc())

    # DISPLAY RESULT
    if st.session_state.ocr_result:
        result = st.session_state.ocr_result

        if result.get("status") == "success":
            data = result.get("data", {})

            pages = data.get("pages") or data.get("data", {}).get("pages", [])

            st.success("✅ OCR Completed")

            if isinstance(pages, list) and len(pages) > 0:

                for page in pages:
                    with st.expander(f"📄 Page {page.get('index', 0)}", expanded=True):
                        # Render markdown content
                        st.markdown(
                            page.get("markdown", ""),
                            unsafe_allow_html=True
                        )

                        # Render hyperlinks if present as separate field
                        links = page.get("links") or page.get("hyperlinks") or []
                        if links:
                            st.markdown("---")
                            st.markdown("**🔗 Hyperlinks:**")
                            for link in links:
                                if isinstance(link, dict):
                                    text = link.get("text") or link.get("title") or ""
                                    url = link.get("url") or link.get("href") or link.get("uri") or ""
                                    if url:
                                        st.markdown(f"- [{text or url}]({url})", unsafe_allow_html=True)
                                elif isinstance(link, str):
                                    st.markdown(f"- [{link}]({link})", unsafe_allow_html=True)

                # TEXT OUTPUT — full content matching JSON download
                text_output = json.dumps(result, indent=2, default=str)

            else:
                text_output = data.get("text") or data.get("data", {}).get("text", "")
                st.markdown(text_output)

            # DOWNLOADS
            st.download_button("📄 Download Text", text_output, "ocr.txt")

            st.download_button(
                "📥 Download JSON",
                json.dumps(result, indent=2, default=str),
                "ocr.json"
            )

        elif result.get("status") == "unauthorized":
            st.error("🔒 " + result.get("error", "Access Denied"))
        else:
            st.error("❌ OCR Failed: " + result.get("error", "Unknown error"))