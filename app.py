import asyncio
import base64
import hashlib
import hmac
import json
import logging
import os
import smtplib
import time
import zoneinfo
from datetime import datetime, timezone
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

import httpx
from fastapi import FastAPI, Request, Response
from fastapi.responses import HTMLResponse

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------
app = FastAPI(title="Shopify → Monday.com Order Sync")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
MONDAY_API_KEY = os.environ.get("MONDAY_API_KEY", "")
MONDAY_BOARD_ID = os.environ.get("MONDAY_BOARD_ID", "")
MONDAY_SAMPLE_BOARD_ID = os.environ.get("MONDAY_SAMPLE_BOARD_ID", "")

STORES = {
    "semco_pro": {
        "secret": os.environ.get("SHOPIFY_SEMCO_PRO_SECRET", ""),
        "type_label": "SEMCO SURFACE",
    },
    "semco_spaces": {
        "secret": os.environ.get("SHOPIFY_SEMCO_SPACES_SECRET", ""),
        "type_label": "SEMCO SPACES",
    },
    "semco_connect": {
        "secret": os.environ.get("SHOPIFY_SEMCO_CONNECT_SECRET", ""),
        "type_label": "SEMCO CONNECT",
    },
    "semco_works": {
        "secret": os.environ.get("SHOPIFY_SEMCO_WORKS_SECRET", ""),
        "type_label": "SEMCO WORKS",
    },
}

# SEMCOWorks only posts LTL and Will Call orders — all other shipping types are skipped
SEMCO_WORKS_ALLOWED_SHIPPING = {"LTL", "WILL CALL"}

# Email notification settings (Gmail SMTP)
SMTP_EMAIL = os.environ.get("SMTP_EMAIL", "")
SMTP_PASSWORD = os.environ.get("SMTP_PASSWORD", "")
NOTIFY_EMAILS = [e.strip() for e in os.environ.get("NOTIFY_EMAILS", "").split(",") if e.strip()]

MONDAY_API_URL = "https://api.monday.com/v2"

# Column names to match on the board (auto-discovered by display name)
PARENT_COL_NAMES = {
    "time_of_order": "Order Input Time",
    "type": "Type",
    "type_shipment": "Type Shipment",
}
SUBITEM_COL_NAMES = {
    "quantity": "Quantity1",
}

# ---------------------------------------------------------------------------
# Duplicate detection — tracks processed Shopify order IDs in memory
# ---------------------------------------------------------------------------
_processed_orders: dict[str, float] = {}  # {"store:order_id": timestamp}
DEDUP_TTL_SECONDS = 3600  # Keep order IDs for 1 hour


def _is_duplicate(store_key: str, order_id: str | int) -> bool:
    """Check if this order was already processed. Returns True if duplicate."""
    dedup_key = f"{store_key}:{order_id}"

    # Clean up old entries (older than TTL)
    now = time.time()
    expired = [k for k, t in _processed_orders.items() if now - t > DEDUP_TTL_SECONDS]
    for k in expired:
        del _processed_orders[k]

    if dedup_key in _processed_orders:
        return True

    _processed_orders[dedup_key] = now
    return False


# ---------------------------------------------------------------------------
# Column ID cache — auto-discovered from board, refreshed on board ID change
# ---------------------------------------------------------------------------
_column_cache: dict = {
    "board_id": None,
    "parent": {},    # {"time_of_order": "text_mkt3txbf", ...}
    "subitem": {},   # {"quantity": "numeric_mm15nd14", ...}
}


async def _discover_column_ids() -> None:
    """Query the Monday.com board and resolve column IDs by display name."""
    board_id = MONDAY_BOARD_ID

    if _column_cache["board_id"] == board_id and _column_cache["parent"]:
        return  # Already cached for this board

    logger.info("Discovering column IDs for board %s ...", board_id)

    # --- Parent columns ---
    query = """
    query ($boardId: [ID!]) {
        boards(ids: $boardId) {
            columns { id title type settings_str }
        }
    }
    """
    result = await monday_request(query, {"boardId": [board_id]})
    if not result:
        logger.error("COLUMN DISCOVERY FAILED: No response from Monday.com API for board %s", board_id)
        return
    if not result.get("data", {}).get("boards"):
        logger.error("COLUMN DISCOVERY FAILED: Board %s not found or returned no data. Response: %s", board_id, result)
        return

    parent_cols = result["data"]["boards"][0]["columns"]
    logger.info("Found %d parent columns on board %s", len(parent_cols), board_id)
    parent_map: dict[str, str] = {}

    subitem_board_id = None
    for col in parent_cols:
        # Match parent columns by display name
        for key, display_name in PARENT_COL_NAMES.items():
            if col["title"] == display_name:
                parent_map[key] = col["id"]
                logger.info("  MATCHED parent column '%s' → ID: %s (type: %s)", display_name, col["id"], col["type"])

        # Find subitem board ID
        if col["type"] == "subtasks":
            try:
                settings = json.loads(col["settings_str"])
                ids = settings.get("boardIds", [])
                if ids:
                    subitem_board_id = str(ids[0])
                    logger.info("  Found subitem board ID: %s", subitem_board_id)
                else:
                    logger.error("  Subtasks column found but no boardIds in settings: %s", col["settings_str"])
            except (json.JSONDecodeError, KeyError) as e:
                logger.error("  Failed to parse subtasks settings: %s — Error: %s", col["settings_str"], e)

    # Log which parent columns were NOT found
    for key, display_name in PARENT_COL_NAMES.items():
        if key not in parent_map:
            logger.warning("  MISSING parent column '%s' — will skip this field on orders", display_name)

    # --- Subitem columns ---
    subitem_map: dict[str, str] = {}
    if subitem_board_id:
        sub_result = await monday_request(query, {"boardId": [subitem_board_id]})
        if not sub_result:
            logger.error("SUBITEM DISCOVERY FAILED: No response from Monday.com API for subitem board %s", subitem_board_id)
        elif not sub_result.get("data", {}).get("boards"):
            logger.error("SUBITEM DISCOVERY FAILED: Subitem board %s not found. Response: %s", subitem_board_id, sub_result)
        else:
            sub_cols = sub_result["data"]["boards"][0]["columns"]
            logger.info("Found %d subitem columns on subitem board %s", len(sub_cols), subitem_board_id)
            for col in sub_cols:
                for key, display_name in SUBITEM_COL_NAMES.items():
                    if col["title"] == display_name:
                        subitem_map[key] = col["id"]
                        logger.info("  MATCHED subitem column '%s' → ID: %s (type: %s)", display_name, col["id"], col["type"])

            # Log which subitem columns were NOT found
            for key, display_name in SUBITEM_COL_NAMES.items():
                if key not in subitem_map:
                    logger.warning("  MISSING subitem column '%s' — will skip this field on subitems", display_name)
    else:
        logger.error("SUBITEM DISCOVERY FAILED: Could not find subitem board ID for board %s", board_id)

    # Summary
    found_count = len(parent_map) + len(subitem_map)
    total_count = len(PARENT_COL_NAMES) + len(SUBITEM_COL_NAMES)
    if found_count == total_count:
        logger.info("Column discovery complete: ALL %d/%d columns found for board %s", found_count, total_count, board_id)
    else:
        logger.warning("Column discovery complete: %d/%d columns found for board %s — missing columns will be skipped", found_count, total_count, board_id)

    _column_cache["board_id"] = board_id
    _column_cache["parent"] = parent_map
    _column_cache["subitem"] = subitem_map


def get_parent_col(key: str) -> str:
    return _column_cache["parent"].get(key, "")


def get_subitem_col(key: str) -> str:
    return _column_cache["subitem"].get(key, "")


# ---------------------------------------------------------------------------
# Sample board — group and column discovery + inventory tracking
# ---------------------------------------------------------------------------
SAMPLE_LOG_GROUP_NAME = "Sample Requests Log"
SAMPLE_INVENTORY_GROUP_NAME = "Sample Inventory"

# Maps Shopify texture prefix → Monday.com parent item name fragment
TEXTURE_MAP = {
    "corsa": "Corsa/Smooth",
    "vellum": "Vellum/Natural",
    "polished": "Polished",
    "solid": "Solid",
    "grain": "Grain",
    "ada": "ADA",
}

_sample_board_cache: dict = {
    "board_id": None,
    "log_group_id": None,         # Group ID for "Sample Requests Log"
    "subitem_board_id": None,     # Subitem board ID for the sample board
    "subitem_qty_col": None,      # Subitem Quantity column ID (numbers6)
    "subitem_label_col": None,    # Subitem Label column ID (text — "X-BOND Corsa/Smooth — Mojave")
    "subitem_times_col": None,    # Subitem Times Ordered column ID (numbers — running counter)
    "inventory": {},              # {"Corsa/Smooth": {"item_id": "123", "colors": {"rawhide": {...}, ...}}, ...}
}


async def _discover_sample_board() -> None:
    """Discover group ID, subitem columns, and inventory items for the sample board."""
    board_id = MONDAY_SAMPLE_BOARD_ID
    if not board_id:
        return

    if _sample_board_cache["board_id"] == board_id and _sample_board_cache["log_group_id"]:
        return  # Already cached

    logger.info("Discovering sample board structure for board %s ...", board_id)

    # Discover groups and columns
    group_query = """
    query ($boardId: [ID!]) {
        boards(ids: $boardId) {
            groups { id title }
            columns { id title type settings_str }
        }
    }
    """
    result = await monday_request(group_query, {"boardId": [board_id]})
    if not result or not result.get("data", {}).get("boards"):
        logger.error("SAMPLE BOARD DISCOVERY FAILED: Board %s not found. Response: %s", board_id, result)
        return

    board_data = result["data"]["boards"][0]

    # Find groups
    log_group_id = None
    for group in board_data.get("groups", []):
        logger.info("  Found group: '%s' (ID: %s)", group["title"], group["id"])
        if group["title"] == SAMPLE_LOG_GROUP_NAME:
            log_group_id = group["id"]
            logger.info("  MATCHED sample log group '%s' → ID: %s", SAMPLE_LOG_GROUP_NAME, log_group_id)

    if not log_group_id:
        logger.error("SAMPLE BOARD DISCOVERY FAILED: Group '%s' not found on board %s", SAMPLE_LOG_GROUP_NAME, board_id)

    # Find subitem board and columns (Quantity, Label, Times Ordered)
    subitem_qty_col = None
    subitem_label_col = None
    subitem_times_col = None
    subitem_board_id = None
    for col in board_data.get("columns", []):
        if col["type"] == "subtasks":
            try:
                settings = json.loads(col["settings_str"])
                ids = settings.get("boardIds", [])
                if ids:
                    subitem_board_id = str(ids[0])
                    logger.info("  Found sample subitem board ID: %s", subitem_board_id)
            except (json.JSONDecodeError, KeyError):
                pass

    if subitem_board_id:
        col_query = """
        query ($boardId: [ID!]) {
            boards(ids: $boardId) {
                columns { id title type }
            }
        }
        """
        sub_result = await monday_request(col_query, {"boardId": [subitem_board_id]})
        if sub_result and sub_result.get("data", {}).get("boards"):
            for col in sub_result["data"]["boards"][0]["columns"]:
                if col["title"] == "Quantity":
                    subitem_qty_col = col["id"]
                    logger.info("  MATCHED sample subitem column 'Quantity' → ID: %s", subitem_qty_col)
                elif col["title"] == "Label":
                    subitem_label_col = col["id"]
                    logger.info("  MATCHED sample subitem column 'Label' → ID: %s", subitem_label_col)
                elif col["title"] == "Times Ordered":
                    subitem_times_col = col["id"]
                    logger.info("  MATCHED sample subitem column 'Times Ordered' → ID: %s", subitem_times_col)

    # Discover inventory items and their color subitems
    inventory: dict = {}
    labels_to_populate: list[dict] = []  # Subitems that need their Label column set

    items_query = """
    query ($boardId: [ID!]) {
        boards(ids: $boardId) {
            items_page(limit: 50) {
                items {
                    id name
                    group { title }
                    subitems {
                        id name
                        column_values { id text }
                    }
                }
            }
        }
    }
    """
    items_result = await monday_request(items_query, {"boardId": [board_id]})
    if items_result and items_result.get("data", {}).get("boards"):
        for item in items_result["data"]["boards"][0]["items_page"]["items"]:
            group_title = item.get("group", {}).get("title", "")
            if group_title != SAMPLE_INVENTORY_GROUP_NAME:
                continue

            # Extract texture from item name (e.g., "Flex Samples - X-BOND Corsa/Smooth" → "Corsa/Smooth")
            item_name = item["name"]
            for texture_key, texture_label in TEXTURE_MAP.items():
                if texture_label.lower() in item_name.lower():
                    colors: dict = {}
                    for sub in item.get("subitems", []):
                        # Get current quantity and label from column values
                        current_qty = 0
                        current_label = ""
                        current_times = 0
                        for cv in sub.get("column_values", []):
                            if cv["id"] == subitem_qty_col and cv.get("text"):
                                try:
                                    current_qty = int(float(cv["text"]))
                                except (ValueError, TypeError):
                                    pass
                            elif cv["id"] == subitem_label_col:
                                current_label = (cv.get("text") or "").strip()
                            elif cv["id"] == subitem_times_col and cv.get("text"):
                                try:
                                    current_times = int(float(cv["text"]))
                                except (ValueError, TypeError):
                                    pass

                        # Build the expected label: "X-BOND Corsa/Smooth — Mojave"
                        expected_label = f"{item_name} — {sub['name']}"

                        colors[sub["name"].strip().lower()] = {
                            "id": sub["id"],
                            "name": sub["name"],
                            "quantity": current_qty,
                            "times_ordered": current_times,
                        }

                        # Queue label population if empty or different
                        if subitem_label_col and current_label != expected_label:
                            labels_to_populate.append({
                                "subitem_id": sub["id"],
                                "label": expected_label,
                            })

                    inventory[texture_label] = {
                        "item_id": item["id"],
                        "item_name": item_name,
                        "colors": colors,
                    }
                    logger.info("  INVENTORY: '%s' (ID: %s) — %d colors", item_name, item["id"], len(colors))
                    break

    _sample_board_cache["board_id"] = board_id
    _sample_board_cache["log_group_id"] = log_group_id
    _sample_board_cache["subitem_board_id"] = subitem_board_id
    _sample_board_cache["subitem_qty_col"] = subitem_qty_col
    _sample_board_cache["subitem_label_col"] = subitem_label_col
    _sample_board_cache["subitem_times_col"] = subitem_times_col
    _sample_board_cache["inventory"] = inventory
    logger.info("Sample board discovery complete: log_group=%s, subitem_board=%s, qty_col=%s, label_col=%s, times_col=%s, inventory_items=%d",
                log_group_id, subitem_board_id, subitem_qty_col, subitem_label_col, subitem_times_col, len(inventory))

    # Populate Label column for any subitems that are missing it
    if labels_to_populate and subitem_board_id and subitem_label_col:
        logger.info("Populating Label column for %d subitems...", len(labels_to_populate))
        for entry in labels_to_populate:
            update_query = """
            mutation ($boardId: ID!, $itemId: ID!, $columnId: String!, $value: JSON!) {
                change_column_value(
                    board_id: $boardId,
                    item_id: $itemId,
                    column_id: $columnId,
                    value: $value
                ) { id }
            }
            """
            await monday_request(update_query, {
                "boardId": subitem_board_id,
                "itemId": entry["subitem_id"],
                "columnId": subitem_label_col,
                "value": json.dumps(entry["label"]),
            })
        logger.info("Label column populated for %d subitems", len(labels_to_populate))


async def _decrement_sample_inventory(color: str, texture: str, quantity: int, order_name: str) -> None:
    """Decrement the inventory count for a specific color+texture on the sample board.

    Args:
        color: Color name (e.g., "Rawhide", "Polar Bear")
        texture: Texture label matching TEXTURE_MAP values (e.g., "Corsa/Smooth", "Vellum/Natural")
        quantity: Amount to subtract
        order_name: For logging
    """
    inventory = _sample_board_cache.get("inventory", {})
    qty_col = _sample_board_cache.get("subitem_qty_col")

    if not inventory or not qty_col:
        logger.warning("INVENTORY DECREMENT SKIPPED for %s/%s — inventory not loaded", texture, color)
        return

    texture_data = inventory.get(texture)
    if not texture_data:
        logger.warning("INVENTORY DECREMENT SKIPPED: Texture '%s' not found on sample board (order %s)", texture, order_name)
        return

    color_key = color.strip().lower()
    color_data = texture_data["colors"].get(color_key)
    if not color_data:
        logger.warning("INVENTORY DECREMENT SKIPPED: Color '%s' not found under '%s' (order %s)", color, texture, order_name)
        return

    # Read current values from Monday.com (fresh read to avoid stale cache)
    times_col = _sample_board_cache.get("subitem_times_col")
    read_query = """
    query ($itemId: [ID!]) {
        items(ids: $itemId) {
            column_values { id text }
        }
    }
    """
    read_result = await monday_request(read_query, {"itemId": [color_data["id"]]})
    current_qty = color_data["quantity"]  # Fallback to cached value
    current_times = color_data.get("times_ordered", 0)
    if read_result and read_result.get("data", {}).get("items"):
        for cv in read_result["data"]["items"][0].get("column_values", []):
            if cv["id"] == qty_col and cv.get("text"):
                try:
                    current_qty = int(float(cv["text"]))
                except (ValueError, TypeError):
                    pass
            elif times_col and cv["id"] == times_col and cv.get("text"):
                try:
                    current_times = int(float(cv["text"]))
                except (ValueError, TypeError):
                    pass

    new_qty = max(0, current_qty - quantity)
    new_times = current_times + quantity

    # Update the subitem quantity and times ordered
    subitem_board_id = _sample_board_cache.get("subitem_board_id")
    if not subitem_board_id:
        logger.error("INVENTORY DECREMENT FAILED: Could not find subitem board ID")
        return

    update_query = """
    mutation ($boardId: ID!, $itemId: ID!, $columnId: String!, $value: JSON!) {
        change_column_value(
            board_id: $boardId,
            item_id: $itemId,
            column_id: $columnId,
            value: $value
        ) {
            id
        }
    }
    """
    result = await monday_request(update_query, {
        "boardId": subitem_board_id,
        "itemId": color_data["id"],
        "columnId": qty_col,
        "value": json.dumps(str(new_qty)),
    })

    if result:
        logger.info("INVENTORY DECREMENTED: %s / %s: %d → %d (-%d) for order %s",
                     texture, color_data["name"], current_qty, new_qty, quantity, order_name)
        color_data["quantity"] = new_qty
    else:
        logger.error("INVENTORY DECREMENT FAILED: %s / %s for order %s", texture, color, order_name)

    # Increment Times Ordered counter
    if times_col and subitem_board_id:
        times_result = await monday_request(update_query, {
            "boardId": subitem_board_id,
            "itemId": color_data["id"],
            "columnId": times_col,
            "value": json.dumps(str(new_times)),
        })
        if times_result:
            logger.info("TIMES ORDERED INCREMENTED: %s / %s: %d → %d (+%d) for order %s",
                         texture, color_data["name"], current_times, new_times, quantity, order_name)
            color_data["times_ordered"] = new_times
        else:
            logger.error("TIMES ORDERED INCREMENT FAILED: %s / %s for order %s", texture, color, order_name)


# ---------------------------------------------------------------------------
# Shopify HMAC verification
# ---------------------------------------------------------------------------

def verify_hmac(body: bytes, secret: str, header_hmac: str) -> bool:
    computed = base64.b64encode(
        hmac.new(secret.encode("utf-8"), body, hashlib.sha256).digest()
    ).decode("utf-8")
    return hmac.compare_digest(computed, header_hmac)

# ---------------------------------------------------------------------------
# Email notifications
# ---------------------------------------------------------------------------

def _send_email(subject: str, body_html: str) -> None:
    """Send an email notification via Gmail SMTP. Runs synchronously but is called from background tasks."""
    if not SMTP_EMAIL or not SMTP_PASSWORD or not NOTIFY_EMAILS:
        logger.debug("Email notifications not configured — skipping")
        return

    try:
        msg = MIMEMultipart("alternative")
        msg["From"] = SMTP_EMAIL
        msg["To"] = ", ".join(NOTIFY_EMAILS)
        msg["Subject"] = subject
        msg.attach(MIMEText(body_html, "html"))

        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(SMTP_EMAIL, SMTP_PASSWORD)
            server.sendmail(SMTP_EMAIL, NOTIFY_EMAILS, msg.as_string())

        logger.info("Email notification sent: %s", subject)
    except Exception as e:
        logger.error("Failed to send email notification: %s — %s", type(e).__name__, e)


def send_success_email(store_key: str, order_name: str, item_name: str, subitems: list[str], shipment_type: str | None) -> None:
    """Send a success notification for a posted order."""
    store_label = STORES.get(store_key, {}).get("type_label", store_key)
    now = datetime.now(zoneinfo.ZoneInfo("America/Los_Angeles")).strftime("%B %d, %Y at %I:%M %p PT")

    subitems_html = "".join(f"<li>{s}</li>" for s in subitems)

    body = f"""
    <div style="font-family: Arial, sans-serif; max-width: 600px;">
        <h2 style="color: #2e7d32;">✅ Order Posted Successfully</h2>
        <table style="border-collapse: collapse; width: 100%;">
            <tr><td style="padding: 8px; font-weight: bold;">Store:</td><td style="padding: 8px;">{store_label}</td></tr>
            <tr><td style="padding: 8px; font-weight: bold;">Order:</td><td style="padding: 8px;">{order_name}</td></tr>
            <tr><td style="padding: 8px; font-weight: bold;">Item Name:</td><td style="padding: 8px;">{item_name}</td></tr>
            <tr><td style="padding: 8px; font-weight: bold;">Shipment:</td><td style="padding: 8px;">{shipment_type or 'N/A'}</td></tr>
            <tr><td style="padding: 8px; font-weight: bold;">Time:</td><td style="padding: 8px;">{now}</td></tr>
        </table>
        <h3>Subitems Created:</h3>
        <ul>{subitems_html}</ul>
    </div>
    """
    subject = f"✅ Order Posted — {store_label} — {order_name}"
    _send_email(subject, body)


def send_failure_email(store_key: str, order_name: str, error_details: str, context: str = "") -> None:
    """Send a failure notification when an order fails to post."""
    store_label = STORES.get(store_key, {}).get("type_label", store_key)
    now = datetime.now(zoneinfo.ZoneInfo("America/Los_Angeles")).strftime("%B %d, %Y at %I:%M %p PT")

    context_html = f"<tr><td style='padding: 8px; font-weight: bold;'>Context:</td><td style='padding: 8px;'>{context}</td></tr>" if context else ""

    body = f"""
    <div style="font-family: Arial, sans-serif; max-width: 600px;">
        <h2 style="color: #c62828;">⚠️ Order Failed to Post</h2>
        <table style="border-collapse: collapse; width: 100%;">
            <tr><td style="padding: 8px; font-weight: bold;">Store:</td><td style="padding: 8px;">{store_label}</td></tr>
            <tr><td style="padding: 8px; font-weight: bold;">Order:</td><td style="padding: 8px;">{order_name}</td></tr>
            <tr><td style="padding: 8px; font-weight: bold;">Time:</td><td style="padding: 8px;">{now}</td></tr>
            {context_html}
        </table>
        <h3>Error Details:</h3>
        <pre style="background: #f5f5f5; padding: 12px; border-radius: 4px; overflow-x: auto; font-size: 13px;">{error_details}</pre>
        <p style="color: #666; font-size: 12px;">Check Render logs for full details.</p>
    </div>
    """
    subject = f"⚠️ Order FAILED — {store_label} — {order_name}"
    _send_email(subject, body)


# ---------------------------------------------------------------------------
# Monday.com helpers
# ---------------------------------------------------------------------------

async def monday_request(query: str, variables: dict) -> dict | None:
    headers = {
        "Authorization": MONDAY_API_KEY,
        "Content-Type": "application/json",
    }
    payload = {"query": query, "variables": variables}

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(MONDAY_API_URL, headers=headers, json=payload)
            data = resp.json()
    except httpx.TimeoutException:
        logger.error("Monday.com API request TIMED OUT after 30s. Query: %s", query[:100])
        return None
    except httpx.HTTPError as e:
        logger.error("Monday.com API HTTP error: %s", e)
        return None
    except Exception as e:
        logger.error("Monday.com API unexpected error: %s — %s", type(e).__name__, e)
        return None

    if "errors" in data:
        logger.error("Monday.com API returned errors: %s | Query: %s | Variables: %s", data["errors"], query[:100], variables)
        return None

    if resp.status_code != 200:
        logger.error("Monday.com API returned status %d: %s", resp.status_code, data)
        return None

    return data


async def create_parent_item(item_name: str, column_values: dict) -> str | None:
    query = """
    mutation ($boardId: ID!, $itemName: String!, $columnValues: JSON!) {
        create_item(
            board_id: $boardId,
            item_name: $itemName,
            column_values: $columnValues
        ) {
            id
        }
    }
    """
    col_values_json = json.dumps(column_values)
    variables = {
        "boardId": MONDAY_BOARD_ID,
        "itemName": item_name,
        "columnValues": col_values_json,
    }
    logger.info("Creating parent item: '%s' with columns: %s", item_name, col_values_json)
    result = await monday_request(query, variables)
    if result and "data" in result:
        try:
            item_id = result["data"]["create_item"]["id"]
            logger.info("SUCCESS: Created parent item '%s' (ID: %s)", item_name, item_id)
            return item_id
        except (KeyError, TypeError) as e:
            logger.error("FAILED: Unexpected response structure creating parent item '%s': %s — Response: %s", item_name, e, result)
            return None
    else:
        logger.error("FAILED: Could not create parent item '%s'. Result: %s", item_name, result)
        return None


async def create_subitem(parent_item_id: str, item_name: str, column_values: dict) -> str | None:
    query = """
    mutation ($parentItemId: ID!, $itemName: String!, $columnValues: JSON!) {
        create_subitem(
            parent_item_id: $parentItemId,
            item_name: $itemName,
            column_values: $columnValues
        ) {
            id
        }
    }
    """
    col_values_json = json.dumps(column_values)
    variables = {
        "parentItemId": parent_item_id,
        "itemName": item_name,
        "columnValues": col_values_json,
    }
    logger.info("Creating subitem: '%s' under parent %s with columns: %s", item_name, parent_item_id, col_values_json)
    result = await monday_request(query, variables)
    if result and "data" in result:
        try:
            sub_id = result["data"]["create_subitem"]["id"]
            logger.info("SUCCESS: Created subitem '%s' (ID: %s) under parent %s", item_name, sub_id, parent_item_id)
            return sub_id
        except (KeyError, TypeError) as e:
            logger.error("FAILED: Unexpected response structure creating subitem '%s': %s — Response: %s", item_name, e, result)
            return None
    else:
        logger.error("FAILED: Could not create subitem '%s' under parent %s. Result: %s", item_name, parent_item_id, result)
        return None


async def create_update(item_id: str, body_text: str) -> bool:
    """Create an update (comment bubble) on a Monday.com item."""
    query = """
    mutation ($itemId: ID!, $body: String!) {
        create_update(
            item_id: $itemId,
            body: $body
        ) {
            id
        }
    }
    """
    variables = {"itemId": item_id, "body": body_text}
    logger.info("Creating update on item %s: '%s'", item_id, body_text[:100])
    result = await monday_request(query, variables)
    if result and "data" in result:
        logger.info("SUCCESS: Created update on item %s", item_id)
        return True
    else:
        logger.error("FAILED: Could not create update on item %s. Result: %s", item_id, result)
        return False


async def create_item_in_group(board_id: str, group_id: str, item_name: str, column_values: dict | None = None) -> str | None:
    """Create an item in a specific group on a Monday.com board."""
    query = """
    mutation ($boardId: ID!, $groupId: String!, $itemName: String!, $columnValues: JSON!) {
        create_item(
            board_id: $boardId,
            group_id: $groupId,
            item_name: $itemName,
            column_values: $columnValues
        ) {
            id
        }
    }
    """
    col_values_json = json.dumps(column_values or {})
    variables = {
        "boardId": board_id,
        "groupId": group_id,
        "itemName": item_name,
        "columnValues": col_values_json,
    }
    logger.info("Creating item in group: '%s' on board %s, group %s", item_name, board_id, group_id)
    result = await monday_request(query, variables)
    if result and "data" in result:
        try:
            item_id = result["data"]["create_item"]["id"]
            logger.info("SUCCESS: Created item '%s' (ID: %s) in group %s", item_name, item_id, group_id)
            return item_id
        except (KeyError, TypeError) as e:
            logger.error("FAILED: Unexpected response creating item '%s' in group: %s — Response: %s", item_name, e, result)
            return None
    else:
        logger.error("FAILED: Could not create item '%s' in group %s. Result: %s", item_name, group_id, result)
        return None


async def log_sample_order(order: dict, store_key: str) -> None:
    """Log a sample order to the Sample Requests Log group on the sample inventory board."""
    if not MONDAY_SAMPLE_BOARD_ID:
        logger.debug("MONDAY_SAMPLE_BOARD_ID not set — skipping sample order logging")
        return

    await _discover_sample_board()

    log_group_id = _sample_board_cache.get("log_group_id")
    if not log_group_id:
        logger.error("Cannot log sample order — Sample Requests Log group not found on board %s", MONDAY_SAMPLE_BOARD_ID)
        return

    order_name = order.get("name", "Unknown")
    contact = extract_contact_name(order)
    company = extract_company_name(order)
    if company:
        item_name = f"{contact} / {company} / Order{order_name}"
    else:
        item_name = f"{contact} / Order{order_name}"

    # Create parent item in the log group
    parent_id = await create_item_in_group(MONDAY_SAMPLE_BOARD_ID, log_group_id, item_name)
    if not parent_id:
        logger.error("FAILED to log sample order %s to sample board", order_name)
        return

    # Create subitems for each sample line item with quantities
    line_items = order.get("line_items") or []
    subitem_qty_col = _sample_board_cache.get("subitem_qty_col")

    for li in line_items:
        title = (li.get("title") or "").strip()
        # Only log sample items
        if not any(sample in title.lower() for sample in SAMPLE_PRODUCT_NAMES):
            continue

        variant_title = (li.get("variant_title") or "").strip()

        # Parse texture and color based on store
        # Pro format: variant_title = "Corsa / Polar Bear" → texture=Corsa/Smooth, color=Polar Bear
        # Spaces format: variant_title = "Phantom" → texture=Corsa/Smooth (always), color=Phantom
        if store_key == "semco_pro" and " / " in variant_title:
            parts_split = variant_title.split(" / ", 1)
            texture_prefix = parts_split[0].strip().lower()
            color_name = parts_split[1].strip()
            # Map Shopify texture prefix to Monday.com texture label
            texture_label = None
            for key, label in TEXTURE_MAP.items():
                if key in texture_prefix:
                    texture_label = label
                    break
            if not texture_label:
                logger.warning("Unknown texture prefix '%s' from Pro sample — defaulting to Corsa/Smooth", texture_prefix)
                texture_label = "Corsa/Smooth"
        else:
            # Spaces: always Corsa/Smooth, variant_title is the color
            texture_label = "Corsa/Smooth"
            color_name = variant_title

        # Use the same color expansion logic as regular orders (handles both Pro and Spaces formats)
        expanded = _expand_line_item_colors(li)
        for item in expanded:
            parts = [p for p in [item["title"], item["variant_title"], item["color"]] if p]
            subitem_name = " - ".join(parts)

            sub_columns: dict = {}
            if subitem_qty_col:
                sub_columns[subitem_qty_col] = str(item["quantity"])

            await create_subitem(parent_id, subitem_name, sub_columns)

            # Decrement inventory — use parsed color or expanded color
            decrement_color = item["color"] if item["color"] else color_name
            if decrement_color:
                await _decrement_sample_inventory(decrement_color, texture_label, item["quantity"], order_name)

    logger.info("SAMPLE LOG: Order %s logged to Sample Requests Log on board %s", order_name, MONDAY_SAMPLE_BOARD_ID)


# ---------------------------------------------------------------------------
# Order parsing helpers
# ---------------------------------------------------------------------------

def extract_contact_name(order: dict) -> str:
    for key in ("shipping_address", "billing_address"):
        addr = order.get(key)
        if addr:
            first = (addr.get("first_name") or "").strip()
            last = (addr.get("last_name") or "").strip()
            if first or last:
                name = f"{first} {last}".strip()
                logger.info("Contact name resolved from %s: '%s'", key, name)
                return name

    customer = order.get("customer")
    if customer:
        first = (customer.get("first_name") or "").strip()
        last = (customer.get("last_name") or "").strip()
        if first or last:
            name = f"{first} {last}".strip()
            logger.info("Contact name resolved from customer: '%s'", name)
            return name

    logger.warning("No contact name found — using 'Unknown Contact'")
    return "Unknown Contact"


def extract_company_name(order: dict) -> str:
    for key in ("shipping_address", "billing_address"):
        addr = order.get(key)
        if addr:
            company = (addr.get("company") or "").strip()
            if company:
                logger.info("Company name resolved from %s: '%s'", key, company)
                return company

    logger.info("No company name found — skipping company in item name")
    return ""


def map_shipping_type(order: dict) -> str | None:
    shipping_lines = order.get("shipping_lines") or []
    if not shipping_lines:
        logger.info("No shipping lines in order — leaving Type Shipment empty")
        return None

    for line in shipping_lines:
        title = (line.get("title") or "").lower()
        code = (line.get("code") or "").lower()
        combined = f"{title} {code}"
        logger.info("Checking shipping line: title='%s' code='%s'", line.get("title"), line.get("code"))

        if "ups" in combined or "flat rate" in combined or "economy" in combined:
            logger.info("Shipping type mapped to: UPS")
            return "UPS"
        if "ltl" in combined or "r + l" in combined or "r+l" in combined:
            logger.info("Shipping type mapped to: LTL")
            return "LTL"
        if "will call" in combined or "pickup" in combined:
            logger.info("Shipping type mapped to: WILL CALL")
            return "WILL CALL"

    # If shipping lines exist but don't match known carriers, it's a will call address
    logger.info("Shipping line didn't match UPS/LTL — defaulting to WILL CALL. Lines: %s", shipping_lines)
    return "WILL CALL"

# ---------------------------------------------------------------------------
# Order processing
# ---------------------------------------------------------------------------

# Sample product names to filter out (case-insensitive)
SAMPLE_PRODUCT_NAMES = [
    "architectural sample kits",           # SEMCO Pro
    "x-bond microcement physical color samples",  # SEMCO Spaces
]


def _expand_line_item_colors(li: dict) -> list[dict]:
    """Expand a single line item into multiple entries if it has multiple color properties.

    Returns a list of dicts with keys: title, variant_title, color, quantity.

    Two known property formats:
      Spaces: [{'name': 'Color', 'value': 'Charcoal Grey'}]           → single color
      Pro:    [{'name': 'Mojave', 'value': '3'}, {'name': 'Phantom', 'value': '4'}] → multi color
    """
    product_title = li.get("title", "Unknown Product")
    variant_title = (li.get("variant_title") or "").strip()
    total_quantity = li.get("quantity", 1)
    properties = li.get("properties") or []

    # Check for Spaces format first (single property named "Color")
    for prop in properties:
        if (prop.get("name") or "").strip().lower() == "color":
            color = (prop.get("value") or "").strip()
            return [{"title": product_title, "variant_title": variant_title, "color": color, "quantity": total_quantity}]

    # Check for Pro format: name is the color, value is the quantity (digit)
    color_entries = []
    for prop in properties:
        prop_name = (prop.get("name") or "").strip()
        prop_value = (prop.get("value") or "").strip()
        if prop_name and prop_value.isdigit():
            color_entries.append({"title": product_title, "variant_title": variant_title, "color": prop_name, "quantity": int(prop_value)})

    if color_entries:
        return color_entries

    # No color properties — return as-is
    return [{"title": product_title, "variant_title": variant_title, "color": "", "quantity": total_quantity}]


def _is_sample_only_order(order: dict) -> bool:
    """Check if the order contains ONLY sample items. Returns True if so."""
    line_items = order.get("line_items") or []
    if not line_items:
        return False

    for li in line_items:
        title = (li.get("title") or "").lower()
        is_sample = any(sample in title for sample in SAMPLE_PRODUCT_NAMES)
        if not is_sample:
            return False  # Found a non-sample item — order should be posted

    return True  # All items are samples


async def process_order(order: dict, store_key: str) -> None:
    # Ensure column IDs are discovered
    await _discover_column_ids()

    store = STORES[store_key]
    order_name = order.get("name", "Unknown")

    logger.info("=" * 60)
    logger.info("PROCESSING ORDER: %s from store: %s", order_name, store_key)
    logger.info("=" * 60)

    # Filter out sample-only orders
    if _is_sample_only_order(order):
        logger.info("SKIPPING ORDER %s: Contains only sample items — not posting to order board", order_name)
        # Log sample orders from Pro and Spaces to the Sample Requests Log board
        if store_key in ("semco_pro", "semco_spaces"):
            await log_sample_order(order, store_key)
        return

    # Mixed orders (samples + regular products): post full order to orders board AND
    # log just the sample items to the sample inventory board
    if store_key in ("semco_pro", "semco_spaces"):
        has_samples = any(
            any(sample in (li.get("title") or "").lower() for sample in SAMPLE_PRODUCT_NAMES)
            for li in (order.get("line_items") or [])
        )
        if has_samples:
            logger.info("Mixed order %s: contains samples + regular items — logging samples to sample board", order_name)
            await log_sample_order(order, store_key)

    # SEMCOWorks: only post orders with explicit LTL or Will Call shipping — skip everything else
    # Uses strict matching (no default fallback) so "Free Ground Shipping" etc. are skipped
    if store_key == "semco_works":
        shipping_lines = order.get("shipping_lines") or []
        is_ltl_or_willcall = False
        for line in shipping_lines:
            title = (line.get("title") or "").lower()
            code = (line.get("code") or "").lower()
            combined = f"{title} {code}"
            if "ltl" in combined or "r + l" in combined or "r+l" in combined:
                is_ltl_or_willcall = True
                break
            if "will call" in combined or "pickup" in combined:
                is_ltl_or_willcall = True
                break
        if not is_ltl_or_willcall:
            ship_titles = [l.get("title", "unknown") for l in shipping_lines]
            logger.info("SKIPPING ORDER %s from semco_works: Shipping '%s' is not explicitly LTL or WILL CALL — skipped", order_name, ship_titles)
            return

    contact = extract_contact_name(order)
    company = extract_company_name(order)
    if company:
        item_name = f"{contact} / {company} / Order{order_name}"
    else:
        item_name = f"{contact} / Order{order_name}"
    logger.info("Parent item name: '%s'", item_name)

    # Build parent column values — skip missing columns
    now = datetime.now(zoneinfo.ZoneInfo("America/Los_Angeles"))
    timestamp_str = now.strftime("%H:%M")

    column_values: dict = {}

    col_time = get_parent_col("time_of_order")
    if col_time:
        column_values[col_time] = timestamp_str
        logger.info("Setting 'Time of Order' → '%s'", timestamp_str)
    else:
        logger.warning("SKIPPING 'Time of Order' — column not found on board")

    col_type = get_parent_col("type")
    if col_type:
        column_values[col_type] = {"label": store["type_label"]}
        logger.info("Setting 'Type' → '%s'", store["type_label"])
    else:
        logger.warning("SKIPPING 'Type' — column not found on board")

    shipment_type = map_shipping_type(order)
    if shipment_type:
        col_shipment = get_parent_col("type_shipment")
        if col_shipment:
            column_values[col_shipment] = {"label": shipment_type}
            logger.info("Setting 'Type Shipment' → '%s'", shipment_type)
        else:
            logger.warning("SKIPPING 'Type Shipment' — column not found on board (value would have been '%s')", shipment_type)

    # Create parent item
    parent_id = await create_parent_item(item_name, column_values)
    if not parent_id:
        logger.error("ABORTING ORDER %s: Failed to create parent item — subitems will not be created", order_name)
        send_failure_email(store_key, order_name, "Failed to create parent item on Monday.com", context=f"Item name: {item_name}")
        return

    # Create subitems for each line item
    line_items = order.get("line_items") or []
    logger.info("Processing %d line items for order %s", len(line_items), order_name)

    # Expand line items — a single line item with multiple color properties
    # gets split into separate subitems (e.g., 3 Mojave + 4 Phantom)
    expanded_items: list[dict] = []
    for li in line_items:
        logger.info("DEBUG line_item properties: %s", li.get("properties", []))
        expanded = _expand_line_item_colors(li)
        if len(expanded) > 1:
            logger.info("Expanded line item '%s' into %d color variants", li.get("title"), len(expanded))
        expanded_items.extend(expanded)

    logger.info("Total subitems to create: %d (from %d line items)", len(expanded_items), len(line_items))

    success_count = 0
    fail_count = 0
    created_subitems: list[str] = []
    failed_subitems: list[str] = []
    for i, item in enumerate(expanded_items, 1):
        # Build subitem name: Title - Variant - Color (skip empty parts)
        parts = [item["title"]]
        if item["variant_title"]:
            parts.append(item["variant_title"])
        if item["color"]:
            parts.append(item["color"])
        subitem_name = " - ".join(parts)

        quantity = item["quantity"]
        subitem_display = f"{subitem_name} x{quantity}"
        logger.info("Subitem %d/%d: '%s' x%d", i, len(expanded_items), subitem_name, quantity)

        sub_columns: dict = {}
        col_qty = get_subitem_col("quantity")
        if col_qty:
            sub_columns[col_qty] = str(quantity)
        else:
            logger.warning("SKIPPING 'Quantity1' for subitem '%s' — column not found on board", subitem_name)

        result = await create_subitem(parent_id, subitem_name, sub_columns)
        if result:
            success_count += 1
            created_subitems.append(subitem_display)
        else:
            fail_count += 1
            failed_subitems.append(subitem_display)

    logger.info("=" * 60)
    logger.info("ORDER %s COMPLETE: %d/%d subitems created successfully", order_name, success_count, len(expanded_items))
    if fail_count > 0:
        logger.error("ORDER %s: %d subitem(s) FAILED to create", order_name, fail_count)
    logger.info("=" * 60)

    # Email notifications
    if fail_count == 0:
        send_success_email(store_key, order_name, item_name, created_subitems, shipment_type)
    else:
        failed_list = "\n".join(failed_subitems)
        send_failure_email(
            store_key, order_name,
            f"{fail_count}/{len(expanded_items)} subitems failed to create:\n{failed_list}",
            context=f"Parent item '{item_name}' was created (ID: {parent_id}), but some subitems failed",
        )

    # SEMCO Pro LTL orders: add SKU summary update bubble
    if store_key == "semco_pro" and shipment_type == "LTL":
        sku_counts: dict[str, int] = {}
        for li in line_items:
            sku = (li.get("sku") or "").strip()
            qty = li.get("quantity", 1)
            if sku:
                sku_counts[sku] = sku_counts.get(sku, 0) + qty
        if sku_counts:
            sku_parts = [f"{qty}: {sku}" for sku, qty in sku_counts.items()]
            update_text = ", ".join(sku_parts) + "\nWater Based Building Products"
            logger.info("Adding SKU summary update for Pro LTL order %s: %s", order_name, update_text)
            await create_update(parent_id, update_text)

# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get("/health")
async def health():
    return {
        "status": "ok",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


async def _fetch_sample_inventory_data() -> list[dict] | None:
    """Query the sample board and return a flat list of all inventory color items."""
    if not MONDAY_SAMPLE_BOARD_ID:
        return None

    board_id = MONDAY_SAMPLE_BOARD_ID
    query = """
    query ($boardId: [ID!]) {
        boards(ids: $boardId) {
            items_page(limit: 50) {
                items {
                    id name
                    group { title }
                    subitems {
                        id name
                        column_values { id text }
                    }
                }
            }
        }
    }
    """
    result = await monday_request(query, {"boardId": [board_id]})
    if not result or not result.get("data", {}).get("boards"):
        return None

    await _discover_sample_board()
    qty_col = _sample_board_cache.get("subitem_qty_col")
    times_col = _sample_board_cache.get("subitem_times_col")
    logger.info("DASHBOARD: qty_col=%s, times_col=%s", qty_col, times_col)

    all_items: list[dict] = []
    board_items = result["data"]["boards"][0]["items_page"]["items"]
    logger.info("DASHBOARD: Found %d items on board", len(board_items))

    for item in board_items:
        group_title = item.get("group", {}).get("title", "")
        if group_title != SAMPLE_INVENTORY_GROUP_NAME:
            continue

        parent_name = item["name"]
        for sub in item.get("subitems", []):
            qty = 0
            times_ordered = 0
            for cv in sub.get("column_values", []):
                col_id = cv.get("id", "")
                val = cv.get("text", "")
                if not val:
                    continue
                if qty_col and col_id == qty_col:
                    try:
                        qty = int(float(val))
                    except (ValueError, TypeError):
                        pass
                elif times_col and col_id == times_col:
                    try:
                        times_ordered = int(float(val))
                    except (ValueError, TypeError):
                        pass

            all_items.append({
                "label": f"{parent_name} — {sub['name']}",
                "color": sub["name"],
                "parent": parent_name,
                "quantity": qty,
                "times_ordered": times_ordered,
            })

    logger.info("DASHBOARD: %d total inventory items, %d in stock", len(all_items), sum(1 for i in all_items if i["quantity"] > 0))
    return all_items


DASHBOARD_CSS = """
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #1a1a2e;
            color: #e0e0e0;
            padding: 24px;
        }}
        .card {{
            background: #16213e;
            border-radius: 8px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.3);
            overflow: hidden;
            border: 1px solid #2a2a4a;
            max-width: 700px;
            margin: 0 auto;
        }}
        .card-header {{
            padding: 16px 20px;
            border-bottom: 1px solid #2a2a4a;
            display: flex;
            align-items: center;
            gap: 10px;
        }}
        .card-header h2 {{
            font-size: 16px;
            font-weight: 600;
            color: #e0e0e0;
        }}
        .card-header .count {{
            margin-left: auto;
            background: #2a2a4a;
            color: #a0a0b8;
            font-size: 12px;
            font-weight: 600;
            padding: 2px 8px;
            border-radius: 10px;
        }}
        .card-body {{
            max-height: 600px;
            overflow-y: auto;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
        }}
        tr {{
            border-bottom: 1px solid #2a2a4a;
        }}
        tr:last-child {{
            border-bottom: none;
        }}
        tr:hover {{
            background: #1e2a4a;
        }}
        td {{
            padding: 10px 16px;
            font-size: 13px;
        }}
        .label-cell {{
            color: #e0e0e0;
            font-weight: 500;
        }}
        .rank-cell {{
            color: #a0a0b8;
            font-weight: 600;
            width: 32px;
            text-align: center;
        }}
        .qty-cell {{
            width: 120px;
        }}
        .bar-container {{
            display: flex;
            align-items: center;
            gap: 8px;
        }}
        .bar {{
            height: 20px;
            background: linear-gradient(90deg, #0073ea, #0060c0);
            border-radius: 4px;
            min-width: 4px;
            transition: width 0.3s ease;
        }}
        .top-bar {{
            background: linear-gradient(90deg, #fdab3d, #e07c00);
        }}
        .bar-value {{
            font-weight: 700;
            font-size: 13px;
            color: #e0e0e0;
            min-width: 24px;
        }}
        .medal {{
            font-size: 10px;
            font-weight: 700;
            padding: 1px 5px;
            border-radius: 3px;
            margin-left: 4px;
            vertical-align: middle;
        }}
        .gold {{ background: #4a3f1f; color: #ffd700; }}
        .silver {{ background: #3a3a4a; color: #c0c0c0; }}
        .bronze {{ background: #3d2f1f; color: #cd7f32; }}
        .empty {{
            text-align: center;
            color: #666;
            padding: 32px 16px;
            font-style: italic;
        }}
        .updated {{
            text-align: center;
            color: #666;
            font-size: 11px;
            padding: 12px;
            margin-top: 16px;
        }}
"""


@app.get("/dashboard/inventory", response_class=HTMLResponse)
async def dashboard_inventory():
    """Inventory quick view — all colors with stock > 0."""
    all_items = await _fetch_sample_inventory_data()
    if all_items is None:
        return HTMLResponse("<h2>Failed to load sample board data</h2>", status_code=500)

    in_stock = sorted([i for i in all_items if i["quantity"] > 0], key=lambda x: (-x["quantity"], x["label"]))
    now_pt = datetime.now(zoneinfo.ZoneInfo("America/Los_Angeles")).strftime("%B %d, %Y at %I:%M %p PT")

    rows = ""
    for item in in_stock:
        bar_width = min(item["quantity"] * 8, 100)
        rows += f"""
        <tr>
            <td class="label-cell">{item["label"]}</td>
            <td class="qty-cell">
                <div class="bar-container">
                    <div class="bar" style="width: {bar_width}%"></div>
                    <span class="bar-value">{item["quantity"]}</span>
                </div>
            </td>
        </tr>"""

    if not rows:
        rows = '<tr><td colspan="2" class="empty">No items in stock</td></tr>'

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Inventory — In Stock</title>
    <style>{DASHBOARD_CSS}</style>
</head>
<body>
    <div class="card">
        <div class="card-header">
            <h2>Inventory — In Stock</h2>
            <span class="count">{len(in_stock)} items</span>
        </div>
        <div class="card-body">
            <table>{rows}</table>
        </div>
    </div>
    <div class="updated">Last updated: {now_pt}</div>
</body>
</html>"""
    return HTMLResponse(html)


@app.get("/dashboard/top-requested", response_class=HTMLResponse)
async def dashboard_top_requested():
    """Top 10 most requested samples — sorted by Times Ordered descending."""
    all_items = await _fetch_sample_inventory_data()
    if all_items is None:
        return HTMLResponse("<h2>Failed to load sample board data</h2>", status_code=500)

    top_requested = sorted([i for i in all_items if i["times_ordered"] > 0], key=lambda x: -x["times_ordered"])[:10]
    now_pt = datetime.now(zoneinfo.ZoneInfo("America/Los_Angeles")).strftime("%B %d, %Y at %I:%M %p PT")

    rows = ""
    max_times = top_requested[0]["times_ordered"] if top_requested else 1
    for i, item in enumerate(top_requested, 1):
        bar_width = (item["times_ordered"] / max_times) * 100
        medal = ""
        if i == 1:
            medal = ' <span class="medal gold">1st</span>'
        elif i == 2:
            medal = ' <span class="medal silver">2nd</span>'
        elif i == 3:
            medal = ' <span class="medal bronze">3rd</span>'

        rows += f"""
        <tr>
            <td class="rank-cell">{i}</td>
            <td class="label-cell">{item["label"]}{medal}</td>
            <td class="qty-cell">
                <div class="bar-container">
                    <div class="bar top-bar" style="width: {bar_width}%"></div>
                    <span class="bar-value">{item["times_ordered"]}</span>
                </div>
            </td>
        </tr>"""

    if not rows:
        rows = '<tr><td colspan="3" class="empty">No sample orders recorded yet</td></tr>'

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Top 10 Most Requested</title>
    <style>{DASHBOARD_CSS}</style>
</head>
<body>
    <div class="card">
        <div class="card-header">
            <h2>Top 10 Most Requested</h2>
            <span class="count">all time</span>
        </div>
        <div class="card-body">
            <table>{rows}</table>
        </div>
    </div>
    <div class="updated">Last updated: {now_pt}</div>
</body>
</html>"""
    return HTMLResponse(html)


@app.post("/webhook/{store_key}")
async def webhook(store_key: str, request: Request):
    if store_key not in STORES:
        logger.warning("Webhook received for UNKNOWN store_key: '%s'", store_key)
        return Response(status_code=404, content="Unknown store")

    body = await request.body()
    header_hmac = request.headers.get("X-Shopify-Hmac-Sha256", "")

    if not header_hmac:
        logger.warning("Webhook from %s REJECTED: Missing X-Shopify-Hmac-Sha256 header", store_key)
        return Response(status_code=401, content="HMAC verification failed")

    if not verify_hmac(body, STORES[store_key]["secret"], header_hmac):
        logger.warning("Webhook from %s REJECTED: HMAC signature mismatch", store_key)
        return Response(status_code=401, content="HMAC verification failed")

    try:
        order = json.loads(body)
    except json.JSONDecodeError as e:
        logger.error("Webhook from %s: Failed to parse JSON body: %s", store_key, e)
        return Response(status_code=200, content="OK")

    # Duplicate detection — check before processing
    order_id = order.get("id", "unknown")
    order_name = order.get("name", "unknown")
    if _is_duplicate(store_key, order_id):
        logger.info("DUPLICATE SKIPPED: Order %s (ID: %s) from %s already processed", order_name, order_id, store_key)
        return Response(status_code=200, content="OK")

    topic = request.headers.get("X-Shopify-Topic", "unknown")
    logger.info("Webhook received: store=%s order=%s topic=%s", store_key, order_name, topic)

    # Process in background so we respond to Shopify immediately
    asyncio.create_task(_safe_process_order(order, store_key, order_name))

    logger.info("Responded 200 to Shopify for order %s — processing in background", order_name)
    return Response(status_code=200, content="OK")


async def _safe_process_order(order: dict, store_key: str, order_name: str) -> None:
    """Wrapper to catch and log any errors from background processing."""
    try:
        await process_order(order, store_key)
    except Exception as e:
        logger.exception("UNHANDLED ERROR processing order %s from %s", order_name, store_key)
        send_failure_email(store_key, order_name, f"Unhandled exception: {type(e).__name__}: {e}", context="Order processing crashed unexpectedly")


@app.post("/test")
async def test_endpoint(request: Request):
    try:
        body = await request.body()
        order = json.loads(body)
    except json.JSONDecodeError as e:
        logger.error("Test endpoint: Failed to parse JSON body: %s", e)
        return {"status": "error", "message": f"Invalid JSON: {e}"}

    store_key = order.pop("_store_key", "semco_pro")
    if store_key not in STORES:
        logger.warning("Test endpoint: Unknown store_key '%s' — defaulting to semco_pro", store_key)
        store_key = "semco_pro"

    logger.info("Test endpoint: processing order %s as %s", order.get("name"), store_key)

    try:
        await process_order(order, store_key)
    except Exception:
        logger.exception("UNHANDLED ERROR processing test order %s", order.get("name"))
        return {"status": "error", "order": order.get("name"), "store": store_key, "message": "See server logs"}

    return {"status": "processed", "order": order.get("name"), "store": store_key}
