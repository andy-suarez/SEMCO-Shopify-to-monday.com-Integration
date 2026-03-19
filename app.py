import asyncio
import base64
import hashlib
import hmac
import json
import logging
import os
import time
import zoneinfo
from datetime import datetime, timezone

import httpx
from fastapi import FastAPI, Request, Response

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
# Shopify HMAC verification
# ---------------------------------------------------------------------------

def verify_hmac(body: bytes, secret: str, header_hmac: str) -> bool:
    computed = base64.b64encode(
        hmac.new(secret.encode("utf-8"), body, hashlib.sha256).digest()
    ).decode("utf-8")
    return hmac.compare_digest(computed, header_hmac)

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
        logger.info("SKIPPING ORDER %s: Contains only sample items — not posting to Monday.com", order_name)
        return

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
    for i, item in enumerate(expanded_items, 1):
        # Build subitem name: Title - Variant - Color (skip empty parts)
        parts = [item["title"]]
        if item["variant_title"]:
            parts.append(item["variant_title"])
        if item["color"]:
            parts.append(item["color"])
        subitem_name = " - ".join(parts)

        quantity = item["quantity"]
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
        else:
            fail_count += 1

    logger.info("=" * 60)
    logger.info("ORDER %s COMPLETE: %d/%d subitems created successfully", order_name, success_count, len(expanded_items))
    if fail_count > 0:
        logger.error("ORDER %s: %d subitem(s) FAILED to create", order_name, fail_count)
    logger.info("=" * 60)

# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get("/health")
async def health():
    return {
        "status": "ok",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


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
    except Exception:
        logger.exception("UNHANDLED ERROR processing order %s from %s", order_name, store_key)


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
