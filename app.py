import base64
import hashlib
import hmac
import json
import logging
import os
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
}

MONDAY_API_URL = "https://api.monday.com/v2"

# Column names to match on the board (auto-discovered by display name)
PARENT_COL_NAMES = {
    "time_of_order": "Time of Order",
    "type": "Type",
    "type_shipment": "Type Shipment",
}
SUBITEM_COL_NAMES = {
    "quantity": "Quantity1",
}

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
    if not result or not result.get("data", {}).get("boards"):
        logger.error("Could not query board %s for column discovery", board_id)
        return

    parent_cols = result["data"]["boards"][0]["columns"]
    parent_map: dict[str, str] = {}

    subitem_board_id = None
    for col in parent_cols:
        # Match parent columns by display name
        for key, display_name in PARENT_COL_NAMES.items():
            if col["title"] == display_name:
                parent_map[key] = col["id"]
                logger.info("  Parent column '%s' → %s (%s)", display_name, col["id"], col["type"])

        # Find subitem board ID
        if col["type"] == "subtasks":
            try:
                settings = json.loads(col["settings_str"])
                ids = settings.get("boardIds", [])
                if ids:
                    subitem_board_id = str(ids[0])
            except (json.JSONDecodeError, KeyError):
                pass

    # --- Subitem columns ---
    subitem_map: dict[str, str] = {}
    if subitem_board_id:
        sub_result = await monday_request(query, {"boardId": [subitem_board_id]})
        if sub_result and sub_result.get("data", {}).get("boards"):
            sub_cols = sub_result["data"]["boards"][0]["columns"]
            for col in sub_cols:
                for key, display_name in SUBITEM_COL_NAMES.items():
                    if col["title"] == display_name:
                        subitem_map[key] = col["id"]
                        logger.info("  Subitem column '%s' → %s (%s)", display_name, col["id"], col["type"])
    else:
        logger.warning("Could not find subitem board ID for board %s", board_id)

    # Verify all required columns were found
    missing = []
    for key in PARENT_COL_NAMES:
        if key not in parent_map:
            missing.append(f"Parent: {PARENT_COL_NAMES[key]}")
    for key in SUBITEM_COL_NAMES:
        if key not in subitem_map:
            missing.append(f"Subitem: {SUBITEM_COL_NAMES[key]}")

    if missing:
        logger.error("Missing columns on board %s: %s", board_id, ", ".join(missing))
    else:
        logger.info("All column IDs discovered successfully for board %s", board_id)

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

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(MONDAY_API_URL, headers=headers, json=payload)
        data = resp.json()

    if "errors" in data:
        logger.error("Monday.com API errors: %s", data["errors"])
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
    variables = {
        "boardId": MONDAY_BOARD_ID,
        "itemName": item_name,
        "columnValues": json.dumps(column_values),
    }
    result = await monday_request(query, variables)
    if result and "data" in result:
        item_id = result["data"]["create_item"]["id"]
        logger.info("Created parent item: %s (ID: %s)", item_name, item_id)
        return item_id
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
    variables = {
        "parentItemId": parent_item_id,
        "itemName": item_name,
        "columnValues": json.dumps(column_values),
    }
    result = await monday_request(query, variables)
    if result and "data" in result:
        sub_id = result["data"]["create_subitem"]["id"]
        logger.info("Created subitem: %s (ID: %s)", item_name, sub_id)
        return sub_id
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
                return f"{first} {last}".strip()

    customer = order.get("customer")
    if customer:
        first = (customer.get("first_name") or "").strip()
        last = (customer.get("last_name") or "").strip()
        if first or last:
            return f"{first} {last}".strip()

    return "Unknown Contact"


def extract_company_name(order: dict) -> str:
    for key in ("shipping_address", "billing_address"):
        addr = order.get(key)
        if addr:
            company = (addr.get("company") or "").strip()
            if company:
                return company

    return "No Company"


def map_shipping_type(order: dict) -> str | None:
    shipping_lines = order.get("shipping_lines") or []
    for line in shipping_lines:
        title = (line.get("title") or "").lower()
        code = (line.get("code") or "").lower()
        combined = f"{title} {code}"

        if "ups" in combined:
            return "UPS"
        if "ltl" in combined:
            return "LTL"
        if "will call" in combined:
            return "Will Calls"

    return None

# ---------------------------------------------------------------------------
# Order processing
# ---------------------------------------------------------------------------

async def process_order(order: dict, store_key: str) -> None:
    # Ensure column IDs are discovered
    await _discover_column_ids()

    store = STORES[store_key]
    order_name = order.get("name", "Unknown")

    contact = extract_contact_name(order)
    company = extract_company_name(order)
    item_name = f"{contact} / {company} / Order{order_name}"

    logger.info("Processing order %s from %s", order_name, store_key)

    # Build parent column values
    now = datetime.now(timezone.utc)
    timestamp_str = now.strftime("%Y-%m-%d %H:%M:%S")

    column_values: dict = {}

    col_time = get_parent_col("time_of_order")
    if col_time:
        column_values[col_time] = timestamp_str

    col_type = get_parent_col("type")
    if col_type:
        column_values[col_type] = {"label": store["type_label"]}

    shipment_type = map_shipping_type(order)
    if shipment_type:
        col_shipment = get_parent_col("type_shipment")
        if col_shipment:
            column_values[col_shipment] = {"label": shipment_type}
        logger.info("Shipping type mapped to: %s", shipment_type)
    else:
        logger.info("No shipping type match — leaving Type Shipment empty")

    # Create parent item
    parent_id = await create_parent_item(item_name, column_values)
    if not parent_id:
        logger.error("Failed to create parent item for order %s", order_name)
        return

    # Create subitems for each line item
    line_items = order.get("line_items") or []
    for li in line_items:
        product_title = li.get("title", "Unknown Product")
        variant_title = (li.get("variant_title") or "").strip()
        if variant_title:
            subitem_name = f"{product_title} - {variant_title}"
        else:
            subitem_name = product_title

        quantity = li.get("quantity", 1)
        sub_columns: dict = {}

        col_qty = get_subitem_col("quantity")
        if col_qty:
            sub_columns[col_qty] = str(quantity)

        await create_subitem(parent_id, subitem_name, sub_columns)

    logger.info("Finished processing order %s — %d line items", order_name, len(line_items))

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
        return Response(status_code=404, content="Unknown store")

    body = await request.body()
    header_hmac = request.headers.get("X-Shopify-Hmac-Sha256", "")

    if not header_hmac or not verify_hmac(body, STORES[store_key]["secret"], header_hmac):
        logger.warning("HMAC verification failed for %s", store_key)
        return Response(status_code=401, content="HMAC verification failed")

    order = json.loads(body)
    topic = request.headers.get("X-Shopify-Topic", "unknown")
    logger.info("Webhook received: store=%s order=%s topic=%s", store_key, order.get("name"), topic)

    try:
        await process_order(order, store_key)
    except Exception:
        logger.exception("Error processing order %s from %s", order.get("name"), store_key)

    return Response(status_code=200, content="OK")


@app.post("/test")
async def test_endpoint(request: Request):
    body = await request.body()
    order = json.loads(body)

    store_key = order.pop("_store_key", "semco_pro")
    if store_key not in STORES:
        store_key = "semco_pro"

    logger.info("Test endpoint: processing order %s as %s", order.get("name"), store_key)

    try:
        await process_order(order, store_key)
    except Exception:
        logger.exception("Error processing test order %s", order.get("name"))

    return {"status": "processed", "order": order.get("name"), "store": store_key}
