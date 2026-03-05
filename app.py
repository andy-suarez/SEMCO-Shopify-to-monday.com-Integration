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

COL_ORDER_INPUT_TIME = os.environ.get("COL_ORDER_INPUT_TIME", "")
COL_TYPE = os.environ.get("COL_TYPE", "")
COL_TYPE_SHIPMENT = os.environ.get("COL_TYPE_SHIPMENT", "")
COL_SUBITEM_QUANTITY = os.environ.get("COL_SUBITEM_QUANTITY", "")

STORES = {
    "semco_pro": {
        "secret": os.environ.get("SHOPIFY_SEMCO_PRO_SECRET", ""),
        "type_label": "SEMCO SURFACE",
    },
    "semco_spaces": {
        "secret": os.environ.get("SHOPIFY_SEMCO_SPACES_SECRET", ""),
        "type_label": "SEMCO SPACES",
    },
}

MONDAY_API_URL = "https://api.monday.com/v2"

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
    store = STORES[store_key]
    order_name = order.get("name", "Unknown")

    contact = extract_contact_name(order)
    company = extract_company_name(order)
    item_name = f"{contact} / {company} / Order{order_name}"

    logger.info("Processing order %s from %s", order_name, store_key)

    # Build parent column values
    now = datetime.now(timezone.utc)
    column_values: dict = {
        COL_ORDER_INPUT_TIME: {
            "date": now.strftime("%Y-%m-%d"),
            "time": now.strftime("%H:%M:%S"),
        },
        COL_TYPE: {"label": store["type_label"]},
    }

    shipment_type = map_shipping_type(order)
    if shipment_type:
        column_values[COL_TYPE_SHIPMENT] = {"label": shipment_type}
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
        sub_columns = {
            COL_SUBITEM_QUANTITY: str(quantity),
        }

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
