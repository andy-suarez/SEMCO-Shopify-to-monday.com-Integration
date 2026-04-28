# CLAUDE.md — Shopify to Monday.com Integration

## Project Overview

A Python/FastAPI web service that receives Shopify `orders/create` webhooks from four stores (SEMCO Pro, SEMCO Spaces, SEMCO Connect, and SEMCO Works) and creates structured items on a Monday.com board in real time. Parent items represent orders; subitems represent individual products.

Additionally manages a **Sample Inventory board** that tracks sample stock across Pro, Spaces, and Connect stores, auto-decrementing quantities when sample orders come in.

Deployed as a Docker container on Render (Starter plan, always-on).

## Tech Stack

- **Language:** Python 3.12
- **Framework:** FastAPI
- **HTTP Client:** httpx (async)
- **Deployment:** Docker on Render
- **APIs:** Shopify Webhooks (incoming), Monday.com GraphQL API (outgoing), Gmail SMTP (outgoing notifications)

## File Structure

```
├── app.py                 # Main FastAPI application (all logic in one file)
├── get_column_ids.py      # Helper to discover Monday.com column IDs
├── requirements.txt       # Python dependencies (fastapi, uvicorn[standard], httpx)
├── Dockerfile             # python:3.12-slim base, uvicorn entrypoint
├── docker-compose.yml     # Local dev compose with .env loading
├── env.example            # Template for all required env vars
├── test_payload.json      # Sample Shopify order for /test endpoint
├── SETUP.md               # Deployment guide
├── CLAUDE.md              # This file
└── update.md              # Implementation progress tracker
```

## Environment Variables

All config is via environment variables (no `.env` in production). Column IDs are **auto-discovered** by display name — no need to configure them manually.

| Variable | Description |
|----------|-------------|
| `MONDAY_API_KEY` | Monday.com API token |
| `MONDAY_BOARD_ID` | Target orders board ID (update monthly when board is duplicated) |
| `MONDAY_SAMPLE_BOARD_ID` | Sample Inventory board ID (permanent, not rotated monthly) |
| `SHOPIFY_SEMCO_PRO_SECRET` | Webhook signing secret for SEMCO Pro |
| `SHOPIFY_SEMCO_SPACES_SECRET` | Webhook signing secret for SEMCO Spaces |
| `SHOPIFY_SEMCO_CONNECT_SECRET` | Webhook signing secret for SEMCO Connect |
| `SHOPIFY_SEMCO_WORKS_SECRET` | Webhook signing secret for SEMCO Works |
| `SMTP_EMAIL` | Gmail address for sending email notifications |
| `SMTP_PASSWORD` | Gmail App Password for SMTP authentication |
| `NOTIFY_EMAILS` | Comma-separated list of notification recipients |

## Stores

| Store Key | Shopify Store | Monday.com "Type" Label | Webhook Path | Notes |
|-----------|--------------|------------------------|--------------|-------|
| `semco_pro` | SEMCO Pro | `SEMCO SURFACE` | `/webhook/semco_pro` | Full orders + sample inventory |
| `semco_spaces` | SEMCO Spaces | `SEMCO SPACES` | `/webhook/semco_spaces` | Full orders + sample inventory |
| `semco_connect` | SEMCO Connect | `SEMCO CONNECT` | `/webhook/semco_connect` | Full orders + sample inventory |
| `semco_works` | SEMCO Works | `SEMCO WORKS` | `/webhook/semco_works` | LTL and Will Call orders only |

## Column Auto-Discovery

Column IDs are resolved automatically by matching display names on the Monday.com board. This means when the orders board is duplicated each month, you only need to update `MONDAY_BOARD_ID` — the new column IDs are discovered on the first webhook.

**Parent columns matched by name:**
- `Order Input Time` (text) — receives HH:MM timestamp in PT
- `Type` (status) — store type label
- `Type Shipment` (status) — shipping method

**Subitem columns matched by name:**
- `Quantity1` (numbers) — product quantity

Column IDs are cached in memory and re-discovered whenever `MONDAY_BOARD_ID` changes.

## Key Conventions

### API Endpoints
- `GET /health` — health check
- `POST /webhook/{store_key}` — receives Shopify webhooks (`semco_pro`, `semco_spaces`, `semco_connect`, or `semco_works`)
- `POST /test` — dev-only endpoint, no HMAC verification; supports `_store_key` field in payload to simulate any store

### Webhook Security
- All webhooks verified via HMAC-SHA256 (raw body + store secret)
- Compare against `X-Shopify-Hmac-Sha256` header using constant-time comparison
- Return 401 on HMAC failure, 404 on unknown store_key

### Duplicate Detection
- In-memory tracking of processed Shopify order IDs (keyed by `store:order_id`)
- 1-hour TTL — entries older than 3600 seconds are cleaned up automatically
- Prevents double-processing when Shopify retries webhooks

### Monday.com Item Structure (Orders Board)
- **Parent item name:** `{Contact Name} / {Company Name} / Order{order_name}`
  - If no company name found, company is omitted: `{Contact Name} / Order{order_name}`
- **Parent columns:** Order Input Time (HH:MM PT), Type (SEMCO SURFACE / SEMCO SPACES / SEMCO CONNECT / SEMCO WORKS), Type Shipment (UPS/LTL/WILL CALL)
- **Subitem name:** `{Product Title} - {Variant Title} - {Color}` (empty parts omitted)
- **Subitem columns:** Quantity1 (numbers)

### Monday.com API Auth
- Pass API key in `Authorization` header as raw key (NOT Bearer token)

### Error Handling
- Always return 200 to Shopify immediately, even if Monday.com calls fail
- Orders process in the background via `asyncio.create_task`
- Log all errors with full detail
- Structured logging with timestamps
- Email notifications sent on success and failure (if SMTP configured)

### Contact Name Resolution (priority order)
1. Shipping address name
2. Billing address name (fallback)
3. Customer name (fallback)
4. "Unknown Contact" (final fallback)

### Company Name Resolution (priority order)
1. Shipping address company
2. Billing address company (fallback)
3. Omitted from item name if not found (no "No Company" placeholder)

### Shipping Method Mapping (case-insensitive contains)
- `ups`, `flat rate`, `economy` → `UPS`
- `ltl`, `r + l`, `r+l` → `LTL`
- `will call`, `pickup` → `WILL CALL`
- No match → defaults to `WILL CALL`

## SEMCO Works — Strict Shipping Filter

SEMCO Works only posts orders with **explicitly LTL or Will Call shipping**. All other shipping types (including "Free Ground Shipping" and similar) are silently skipped. This uses strict inline matching — it does NOT use `map_shipping_type()` because the default WILL CALL fallback would incorrectly let ground orders through.

## Multi-Color Line Item Expansion

A single Shopify line item can contain multiple color selections via `properties`. The system detects two formats and expands them into separate subitems:

**Spaces format:** `[{'name': 'Color', 'value': 'Charcoal Grey'}]` — single color, uses the line item quantity.

**Pro format:** `[{'name': 'Mojave', 'value': '3'}, {'name': 'Phantom', 'value': '4'}]` — multiple colors with individual quantities (property name = color, property value = quantity as digit).

If no color properties are found, the line item is created as-is with no color suffix.

## SEMCO Pro LTL — SKU Summary Update

When a SEMCO Pro order ships via LTL, an update bubble (comment) is automatically added to the parent item on Monday.com. The update summarizes SKU counts in the format:

```
{qty}: {sku}, {qty}: {sku}
Water Based Building Products
```

## Sample Inventory System

SEMCO Pro, Spaces, and Connect sell sample products from a shared physical inventory. The system routes sample orders to a dedicated **Sample Inventory board** on Monday.com.

### Two Boards
- **Orders Board** (`MONDAY_BOARD_ID`): Rotated monthly. Full orders with all line items.
- **Sample Inventory Board** (`MONDAY_SAMPLE_BOARD_ID`): Permanent. Tracks sample stock by texture/color and logs sample requests.

### Sample Board Structure

The Sample Inventory board has two groups:

1. **Sample Inventory** — Parent items are texture lines (Corsa/Smooth, Vellum/Natural, Polished, Solid, Grain, ADA, Custom). Each parent has color subitems (Baked Clay, Black Pearl, Blanco, etc.) with a `Quantity1` column tracking current stock.

2. **Sample Requests Log** — Orders containing samples are logged here as parent items with subitems for each sample color ordered. Parent items have a `Type` status column populated with the originating store (`SEMCO SURFACE`, `SEMCO SPACES`, `SEMCO CONNECT`, or `SEMCO WORKS`) — same labels as the orders board's Type column.

### Sample Detection

Products are identified as samples by matching titles against `SAMPLE_PRODUCT_NAMES`:
- `"architectural sample kits"` (Pro)
- `"x-bond microcement physical color samples"` (Spaces)
- Connect: confirm and add the exact Connect sample product title (substring-matched, case-insensitive)

### Three Order Scenarios (Pro, Spaces, Connect)

1. **Sample-only order** → Skips the orders board entirely. Logs to Sample Requests Log and decrements inventory.
2. **Mixed order (samples + regular products)** → Posts the full order (all items) to the orders board AND also logs just the sample items to the Sample Requests Log with inventory decrement.
3. **Non-sample order** → Posts to the orders board normally. No sample board interaction.

### Texture/Color Parsing from Shopify Variant Titles

**Pro/Connect format:** `"Corsa / Polar Bear"` — splits on ` / ` to extract texture prefix and color name. Texture prefix is mapped via `TEXTURE_MAP` (e.g., `corsa` → `Corsa/Smooth`, `vellum` → `Vellum/Natural`). Unknown textures default to `Corsa/Smooth`. Connect is assumed to use this format; flip to Spaces format below if its variants are just the color.

**Spaces format:** `"Phantom"` — variant title IS the color. Texture is always `Corsa/Smooth` (Spaces only sells Corsa samples).

### Inventory Decrement

When a sample order is processed:
1. The system looks up the matching texture parent item and color subitem on the Sample Inventory board
2. Reads the **fresh** current quantity from Monday.com (not from cache, to avoid stale data)
3. Subtracts the ordered quantity (clamped to 0 minimum)
4. Updates the subitem quantity via `change_column_value` mutation
5. Updates the local cache with the new value

### Texture Map

```python
TEXTURE_MAP = {
    "corsa": "Corsa/Smooth",
    "vellum": "Vellum/Natural",
    "polished": "Polished",
    "solid": "Solid",
    "grain": "Grain",
    "ada": "ADA",
}
```

### Sample Board Discovery

On first webhook (or when `MONDAY_SAMPLE_BOARD_ID` changes), the system auto-discovers:
- The "Sample Requests Log" group ID
- The subitem board ID and `Quantity1` column ID
- All inventory items: texture parent items in the "Sample Inventory" group, their color subitems, and current quantities

All cached in `_sample_board_cache` and reused for subsequent requests.

## Email Notifications

HTML email notifications are sent via Gmail SMTP for order success and failure events. Requires `SMTP_EMAIL`, `SMTP_PASSWORD` (Gmail App Password), and `NOTIFY_EMAILS` to be configured.

**Success email:** Includes store name, order number, item name, shipment type, timestamp (PT), and list of created subitems.

**Failure email:** Includes store name, order number, error details, and context about what failed.

Emails are sent synchronously from background tasks. If SMTP is not configured, notifications are silently skipped.

## Monthly Board Rotation

The Monday.com orders board is duplicated and renamed each month. When this happens:
1. Get the new board ID from the Monday.com URL
2. Update `MONDAY_BOARD_ID` in Render environment variables
3. Redeploy — column IDs are auto-discovered on the next webhook

**Note:** The Sample Inventory board (`MONDAY_SAMPLE_BOARD_ID`) is NOT rotated monthly — it's a permanent board.

## Build & Run

```bash
# Local development
docker-compose up --build

# Or run directly
pip install -r requirements.txt
uvicorn app:app --host 0.0.0.0 --port 8000 --reload

# Test with sample payload
curl -X POST http://localhost:8000/test -H "Content-Type: application/json" -d @test_payload.json

# Test as a specific store (add _store_key field)
curl -X POST http://localhost:8000/test -H "Content-Type: application/json" \
  -d '{"_store_key": "semco_spaces", "name": "#TEST1", "line_items": [...]}'

# Health check
curl http://localhost:8000/health

# Discover Monday.com column IDs (debugging)
python get_column_ids.py <MONDAY_API_KEY> <BOARD_ID>
```

## Sample Inventory Sync (Monday → Shopify)

A one-way polling sync pushes sample stock quantities from the Monday.com **Sample Inventory board** to the Shopify sample product on each configured store. Triggered by a **Render Cron Job** that POSTs to `/sync-inventory` once daily.

### What it does
- Reads the Monday sample board via the existing `_fetch_sample_inventory_data()` helper (single source of truth — same data the dashboards use)
- For each Shopify store in `SHOPIFY_SYNC_STORES`:
  - Fetches all variants of the configured sample product
  - Joins Monday rows to Shopify variants by `(texture, color)` (case-insensitive)
  - Sets `inventory_level.available` at the configured location to the Monday quantity
- Monday rows without a Shopify match → logged as `Missing mapping` and skipped (no failure)
- Shopify variants without a Monday match → left alone (no zero-out)

### Texture/color join keys
Both sides are normalized to a canonical lowercase texture via `_canonical_texture()`, which uses the existing `TEXTURE_MAP`:

- **Monday side:** parent names like `"Flex Samples - X-BOND Corsa/Smooth"` → strip prefix → `"Corsa/Smooth"` → canonical `"corsa/smooth"`. Subitem name → `color` (lowercased + stripped).
- **Shopify side:** variant titles are in the **short form** `"Corsa / Baked Clay"` (just the texture prefix like `corsa`, `vellum`, not the full `Corsa/Smooth`). Split on ` / `, look up the prefix in `TEXTURE_MAP` → canonical `"corsa/smooth"`.
- Special case: `"Custom Flex Samples"` → canonical `"custom"`.
- Variants/parents that don't match any `TEXTURE_MAP` entry are logged as warnings and skipped.

### Shopify auth (2026 Dev Dashboard flow)
- Single OAuth `client_credentials` grant per store install — but all installs of the **same app** in the same org share **one** Client ID + Secret (Shopify's Dev Dashboard issues credentials at the app level, not per-install).
- Token mint POSTs to each store's own `https://{store}.myshopify.com/admin/oauth/access_token` and gets back a **store-specific** access token.
- Access token cached in memory per-store for 24h; re-minted on 401 or expiry.
- `X-Shopify-Access-Token` header on all REST calls.
- Uses Shopify Admin REST API version `2024-10`.
- 429 responses honor `Retry-After` and retry once.

### Environment Variables (sync-specific)

| Variable | Scope | Description |
|----------|-------|-------------|
| `SHOPIFY_CLIENT_ID` | shared | OAuth client ID from Dev Dashboard → Settings (one app, all installs) |
| `SHOPIFY_CLIENT_SECRET` | shared | OAuth client secret (one app, all installs) |
| `SHOPIFY_PRO_STORE_DOMAIN` | per-store | e.g. `semcopro.myshopify.com` |
| `SHOPIFY_PRO_LOCATION_ID` | per-store | Shopify location ID where Pro sample stock lives |
| `SHOPIFY_PRO_SAMPLE_PRODUCT_ID` | per-store | Sample product ID on Pro |
| `SHOPIFY_SPACES_STORE_DOMAIN` | per-store | e.g. `semcospaces.myshopify.com` |
| `SHOPIFY_SPACES_LOCATION_ID` | per-store | Shopify location ID where Spaces sample stock lives |
| `SHOPIFY_SPACES_SAMPLE_PRODUCT_ID` | per-store | Sample product ID on Spaces |
| `SHOPIFY_CONNECT_STORE_DOMAIN` | per-store | e.g. `semcoconnect.myshopify.com` |
| `SHOPIFY_CONNECT_LOCATION_ID` | per-store | Shopify location ID where Connect sample stock lives |
| `SHOPIFY_CONNECT_SAMPLE_PRODUCT_ID` | per-store | Sample product ID on Connect |
| `SYNC_AUTH_TOKEN` | optional | Shared secret for `/sync-inventory`. If unset, endpoint is open. |

`MONDAY_SAMPLE_BOARD_ID` is reused from the existing sample inventory config.

Stores without complete config (missing `domain`/`location_id`/`product_id`) are silently skipped — enables staged rollout. The shared `SHOPIFY_CLIENT_ID`/`SECRET` must be set for any store to be considered configured.

**Backwards-compat:** Legacy `SHOPIFY_PRO_CLIENT_ID` / `SHOPIFY_PRO_CLIENT_SECRET` env vars still work as a fallback if the new shared names aren't set. Migrate when convenient.

### Per-store variant title formats

Each entry in `SHOPIFY_SYNC_STORES` has a `variant_format` flag that controls how Shopify variant titles are parsed:

- **`"pro"`** — variant titles look like `"Corsa / Baked Clay"`. Split on ` / ` → `(texture, color)`. Texture is normalized via `_canonical_texture()` using `TEXTURE_MAP`.
- **`"spaces"`** — variant titles are just the color, e.g. `"Phantom"`. Texture is implicitly hardcoded to `corsa/smooth` (Spaces only sells Corsa samples).

### Endpoint
```
GET or POST /sync-inventory[?dry_run=true|1|yes][&token=<SYNC_AUTH_TOKEN>]
Optional header: X-Sync-Token: <SYNC_AUTH_TOKEN>
```
Returns `{"status": "ok", "dry_run": bool, "duration_seconds": N, "summary": {...}}`. `dry_run=true` logs what would be set but writes nothing to Shopify.

Per-store summary counts: `matched`, `skipped_missing`, `updated`, `errors`.

**Accepts both GET and POST** so you can trigger a sync from a browser by just visiting the URL.

**Auth is optional:**
- If `SYNC_AUTH_TOKEN` env var is set, callers must provide a matching token via `X-Sync-Token` header OR `?token=...` query param. Missing/wrong → 401.
- If `SYNC_AUTH_TOKEN` env var is not set, the endpoint is open (still rate-limited).

**Rate limit:** 1 minute between runs (in-memory, per-process). Concurrent calls get `status: "busy"`; too-soon calls get `status: "rate_limited"` with `retry_in_seconds`. This is a testing-window setting — bump `SYNC_RATE_LIMIT_SECONDS` in `app.py` to 300 (5 min) or higher for production.

**Last-run state** is tracked in memory (`_sync_run_state`): last start/finish timestamps, last dry_run flag, last summary, last error. Useful for quick debugging and for future last-run display.

### Render Cron Schedule
- Summer (PDT): `0 13 * * *` UTC → 6 AM PT
- Winter (PST): `0 14 * * *` UTC → 6 AM PT
- Command:
  ```bash
  curl -fsS -X POST \
    -H "X-Sync-Token: $SYNC_AUTH_TOKEN" \
    "https://<render-url>/sync-inventory"
  ```

### Adding more stores in the future
1. Install the Dev Dashboard app on the additional store (no new credentials needed — `SHOPIFY_CLIENT_ID`/`SECRET` are reused across all installs of the same app).
2. Add the store's `*_STORE_DOMAIN`, `*_LOCATION_ID`, `*_SAMPLE_PRODUCT_ID` env vars in Render.
3. Add a new entry to `SHOPIFY_SYNC_STORES` in `app.py` with the appropriate `variant_format` (`"pro"` or `"spaces"`).
4. Redeploy — sync will pick up the new store automatically.

### Isolation from order processing
- Uses its own config dict (`SHOPIFY_SYNC_STORES`) — separate from webhook `STORES`
- Uses its own token and variant caches (`_shopify_token_cache`, `_shopify_variant_cache`)
- Reuses existing `_fetch_sample_inventory_data()` and `_sample_board_cache` for Monday reads (single source of truth — no double-polling the board)
- The existing order webhook path, `process_order()`, and sample decrement logic are untouched

## Out of Scope
- No database or persistent storage — all state lives in Monday.com
- No retry queue (Shopify retries webhooks natively)
- No order update/cancel handling — only `orders/create`
- No UI — headless service
- Unlisted columns on the orders board are left empty for manual team input
- Inventory sync is one-way only (Monday → Shopify); Shopify-originated inventory changes are not read back
