# CLAUDE.md — Shopify to Monday.com Integration

## Project Overview

A Python/FastAPI web service that receives Shopify `orders/create` webhooks from three stores (SEMCO Pro, SEMCO Spaces, and SEMCO Connect) and creates structured items on a Monday.com board in real time. Parent items represent orders; subitems represent individual products.

Deployed as a Docker container on Render (Starter plan, always-on).

## Tech Stack

- **Language:** Python 3.12
- **Framework:** FastAPI
- **HTTP Client:** httpx (async)
- **Deployment:** Docker on Render
- **APIs:** Shopify Webhooks (incoming), Monday.com GraphQL API (outgoing)

## File Structure

```
├── app.py                 # Main FastAPI application
├── get_column_ids.py      # Helper to discover Monday.com column IDs
├── requirements.txt       # Python dependencies (fastapi, uvicorn[standard], httpx)
├── Dockerfile             # python:3.12-slim base, uvicorn entrypoint
├── docker-compose.yml     # Local dev compose with .env loading
├── env.example            # Template for all required env vars
├── test_payload.json      # Sample Shopify order for /test endpoint
├── SETUP.md               # Deployment guide
├── PRD-shopify-monday.md  # Full product requirements document
├── CLAUDE.md              # This file
└── update.md              # Implementation progress tracker
```

## Environment Variables

All config is via environment variables (no `.env` in production). Column IDs are **auto-discovered** by display name — no need to configure them manually.

| Variable | Description |
|----------|-------------|
| `MONDAY_API_KEY` | Monday.com API token |
| `MONDAY_BOARD_ID` | Target board ID (update monthly when board is duplicated) |
| `SHOPIFY_SEMCO_PRO_SECRET` | Webhook signing secret for SEMCO Pro |
| `SHOPIFY_SEMCO_SPACES_SECRET` | Webhook signing secret for SEMCO Spaces |
| `SHOPIFY_SEMCO_CONNECT_SECRET` | Webhook signing secret for SEMCO Connect |

## Stores

| Store Key | Shopify Store | Monday.com "Type" Label | Webhook Path |
|-----------|--------------|------------------------|--------------|
| `semco_pro` | SEMCO Pro | `SEMCO SURFACE` | `/webhook/semco_pro` |
| `semco_spaces` | SEMCO Spaces | `SEMCO SPACES` | `/webhook/semco_spaces` |
| `semco_connect` | SEMCO Connect | `SEMCO CONNECT` | `/webhook/semco_connect` |

## Column Auto-Discovery

Column IDs are resolved automatically by matching display names on the Monday.com board. This means when the board is duplicated each month, you only need to update `MONDAY_BOARD_ID` — the new column IDs are discovered on the first webhook.

**Parent columns matched by name:**
- `Time of Order` (text) — receives UTC timestamp
- `Type` (status) — store type label
- `Type Shipment` (status) — shipping method

**Subitem columns matched by name:**
- `Quantity1` (numbers) — product quantity

Column IDs are cached in memory and re-discovered whenever `MONDAY_BOARD_ID` changes.

## Key Conventions

### API Endpoints
- `GET /health` — health check
- `POST /webhook/{store_key}` — receives Shopify webhooks (`semco_pro`, `semco_spaces`, or `semco_connect`)
- `POST /test` — dev-only endpoint, no HMAC verification

### Webhook Security
- All webhooks verified via HMAC-SHA256 (raw body + store secret)
- Compare against `X-Shopify-Hmac-Sha256` header using constant-time comparison
- Return 401 on HMAC failure, 404 on unknown store_key

### Monday.com Item Structure
- **Parent item name:** `{Contact Name} / {Company Name} / Order{order_name}`
- **Parent columns:** Time of Order (text timestamp), Type (SEMCO SURFACE / SEMCO SPACES / SEMCO CONNECT), Type Shipment (UPS/LTL/WILL CALL)
- **Subitem name:** `{Product Title} - {Variant Title}`
- **Subitem columns:** Quantity1 (numbers)

### Monday.com API Auth
- Pass API key in `Authorization` header as raw key (NOT Bearer token)

### Error Handling
- Always return 200 to Shopify, even if Monday.com calls fail
- Log all errors with full detail
- Structured logging with timestamps

### Contact Name Resolution (priority order)
1. Shipping address name
2. Billing address name (fallback)
3. Customer name (fallback)
4. "Unknown Contact" (final fallback)

### Company Name Resolution (priority order)
1. Shipping address company
2. Billing address company (fallback)
3. "No Company" (final fallback)

### Shipping Method Mapping (case-insensitive contains)
- `ups` → `UPS`
- `ltl` → `LTL`
- `will call` → `WILL CALL`
- No match → leave empty

## Monthly Board Rotation

The Monday.com board is duplicated and renamed each month. When this happens:
1. Get the new board ID from the Monday.com URL
2. Update `MONDAY_BOARD_ID` in Render environment variables
3. Redeploy — column IDs are auto-discovered on the next webhook

## Build & Run

```bash
# Local development
docker-compose up --build

# Or run directly
pip install -r requirements.txt
uvicorn app:app --host 0.0.0.0 --port 8000 --reload

# Test with sample payload
curl -X POST http://localhost:8000/test -H "Content-Type: application/json" -d @test_payload.json

# Health check
curl http://localhost:8000/health

# Discover Monday.com column IDs (debugging)
python get_column_ids.py <MONDAY_API_KEY> <BOARD_ID>
```

## Out of Scope
- No database or persistent storage
- No retry queue (Shopify retries webhooks natively)
- No order update/cancel handling — only `orders/create`
- No UI — headless service
- Unlisted columns are left empty for manual team input
