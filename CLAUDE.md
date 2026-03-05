# CLAUDE.md — Shopify to Monday.com Integration

## Project Overview

A Python/FastAPI web service that receives Shopify `orders/create` webhooks from two stores (SEMCO Pro and SEMCO Spaces) and creates structured items on a Monday.com board in real time. Parent items represent orders; subitems represent individual products.

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

All config is via environment variables (no `.env` in production):

| Variable | Description |
|----------|-------------|
| `MONDAY_API_KEY` | Monday.com API token |
| `MONDAY_BOARD_ID` | Target board ID (integer) |
| `COL_ORDER_INPUT_TIME` | Column ID for Order Input Time |
| `COL_TYPE` | Column ID for Type column |
| `COL_TYPE_SHIPMENT` | Column ID for Type Shipment column |
| `COL_SUBITEM_QUANTITY` | Column ID for Quantity1 subitem column |
| `SHOPIFY_SEMCO_PRO_SECRET` | Webhook signing secret for SEMCO Pro |
| `SHOPIFY_SEMCO_SPACES_SECRET` | Webhook signing secret for SEMCO Spaces |

## Key Conventions

### API Endpoints
- `GET /health` — health check
- `POST /webhook/{store_key}` — receives Shopify webhooks (`semco_pro` or `semco_spaces`)
- `POST /test` — dev-only endpoint, no HMAC verification

### Webhook Security
- All webhooks verified via HMAC-SHA256 (raw body + store secret)
- Compare against `X-Shopify-Hmac-Sha256` header using constant-time comparison
- Return 401 on HMAC failure, 404 on unknown store_key

### Monday.com Item Structure
- **Parent item name:** `{Contact Name} / {Company Name} / Order#{order_name}`
- **Parent columns:** Order Input Time (date+time), Type (SEMCO SURFACE or SEMCO SPACES), Type Shipment (UPS/LTL/Will Calls)
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
- `will call` → `Will Calls`
- No match → leave empty

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

# Discover Monday.com column IDs
python get_column_ids.py <MONDAY_API_KEY> <BOARD_ID>
```

## Out of Scope
- No database or persistent storage
- No retry queue (Shopify retries webhooks natively)
- No order update/cancel handling — only `orders/create`
- No UI — headless service
- Unlisted columns are left empty for manual team input
