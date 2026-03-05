# update.md — Implementation Progress Tracker

## Status Legend
- [ ] Not started
- [x] Complete
- [~] In progress

---

## 1. Project Setup

- [x] Create `requirements.txt` with pinned versions (fastapi, uvicorn[standard], httpx)
- [x] Create `env.example` with all required environment variables
- [x] Create `Dockerfile` (python:3.12-slim, uvicorn entrypoint, port 8000)
- [x] Create `docker-compose.yml` (single service, .env loading, logging config)
- [x] Create `.gitignore` (Python defaults, .env, __pycache__, etc.)

## 2. Core Application (`app.py`)

### 2a. FastAPI App & Config
- [x] Initialize FastAPI app
- [x] Load environment variables (Monday API key, board ID, Shopify secrets)
- [x] Define store config mapping (3 stores: `semco_pro`, `semco_spaces`, `semco_connect`)
- [x] Set up structured logging with timestamps

### 2b. Column Auto-Discovery
- [x] Query board columns by display name on first request
- [x] Resolve subitem board ID from parent board settings
- [x] Query subitem columns by display name
- [x] Cache column IDs in memory, refresh when board ID changes
- [x] Log discovered column mappings
- [x] Log warnings for missing columns

### 2c. Health Endpoint
- [x] `GET /health` returns `{"status": "ok", "timestamp": "..."}`

### 2d. Webhook Security
- [x] Read raw request body as bytes
- [x] Compute HMAC-SHA256 with store-specific secret
- [x] Base64 encode and compare against `X-Shopify-Hmac-Sha256` header
- [x] Use constant-time comparison (`hmac.compare_digest`)
- [x] Return 401 on missing/invalid HMAC

### 2e. Webhook Endpoint
- [x] `POST /webhook/{store_key}` — validate store_key (404 if unknown)
- [x] Verify HMAC signature
- [x] Parse order JSON from request body
- [x] Process order (create parent + subitems)
- [x] Always return 200, even on Monday.com errors
- [x] Log webhook receipt (store, order name, topic)

### 2f. Test Endpoint
- [x] `POST /test` — accepts order JSON without HMAC
- [x] Read optional `_store_key` from JSON body (default: `semco_pro`)

### 2g. Order Processing Logic
- [x] Extract contact name (shipping → billing → customer → "Unknown Contact")
- [x] Extract company name (shipping → billing → "No Company")
- [x] Extract order name from Shopify `name` field
- [x] Build parent item name: `{Contact} / {Company} / Order{order_name}`
- [x] Map shipping method to Type Shipment label (case-insensitive contains: ups/ltl/will call)
- [x] Build parent column values:
  - [x] Time of Order: current UTC timestamp as text string
  - [x] Type: SEMCO SURFACE / SEMCO SPACES / SEMCO CONNECT based on store
  - [x] Type Shipment: mapped shipping label (or omit if no match)

### 2h. Monday.com API Integration
- [x] Async HTTP client (httpx) for Monday.com GraphQL API
- [x] `Authorization` header with raw API key (not Bearer)
- [x] Create parent item mutation (`create_item`)
- [x] Create subitem mutation (`create_subitem`)
- [x] Loop through line_items → create subitems with name + quantity
- [x] Subitem name: `{Product Title} - {Variant Title}` (or just title if no variant)
- [x] Column values use auto-discovered IDs
- [x] Log every parent item and subitem created (name, ID)
- [x] Handle and log Monday.com API errors without raising

## 3. Helper Utility

- [x] Create `get_column_ids.py`
- [x] Accept CLI args: `<MONDAY_API_KEY> <BOARD_ID>`
- [x] Query board columns via GraphQL
- [x] Query subitem columns via GraphQL
- [x] Print results in readable table format (title, ID, type)

## 4. Test Payload

- [x] Create `test_payload.json` with realistic Shopify order structure
- [x] Include shipping address with contact name and company
- [x] Include multiple line_items with different variant sizes
- [x] Include shipping_line with UPS method
- [x] Include `_store_key` field set to `semco_pro`

## 5. Documentation

- [x] Create `CLAUDE.md`
- [x] Create `update.md` (this file)
- [x] Create `SETUP.md` with deployment steps for Render

## 6. Pre-Production Checklist

- [ ] Verify all endpoints work with test payload
- [ ] Verify HMAC validation with real Shopify webhook
- [ ] Confirm Monday.com parent items created correctly
- [ ] Confirm Monday.com subitems created correctly
- [ ] Confirm column values populate correctly (time of order, type, shipment, quantity)
- [ ] Confirm shipping method mapping works for all cases
- [ ] Confirm contact/company fallback logic works
- [ ] Protect or remove `/test` endpoint
- [ ] Deploy to Render
- [ ] Register Shopify webhooks for all three stores
