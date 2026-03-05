# Setup & Deployment Guide

## Local Development

### 1. Clone the repo

```bash
git clone <repo-url>
cd shopify-monday
```

### 2. Create your `.env` file

```bash
cp env.example .env
```

Edit `.env` and fill in your real values.

### 3. Find your Monday.com column IDs

```bash
pip install httpx
python get_column_ids.py <YOUR_MONDAY_API_KEY> <YOUR_BOARD_ID>
```

Copy the column IDs into your `.env` file.

### 4. Run locally

With Docker:
```bash
docker-compose up --build
```

Or without Docker:
```bash
pip install -r requirements.txt
uvicorn app:app --host 0.0.0.0 --port 8000 --reload
```

### 5. Test

```bash
# Health check
curl http://localhost:8000/health

# Send test payload
curl -X POST http://localhost:8000/test \
  -H "Content-Type: application/json" \
  -d @test_payload.json
```

---

## Deploy to Render

### 1. Push to GitHub

Push this project to a GitHub repository.

### 2. Create Web Service on Render

1. Go to [render.com](https://render.com) and create a new **Web Service**
2. Connect your GitHub repo
3. Render will auto-detect the `Dockerfile`
4. Select the **Starter plan** ($7/mo) for always-on

### 3. Add Environment Variables

In the Render dashboard, go to the **Environment** tab and add all variables from `env.example` with your real values:

- `MONDAY_API_KEY`
- `MONDAY_BOARD_ID`
- `COL_ORDER_INPUT_TIME`
- `COL_TYPE`
- `COL_TYPE_SHIPMENT`
- `COL_SUBITEM_QUANTITY`
- `SHOPIFY_SEMCO_PRO_SECRET`
- `SHOPIFY_SEMCO_SPACES_SECRET`

### 4. Deploy

Render will build and deploy automatically. Verify via:

```
https://<your-render-url>/health
```

### 5. Test with sample payload

```bash
curl -X POST https://<your-render-url>/test \
  -H "Content-Type: application/json" \
  -d @test_payload.json
```

---

## Register Shopify Webhooks

In each Shopify store admin, create an `orders/create` webhook:

### SEMCO Pro
- **Event:** Order creation
- **URL:** `https://<your-render-url>/webhook/semco_pro`
- **Format:** JSON

### SEMCO Spaces
- **Event:** Order creation
- **URL:** `https://<your-render-url>/webhook/semco_spaces`
- **Format:** JSON

Copy the webhook signing secret from each store into the corresponding environment variable on Render.

---

## Pre-Production Checklist

- [ ] `/health` returns OK on Render
- [ ] `/test` creates a parent item + subitems on Monday.com
- [ ] Column values populate correctly (date, type, shipment, quantity)
- [ ] Remove or protect the `/test` endpoint
- [ ] Register webhooks in both Shopify stores
- [ ] Verify a real Shopify order creates items correctly
