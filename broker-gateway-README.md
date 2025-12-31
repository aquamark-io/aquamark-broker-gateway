# Aquamark Broker Email Gateway

Email gateway service that allows brokers to watermark PDFs via email.

## How It Works

1. Broker sends email to their unique gateway address: `broker-{id}@gateway.aquamark.io`
2. Attaches PDF files
3. **Optional:** Include funder names in subject line (comma-separated)
4. Receives watermarked PDFs back via email

## Features

- ✅ Process multiple PDFs in one email
- ✅ Optional funder name watermarking
- ✅ Individual file attachments (not zipped)
- ✅ Automatic usage tracking
- ✅ Logo + text watermarks

## Funder Name Watermarking

**Subject line examples:**

- **No subject or blank:** Standard broker watermark only
- **"OnDeck Capital":** Creates one version with OnDeck watermark
- **"OnDeck, Kapitus, Rapid Finance":** Creates 3 versions (one per funder)

**Result:**
- 2 PDFs + 3 funders = 6 watermarked files returned
- All as individual email attachments

## Email Format

**Gateway address:** `broker-{broker_id}@gateway.aquamark.io`

**Example:**
- Broker ID: `fusion`
- Gateway email: `broker-fusion@gateway.aquamark.io`

## Environment Variables

Required:
- `SUPABASE_URL` - Supabase project URL
- `SUPABASE_KEY` - Supabase service key
- `POSTMARK_API_KEY` - Postmark server API token
- `PORT` - Server port (default: 10000)

## Database Tables

Uses:
- `broker_gateway_users` - Gateway user configuration
- `broker_monthly_usage` - Usage tracking (files + pages)

## Postmark Setup

1. Create inbound server in Postmark
2. Set webhook URL: `https://your-service.onrender.com/inbound`
3. Configure domain: `gateway.aquamark.io`

## Deployment

### Render
1. Create new web service
2. Connect GitHub repo
3. Set environment variables
4. Deploy

## API Endpoints

- `POST /inbound` - Postmark webhook (receives emails)
- `GET /health` - Health check

## Usage Tracking

Tracks in `broker_monthly_usage`:
- **file_count:** Total watermarked files (not original count)
  - Example: 2 PDFs × 3 funders = 6 files
- **page_count:** Total pages across all watermarked files

## Support

For issues or questions, contact support@aquamark.io
