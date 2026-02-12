# Render Deployment Guide

This guide explains how to deploy the Florida Schools Dashboard to Render.

## Files Created for Deployment

1. **`app.py`** - Entry point for Render (exposes the Dash server)
2. **`Procfile`** - Tells Render how to start the app
3. **`render.yaml`** - Optional Render configuration file
4. **`.renderignore`** - Files to exclude from deployment

## Deployment Steps

### Option 1: Using Render Dashboard (Recommended)

1. **Push to GitHub**
   - Make sure all files are committed and pushed to your GitHub repository

2. **Create New Web Service on Render**
   - Go to https://dashboard.render.com
   - Click "New +" → "Web Service"
   - Connect your GitHub repository
   - Select the repository

3. **Configure the Service**
   - **Name**: `florida-schools-dashboard` (or your preferred name)
   - **Environment**: `Python 3`
   - **Build Command**: `pip install -r requirements.txt`
   - **Start Command**: `gunicorn app:app --bind 0.0.0.0:$PORT`
   - **Python Version**: `3.10` (or latest)

4. **Environment Variables** (if needed)
   - Add any required environment variables in the Render dashboard
   - The app will automatically use `output/results.csv` or `output/florida_with_ai_summary_updated.csv`

5. **Deploy**
   - Click "Create Web Service"
   - Render will build and deploy your app
   - The dashboard will be available at `https://your-app-name.onrender.com`

### Option 2: Using render.yaml

If you prefer configuration as code:

1. Push to GitHub (same as above)
2. In Render dashboard, select "New +" → "Blueprint"
3. Connect your repository
4. Render will automatically detect `render.yaml` and use those settings

## Important Notes

### Data Files

- The dashboard needs the CSV file (`output/results.csv` or `output/florida_with_ai_summary_updated.csv`)
- **You need to upload the CSV file to your repository** or use a persistent disk
- Render's free tier has ephemeral storage, so files may be lost on restart
- Consider:
  - Committing the CSV to the repo (if it's not too large)
  - Using Render's persistent disk feature
  - Loading data from an external source (S3, database, etc.)

### File Size Limits

- Render has limits on repository size
- If your CSV is very large, consider:
  - Using a database instead
  - Loading from external storage
  - Compressing the data

### Build Time

- First build may take 5-10 minutes
- Subsequent builds are faster due to caching

## Troubleshooting

### App won't start
- Check logs in Render dashboard
- Verify `gunicorn` is in `requirements.txt`
- Ensure `app.py` exists and exposes `app` variable

### CSV file not found
- Make sure the CSV is in the repository or on persistent storage
- Check the path in `map_app.py` (should be `output/results.csv`)

### Port binding errors
- Render automatically sets `$PORT` environment variable
- The `Procfile` uses `$PORT` correctly

## Local Testing

Test the deployment setup locally:

```bash
# Install gunicorn
pip install gunicorn

# Run with gunicorn (simulating Render)
gunicorn app:app --bind 0.0.0.0:8051
```

Then visit `http://localhost:8051` to verify it works.

