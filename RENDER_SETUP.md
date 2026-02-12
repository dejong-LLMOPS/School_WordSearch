# Render Deployment Setup - Dashboard Only

## Quick Answer: Publish Directory

**Leave it BLANK** - Dash apps don't need a publish directory. That field is for static sites (React, Vue, etc.). Dash runs directly from Python.

## Render Configuration

When creating your web service on Render, use these settings:

### Basic Settings
- **Name**: `florida-schools-dashboard` (or your choice)
- **Environment**: `Python 3`
- **Region**: Choose closest to you
- **Branch**: `main` (or your default branch)

### Build & Deploy
- **Build Command**: `pip install -r requirements.txt`
- **Start Command**: `gunicorn app:app --bind 0.0.0.0:$PORT`
- **Publish Directory**: **LEAVE BLANK** (empty)

### Python Version
- **Python Version**: `3.10` or `3.11`

## What Gets Deployed

Only the dashboard files:
- `app.py` (entry point)
- `map_app.py` (dashboard code)
- `config.py` (configuration)
- `utils/` (utilities)
- `requirements.txt` (dependencies)

**NOT deployed** (excluded by .gitignore or .renderignore):
- Scraper files (`main.py`, `web_scraper.py`, etc.) - still in repo but not needed
- Test files
- Excel files
- Cache files
- Output files (except CSV if you commit it)

## Important: CSV Data File

The dashboard needs the CSV file. You have two options:

### Option 1: Commit CSV to Repository (Simplest)
```bash
# Add the CSV file (if not too large)
git add output/results.csv
# OR
git add output/florida_with_ai_summary_updated.csv
git commit -m "Add CSV data for dashboard"
git push
```

### Option 2: Use Persistent Disk (Better for large files)
- In Render dashboard, enable "Persistent Disk"
- Upload CSV file to the persistent disk after first deployment
- Update `map_app.py` to read from persistent disk path

## Testing Locally

Before deploying, test with gunicorn:

```bash
pip install gunicorn
gunicorn app:app --bind 0.0.0.0:8051
```

Visit `http://localhost:8051` to verify it works.

## Deployment Checklist

- [ ] `app.py` exists and exposes `app` variable
- [ ] `Procfile` exists with correct start command
- [ ] `requirements.txt` includes `gunicorn`
- [ ] CSV file is in repository OR plan to use persistent disk
- [ ] All files committed and pushed to GitHub
- [ ] Render service configured (Publish Directory = blank)

