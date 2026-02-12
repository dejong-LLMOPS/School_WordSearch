"""Render deployment entry point for Dash dashboard."""
import os
from map_app import create_app
from utils.logging_config import setup_logging

# Setup logging
setup_logging()

# Create the Dash app
dash_app = create_app()

# Expose the server for gunicorn
app = dash_app.server

# For local testing
if __name__ == "__main__":
    port = int(os.environ.get('PORT', 8051))
    dash_app.run(host='0.0.0.0', port=port, debug=False)

