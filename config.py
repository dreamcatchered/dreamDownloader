import os

BOT_TOKEN = os.environ.get("BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")

# Proxy format: http://user:pass@host:port
PROXY_URL = os.environ.get("PROXY_URL", "http://user:pass@host:port")

# For aiogram, we might need the proxy settings split or as a URL
PROXY_HOST = os.environ.get("PROXY_HOST", "your_proxy_host")
PROXY_PORT = int(os.environ.get("PROXY_PORT", "9068"))
PROXY_USER = os.environ.get("PROXY_USER", "your_proxy_user")
PROXY_PASS = os.environ.get("PROXY_PASS", "your_proxy_pass")

# Enable/disable cleanup after file upload
# If False, files will not be deleted after sending to Telegram
ENABLE_CLEANUP = True

# Enable/disable proxy usage
# If False, connections will be made directly without proxy
USE_PROXY = True

# Enable/disable Flask API website
# If False, API website will not be started
ENABLE_API = True

