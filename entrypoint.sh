#!/bin/sh
set -e

# Write Google credentials file from env variable before starting the bot.
# This avoids PEM escaping issues when passing JSON through env vars.
if [ -n "$GOOGLE_CREDENTIALS_BASE64" ]; then
    echo "$GOOGLE_CREDENTIALS_BASE64" | base64 -d > /app/google_credentials.json
    echo "Google credentials decoded from GOOGLE_CREDENTIALS_BASE64"
elif [ -n "$GOOGLE_CREDENTIALS_JSON" ]; then
    printf '%s' "$GOOGLE_CREDENTIALS_JSON" > /app/google_credentials.json
    echo "Google credentials written from GOOGLE_CREDENTIALS_JSON"
fi

exec python -m src.bot.main
