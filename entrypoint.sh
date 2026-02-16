#!/bin/sh
set -e

# Write Google credentials file from env variable using Python
# (more reliable than bash base64 -d for PEM key handling)
python3 -c "
import os, sys, json, base64

path = '/app/google_credentials.json'

b64 = os.environ.get('GOOGLE_CREDENTIALS_BASE64', '')
raw_json = os.environ.get('GOOGLE_CREDENTIALS_JSON', '')

if b64:
    data = base64.b64decode(b64)
    info = json.loads(data)
    with open(path, 'w') as f:
        json.dump(info, f, indent=2)
    pk = info.get('private_key', '')
    print(f'Credentials from base64: {len(b64)} chars -> {len(data)} bytes, PK len={len(pk)}')
elif raw_json:
    info = json.loads(raw_json)
    with open(path, 'w') as f:
        json.dump(info, f, indent=2)
    print(f'Credentials from JSON env var: {len(raw_json)} chars')
else:
    if os.path.exists(path):
        print(f'Using existing credentials file: {path}')
    else:
        print('WARNING: No Google credentials found', file=sys.stderr)

# Verify the file
if os.path.exists(path):
    with open(path) as f:
        d = json.load(f)
    pk = d.get('private_key', '')
    ok = pk.startswith('-----BEGIN') and pk.strip().endswith('-----')
    print(f'Credentials file OK: {os.path.getsize(path)} bytes, PEM valid={ok}, email={d.get(\"client_email\",\"?\")}')
"

exec python -m src.bot.main
