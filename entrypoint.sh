#!/bin/sh
set -e

# Write Google credentials file from env variable using Python
python3 -c "
import os, sys, json, base64

path = '/app/google_credentials.json'

# Support split base64 (Railway truncates long values)
b64_1 = os.environ.get('GOOGLE_CREDENTIALS_B64_1', '')
b64_2 = os.environ.get('GOOGLE_CREDENTIALS_B64_2', '')
b64 = os.environ.get('GOOGLE_CREDENTIALS_BASE64', '')
raw_json = os.environ.get('GOOGLE_CREDENTIALS_JSON', '')

if b64_1 and b64_2:
    b64 = b64_1 + b64_2
    print(f'Credentials: joined 2 parts ({len(b64_1)} + {len(b64_2)} = {len(b64)} chars)')

if b64:
    data = base64.b64decode(b64)
    info = json.loads(data)
    with open(path, 'w') as f:
        json.dump(info, f, indent=2)
    print(f'Credentials decoded: {len(b64)} b64 chars -> {len(data)} bytes')
elif raw_json:
    info = json.loads(raw_json)
    with open(path, 'w') as f:
        json.dump(info, f, indent=2)
    print(f'Credentials from JSON: {len(raw_json)} chars')
elif not os.path.exists(path):
    print('WARNING: No Google credentials found', file=sys.stderr)
    sys.exit(0)

if os.path.exists(path):
    with open(path) as f:
        d = json.load(f)
    pk = d.get('private_key', '')
    ok = pk.startswith('-----BEGIN') and pk.rstrip().endswith('-----')
    print(f'File OK: {os.path.getsize(path)}B, PEM={ok}, email={d.get(\"client_email\",\"?\")}')
    if not ok:
        print('ERROR: PEM key invalid!', file=sys.stderr)
        sys.exit(1)
"

exec python -m src.bot.main
