import json
import os
import sys
import time
import urllib.parse
import webbrowser
from pathlib import Path

from dotenv import load_dotenv
from requests_oauthlib import OAuth2Session

AUTH_URL = "https://api.login.yahoo.com/oauth2/request_auth"
TOKEN_URL = "https://api.login.yahoo.com/oauth2/get_token"

BASE_DIR = Path(__file__).resolve().parents[1]
ENV_PATH = BASE_DIR / ".env"
TOKENS_PATH = BASE_DIR / "config" / "oauth_tokens.json"


def _require_env(name):
    value = os.getenv(name, "").strip()
    if not value:
        print(f"Missing {name} in .env")
        sys.exit(1)
    return value


def _prompt_for_code(auth_url, redirect_uri):
    print("Authorize this app using the URL below.")
    print(auth_url)
    try:
        webbrowser.open(auth_url)
    except Exception:
        pass

    print("\nAfter approving, you will be redirected to:")
    print(redirect_uri)
    print("If the page fails to load, copy the full URL from the address bar.")
    raw = input("Paste the full redirect URL (or just the code): ").strip()

    if raw.startswith("http"):
        parsed = urllib.parse.urlparse(raw)
        params = urllib.parse.parse_qs(parsed.query)
        return params.get("code", [""])[0]
    if "code=" in raw or "state=" in raw or "&" in raw:
        params = urllib.parse.parse_qs(raw)
        return params.get("code", [""])[0] or raw.split("&", 1)[0]
    return raw


def main():
    load_dotenv(ENV_PATH)

    client_id = _require_env("YAHOO_CONSUMER_KEY")
    client_secret = _require_env("YAHOO_CONSUMER_SECRET")
    redirect_uri = _require_env("YAHOO_OAUTH_REDIRECT_URI")
    scope = os.getenv("YAHOO_OAUTH_SCOPE", "fspt-r").strip()

    scope_list = scope.split() if scope else None

    oauth = OAuth2Session(client_id=client_id, redirect_uri=redirect_uri, scope=scope_list)
    auth_url, _state = oauth.authorization_url(AUTH_URL)

    code = _prompt_for_code(auth_url, redirect_uri)
    if not code:
        print("Missing authorization code.")
        sys.exit(1)

    token = oauth.fetch_token(
        TOKEN_URL,
        code=code,
        client_id=client_id,
        client_secret=client_secret,
    )

    token["oauth_version"] = "2.0"
    token["obtained_at"] = int(time.time())

    TOKENS_PATH.parent.mkdir(parents=True, exist_ok=True)
    TOKENS_PATH.write_text(json.dumps(token, indent=2), encoding="utf-8")
    print(f"Saved tokens to {TOKENS_PATH}")


if __name__ == "__main__":
    main()
