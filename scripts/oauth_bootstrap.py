import http.server
import json
import os
import socketserver
import sys
import threading
import time
import urllib.parse
import webbrowser
from pathlib import Path

from dotenv import load_dotenv
from requests_oauthlib import OAuth1Session
from requests_oauthlib.oauth1_session import TokenRequestDenied

REQUEST_TOKEN_URL = "https://api.login.yahoo.com/oauth/v2/get_request_token"
AUTHORIZATION_URL = "https://api.login.yahoo.com/oauth/v2/request_auth"
ACCESS_TOKEN_URL = "https://api.login.yahoo.com/oauth/v2/get_token"

BASE_DIR = Path(__file__).resolve().parents[1]
ENV_PATH = BASE_DIR / ".env"
TOKENS_PATH = BASE_DIR / "config" / "oauth_tokens.json"


def _require_env(name):
    value = os.getenv(name, "").strip()
    if not value:
        print(f"Missing {name} in .env")
        sys.exit(1)
    return value


def _mask_value(value, show=4):
    if not value:
        return ""
    if len(value) <= show:
        return "*" * len(value)
    return f"...{value[-show:]} (len={len(value)})"


def _wait_for_callback(redirect_uri, auth_url):
    parsed = urllib.parse.urlparse(redirect_uri)
    if parsed.scheme != "http":
        print("Redirect URI must use http for the local callback.")
        sys.exit(1)

    host = parsed.hostname or "localhost"
    port = parsed.port or 80
    path = parsed.path or "/"
    result = {"oauth_verifier": ""}
    event = threading.Event()

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            req = urllib.parse.urlparse(self.path)
            if req.path != path:
                self.send_response(404)
                self.end_headers()
                return

            params = urllib.parse.parse_qs(req.query)
            verifier = params.get("oauth_verifier", [""])[0]
            if not verifier:
                self.send_response(400)
                self.end_headers()
                self.wfile.write(b"Missing oauth_verifier.")
                return

            result["oauth_verifier"] = verifier
            event.set()
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"Authorization received. You can close this window.")

        def log_message(self, format, *args):
            return

    try:
        with socketserver.TCPServer((host, port), Handler) as httpd:
            print(f"Listening for OAuth callback at {redirect_uri}")
            try:
                webbrowser.open(auth_url)
            except Exception:
                print("Open this URL in a browser:")
                print(auth_url)

            while not event.is_set():
                httpd.handle_request()
    except OSError as exc:
        print(f"Could not start local callback server: {exc}")
        print("Confirm the redirect URI and that the port is free.")
        sys.exit(1)

    return result["oauth_verifier"]

def _manual_verifier_flow(auth_url, redirect_uri):
    print("Authorize this app using the URL below.")
    print(auth_url)
    print("")
    print("After approving, your browser will redirect to:")
    print(redirect_uri)
    print("If the page fails to load, copy the oauth_verifier from the URL bar.")
    return input("Paste the oauth_verifier from the redirected URL: ").strip()


def _print_request_token_debug(consumer_key, redirect_uri):
    print("Request token failed. Double-check these settings:")
    print(f"- Consumer key: {_mask_value(consumer_key)}")
    print(f"- Redirect URI: {redirect_uri}")
    print("- App type: Confidential client")
    print("- Permission: Fantasy Sports (Read)")
    print("- Redirect URI must exactly match the app settings")


def _print_env_debug(consumer_key, consumer_secret, redirect_uri):
    print("Debug: env values detected")
    print(f"- Consumer key: {_mask_value(consumer_key)}")
    print(f"- Consumer secret: {_mask_value(consumer_secret)}")
    print(f"- Redirect URI: {redirect_uri}")
    if consumer_key != consumer_key.strip() or consumer_secret != consumer_secret.strip():
        print("- Warning: leading/trailing whitespace detected in key/secret")


def _parse_urlencoded(text):
    return dict(urllib.parse.parse_qsl(text, keep_blank_values=True))


def _fetch_request_token_via_get(oauth, url):
    response = oauth.get(url)
    if response.status_code != 200:
        auth_header = response.headers.get("WWW-Authenticate", "")
        if auth_header:
            print(f"GET WWW-Authenticate: {auth_header}")
        print(f"GET request token failed: {response.status_code} {response.text}")
        return None
    return _parse_urlencoded(response.text)


def _try_request_token(consumer_key, consumer_secret, redirect_uri, signature_method, signature_type):
    oauth = OAuth1Session(
        consumer_key,
        client_secret=consumer_secret,
        callback_uri=redirect_uri,
        signature_type=signature_type,
        signature_method=signature_method,
    )
    try:
        return oauth.fetch_request_token(REQUEST_TOKEN_URL)
    except TokenRequestDenied as exc:
        print(f"Request token via POST failed ({signature_method}, {signature_type}). Retrying with GET.")
        resp = getattr(exc, "response", None)
        if resp is not None:
            auth_header = resp.headers.get("WWW-Authenticate", "")
            if auth_header:
                print(f"POST WWW-Authenticate: {auth_header}")
            print(f"POST response: {resp.status_code} {resp.text}")
        return _fetch_request_token_via_get(oauth, REQUEST_TOKEN_URL)


def main():
    load_dotenv(ENV_PATH)

    consumer_key = _require_env("YAHOO_CONSUMER_KEY")
    consumer_secret = _require_env("YAHOO_CONSUMER_SECRET")
    redirect_uri = _require_env("YAHOO_OAUTH_REDIRECT_URI")

    _print_env_debug(consumer_key, consumer_secret, redirect_uri)

    attempts = [
        ("HMAC-SHA1", "query"),
        ("HMAC-SHA1", "auth_header"),
        ("PLAINTEXT", "query"),
        ("PLAINTEXT", "auth_header"),
    ]
    request_token = None
    for method, sig_type in attempts:
        print(f"Attempting request token with {method} + {sig_type}.")
        request_token = _try_request_token(consumer_key, consumer_secret, redirect_uri, method, sig_type)
        if request_token and "oauth_token" in request_token:
            break

    if not request_token or "oauth_token" not in request_token:
        _print_request_token_debug(consumer_key, redirect_uri)
        sys.exit(1)

    auth_url = oauth.authorization_url(AUTHORIZATION_URL)

    redirect_uri_lower = redirect_uri.lower()
    if redirect_uri_lower == "oob":
        verifier = input("Paste the oauth_verifier from Yahoo: ").strip()
    elif redirect_uri_lower.startswith("https://localhost") or redirect_uri_lower.startswith("https://127.0.0.1"):
        verifier = _manual_verifier_flow(auth_url, redirect_uri)
    else:
        verifier = _wait_for_callback(redirect_uri, auth_url)

    access_token = oauth.fetch_access_token(ACCESS_TOKEN_URL, verifier=verifier)

    payload = {
        "oauth_token": access_token.get("oauth_token"),
        "oauth_token_secret": access_token.get("oauth_token_secret"),
        "oauth_session_handle": access_token.get("oauth_session_handle"),
        "obtained_at": int(time.time()),
    }

    TOKENS_PATH.parent.mkdir(parents=True, exist_ok=True)
    TOKENS_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"Saved tokens to {TOKENS_PATH}")


if __name__ == "__main__":
    main()
