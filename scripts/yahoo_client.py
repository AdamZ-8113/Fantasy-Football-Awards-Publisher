import json
import os
from pathlib import Path

import tomllib
import requests
from dotenv import load_dotenv
from requests_oauthlib import OAuth1Session
from xml.etree import ElementTree

BASE_URL = "https://fantasysports.yahooapis.com/fantasy/v2"
TOKEN_URL = "https://api.login.yahoo.com/oauth2/get_token"

BASE_DIR = Path(__file__).resolve().parents[1]
ENV_PATH = BASE_DIR / ".env"
TOKENS_PATH = BASE_DIR / "config" / "oauth_tokens.json"
CONFIG_PATH = BASE_DIR / "config" / "config.toml"


def load_env():
    load_dotenv(ENV_PATH)


def load_config():
    if not CONFIG_PATH.exists():
        return {}
    with CONFIG_PATH.open("rb") as f:
        return tomllib.load(f)


def load_tokens():
    if not TOKENS_PATH.exists():
        raise FileNotFoundError("Missing config/oauth_tokens.json. Run scripts/oauth_bootstrap.py first.")
    return json.loads(TOKENS_PATH.read_text(encoding="utf-8"))


def _is_oauth2(tokens):
    return tokens.get("oauth_version") == "2.0" or "access_token" in tokens


def _save_tokens(tokens):
    TOKENS_PATH.write_text(json.dumps(tokens, indent=2), encoding="utf-8")


def _refresh_oauth2_token(tokens):
    client_id = os.getenv("YAHOO_CONSUMER_KEY", "").strip()
    client_secret = os.getenv("YAHOO_CONSUMER_SECRET", "").strip()
    refresh_token = tokens.get("refresh_token")
    if not client_id or not client_secret or not refresh_token:
        raise RuntimeError("Missing OAuth2 refresh prerequisites in .env or token file.")

    response = requests.post(
        TOKEN_URL,
        data={
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        },
        auth=(client_id, client_secret),
    )
    response.raise_for_status()
    updated = response.json()
    updated["oauth_version"] = "2.0"
    tokens.update(updated)
    _save_tokens(tokens)
    return tokens


def get_oauth_session():
    load_env()
    tokens = load_tokens()

    consumer_key = os.getenv("YAHOO_CONSUMER_KEY", "").strip()
    consumer_secret = os.getenv("YAHOO_CONSUMER_SECRET", "").strip()
    if not consumer_key or not consumer_secret:
        raise RuntimeError("Missing YAHOO_CONSUMER_KEY/YAHOO_CONSUMER_SECRET in .env")

    if _is_oauth2(tokens):
        session = requests.Session()
        session.headers.update({"Authorization": f"Bearer {tokens.get('access_token', '')}"})
        return session

    return OAuth1Session(
        consumer_key,
        client_secret=consumer_secret,
        resource_owner_key=tokens.get("oauth_token"),
        resource_owner_secret=tokens.get("oauth_token_secret"),
        signature_type="query",
    )


def api_get_response(path, params=None):
    session = get_oauth_session()
    if not path.startswith("/"):
        path = "/" + path
    url = f"{BASE_URL}{path}"
    response = session.get(url, params=params)

    if response.status_code == 401:
        tokens = load_tokens()
        if _is_oauth2(tokens) and tokens.get("refresh_token"):
            _refresh_oauth2_token(tokens)
            session = get_oauth_session()
            response = session.get(url, params=params)

    return response


def api_get(path, params=None):
    response = api_get_response(path, params=params)
    response.raise_for_status()
    return response.content


def parse_xml(xml_bytes):
    return ElementTree.fromstring(xml_bytes)


def strip_ns(tag):
    return tag.split("}", 1)[-1]


def find_child_text(elem, name):
    for child in elem:
        if strip_ns(child.tag) == name:
            return (child.text or "").strip()
    return ""
