# OAuth Setup (Yahoo Fantasy API)

## Redirect URI
Set a local callback in the Yahoo app settings:
- `https://localhost:8080/callback`

Then set the same value in `.env`:
- `YAHOO_OAUTH_REDIRECT_URI=https://localhost:8080/callback`

## Bootstrap (OAuth 1.0a)
1) Activate your venv and install dependencies.
2) Run the bootstrap script:
   - `python scripts/oauth_bootstrap.py`
3) Approve the app in the browser window that opens.
4) If the redirect page fails to load, copy `oauth_verifier` from the URL bar.
5) The script writes tokens to `config/oauth_tokens.json`.

If the browser does not open, copy/paste the printed authorization URL manually.

## Bootstrap (OAuth 2.0 fallback)
If OAuth 1.0a continues to return 401s, Yahoo may be enforcing OAuth 2.0 for your app.
Use this flow:
1) Ensure your Redirect URI matches the Yahoo app settings.
2) Set scope in `.env` if needed:
   - `YAHOO_OAUTH_SCOPE=fspt-r`
3) Run:
   - `python scripts/oauth2_bootstrap.py`
4) Approve the app and paste the redirect URL or code.
5) Tokens will be saved to `config/oauth_tokens.json`.
