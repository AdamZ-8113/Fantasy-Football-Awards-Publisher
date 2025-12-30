# Tech Stack

## Backend and data
- Python 3.11+ (scripts in `scripts/`)
- requests / requests-oauthlib
- python-dotenv
- lxml
- pandas
- SQLite database at `data/processed/fantasy_insights.sqlite`

## Frontend
- Static HTML/CSS/JS in `site/`
- Data in `site/data/*.json`
- Fonts via Google Fonts (Space Grotesk, Unbounded)

## Hosting
- GitHub Pages (gh-pages branch)
- Optional Cloudflare proxy in front of GitHub Pages

## Tooling
- VS Code tasks in `.vscode/tasks.json`
- Git subtree for publishing `site/` to gh-pages
