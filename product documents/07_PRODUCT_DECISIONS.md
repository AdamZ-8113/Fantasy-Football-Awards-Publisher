# Product Decisions Log

## Data storage
- Keep raw XML snapshots in `data/raw/` for replay and audits.
- Normalize into SQLite (`data/processed/fantasy_insights.sqlite`).

## OAuth
- Prefer OAuth 2.0 for Yahoo; keep OAuth 1.0a as fallback.

## Awards and sections
- Removed: Snatched Victory, Stack Enjoyer, Vulture Victim, Auto Draft Menace, Tinkerer (no longer displayed).
- Added: Always the Bridesmaid, Commitment Issues, Why Don't He Want Me, Man?
- Injury-based award ("Intensive Care Unit") removed from UI for now, data can be revisited later.

## Team-specific insights
- Team picker added.
- Team view hides the League Overview section to focus on manager-specific awards.

## Frontend themes
- Dark theme default with Ember, Glacier, and a Light option.
- Reduced background gradients and removed lens flare effects.

## Deployment
- GitHub Pages with gh-pages branch using git subtree.
- Cloudflare proxy in front of GitHub Pages; cache purges may be needed.
