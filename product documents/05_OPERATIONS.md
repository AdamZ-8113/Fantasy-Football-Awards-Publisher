# Operations and Publishing

## GitHub Pages (gh-pages)
The static site is published from `site/` to the `gh-pages` branch using git subtree.

Publish updates:
```
git add .
git commit -m "Refresh data"
git push
git subtree push --prefix site origin gh-pages
```

## Cloudflare proxy
If Cloudflare is in front of GitHub Pages, caching can delay updates.
Troubleshooting:
- Purge cache for `index.html` and `app.js` after deploy.
- Confirm latest `app.js` via:
  https://raw.githubusercontent.com/AdamZ-8113/Fantasy-Football-Awards-Publisher/gh-pages/app.js

## Verification checklist
- Season picker includes latest season.
- Team picker works and hides League Overview on team view.
- "The Schedule Screwed Me" description matches latest copy.
