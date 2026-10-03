# Glider website

A static landing page: `index.html` (inline CSS and a small inline script for
the copy buttons), `runtime.svg` (copied from `docs/architecture/runtime.svg`)
and `favicon.svg`. There is no build step and no external requests, so it also
works offline once loaded.

Preview locally:

```sh
cd site && python3 -m http.server 8000
```

Keep `runtime.svg` in sync when `docs/architecture/runtime.svg` changes. Update
the image tag, the performance table and the "New in 1.1" labels when a
release changes them; every number must come from `README.md` or `benchmarks/`.

## Deploy on GitHub Pages

Under **Settings -> Pages**, set the source to **GitHub Actions**, then add
`.github/workflows/pages.yml`:

```yaml
name: pages
on:
  push:
    branches: [main]
    paths: [site/**]
  workflow_dispatch:
permissions:
  contents: read
  pages: write
  id-token: write
concurrency:
  group: pages
  cancel-in-progress: true
jobs:
  deploy:
    runs-on: ubuntu-latest
    environment:
      name: github-pages
      url: ${{ steps.deployment.outputs.page_url }}
    steps:
      - uses: actions/checkout@v4
      - uses: actions/configure-pages@v5
      - uses: actions/upload-pages-artifact@v3
        with:
          path: site
      - id: deployment
        uses: actions/deploy-pages@v4
```

The page uses only relative links, so it works under the project path
`https://<user>.github.io/glider/`.

## Deploy on Cloudflare Pages

Connect the repository and set:

- Framework preset: None
- Build command: (empty)
- Build output directory: `site`
