# Glider website

A static landing page: `index.html` (inline CSS, inline SVG for the paper
plane and icons, and a small inline script for the code tabs and copy
buttons), `runtime.svg` (copied from `docs/architecture/runtime.svg`, linked as
the detailed diagram) and `favicon.svg`. There is no build step. The only
external request is the Google Fonts stylesheet (Archivo and IBM Plex Mono);
without it the page falls back to system fonts. Colors follow the brand image
`glider.png` (navy, wing-tip orange, paper white).

Preview locally:

```sh
cd site && python3 -m http.server 8000
```

Keep `runtime.svg` in sync when `docs/architecture/runtime.svg` changes. The
page presents features without version labels and uses the
`ghcr.io/omerfeyzioglu/glider:latest` image. Every number must come from
`README.md` or `benchmarks/`; the results band rounds them and links to the
full benchmarks.

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
