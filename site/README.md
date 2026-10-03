# Glider website

Static HTML, CSS and JavaScript; no package install or build step. Serve or
deploy this directory with its assets intact. For a local preview, from the
repository root:

```sh
python3 -m http.server 8765 --bind 127.0.0.1 --directory site
```

Open `http://127.0.0.1:8765/`. The playground illustrates exact search on a
fixed synthetic 2D dataset in the browser. It does not run the Rust engine,
create embeddings, or measure ANN quality, storage I/O or engine latency.
Its exported commands load the same points into metric-specific collections
on a real server. `playground-model.mjs` owns the data, scoring and exports.

Check the model with Node.js 18 or later, and compare it against the current
Glider server (including executing the exported curl commands):

```sh
node --check site/playground.mjs
node --test site/playground.test.mjs
cargo build --release --locked --features server --bin glider-server
GLIDER_SERVER_BIN=target/release/glider-server \
  python3 -m unittest discover -s tests -p 'test_site_playground.py'
```

CI runs both checks. The server comparison uses a temporary local namespace
and checks IDs, metadata and distances across all three metrics and the demo
filters. The browser layout and interactions still need visual verification
when changing the UI.
