import {recording, blockSources, fetchedBytes} from './playground-model.mjs';
const el = id => document.getElementById(id);
const state = {query: 0, effort: 'balanced', tier: 'cold', fresh: false};
function element(tag, className, text) {
  const node = document.createElement(tag);
  node.className = className;
  if (text !== undefined) node.textContent = text;
  return node;
}
async function start() {
  const response = await fetch(new URL('./explorer-data.json', import.meta.url));
  if (!response.ok) throw new Error('Query records unavailable');
  const data = await response.json();
  function render() {
    const run = recording(data, state);
    el('pg-probes').textContent = `${run.probed.length} / ${data.centroids} clusters`;
    el('pg-clusters').replaceChildren(...Array.from({length: data.centroids}, (_, id) => {
      const node = element('span', `pg-cluster${run.probed.includes(id) ? ' active' : ''}`, String(id).padStart(2, '0'));
      node.title = `Cluster ${id}: ${run.probed.includes(id) ? 'probed' : 'not probed'}`;
      return node;
    }));
    el('pg-block-count').textContent = `${run.selected.length} / ${data.blocks.length} blocks`;
    el('pg-blocks').replaceChildren(...blockSources(data, run).map((block, index) => {
      const node = element('span', `pg-block ${block.source}`);
      node.style.setProperty('--delay', `${Math.min(index * 10, 350)}ms`);
      const description = `Block ${block.id} · cluster ${block.cluster} · ${block.rows} rows · ${block.source === 'object' ? 'object storage' : block.source}`;
      node.title = description;
      node.setAttribute('aria-label', description);
      node.setAttribute('role', 'img');
      return node;
    }));
    el('pg-results').replaceChildren(...run.hits.map(hit => {
      const row = element('li', hit.tail ? 'pg-new' : '');
      row.append(element('span', 'pg-id', `#${hit.id}`), element('span', 'pg-match', hit.tail ? 'NEW' : hit.exact ? '✓' : ''), element('span', 'pg-distance', hit.distance.toFixed(4)));
      row.title = hit.tail ? 'Acknowledged vector in the log tail' : hit.exact ? 'Matches an exact top-5 neighbor' : 'Approximate neighbor';
      return row;
    }));
    el('pg-recall').textContent = `${Math.round(run.recall * 100)}%`;
    el('pg-reads').textContent = run.requests;
    el('pg-bytes').textContent = fetchedBytes(run.bytes);
    el('pg-hits').textContent = run.ram_hits + run.ssd_hits;
    el('pg-story').textContent = state.tier === 'cold'
      ? `${run.requests} range reads fetch ${run.selected.length} blocks. The remaining blocks stay in object storage.`
      : `${run.selected.length} blocks read from ${state.tier === 'ssd' ? 'SSD' : 'RAM'}. ${run.requests === 0 ? 'The query makes zero object-store reads.' : `${run.requests} object-store reads remain.`}`;
    el('pg-fresh').setAttribute('aria-pressed', String(state.fresh));
    el('pg-fresh').textContent = state.fresh ? '↶ Reset fresh write' : '+ Add a fresh vector';
    el('pg-tail').textContent = state.fresh
      ? `Write acknowledged at sequence ${run.sequence}. The new vector is already the nearest neighbor.`
      : 'New writes join the search from the in-memory log tail. ✓ marks an exact top-5 match; scores are squared L2.';
    document.querySelectorAll('[data-tier]').forEach(button => button.setAttribute('aria-pressed', String(button.dataset.tier === state.tier)));
  }
  el('pg-query').addEventListener('change', event => { state.query = Number(event.target.value); render(); });
  el('pg-effort').addEventListener('change', event => { state.effort = event.target.value; render(); });
  document.querySelectorAll('[data-tier]').forEach(button => button.addEventListener('click', () => { state.tier = button.dataset.tier; render(); }));
  el('pg-fresh').addEventListener('click', () => { state.fresh = !state.fresh; render(); });
  render();
  el('pg-app').hidden = false;
}
start().catch(() => { el('pg-error').hidden = false; });
