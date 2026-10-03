import {recording, blockSources, fetchedBytes} from './playground-model.mjs';
const el = id => document.getElementById(id);
const state = {query: 0, effort: 'wide', tier: 'cold'};
function element(tag, className, text) {
  const node = document.createElement(tag);
  node.className = className;
  if (text !== undefined) node.textContent = text;
  return node;
}
async function start() {
  const response = await fetch(new URL('./explorer-data.json', import.meta.url));
  if (!response.ok) throw new Error('Search records unavailable');
  const data = await response.json();
  const documents = new Map(data.documents.map(doc => [doc.id, doc]));
  function render() {
    const run = recording(data, state);
    const cold = recording(data, {...state, tier: 'cold'});
    el('pg-dataset').textContent = `${data.rows} documents`;
    el('pg-question').textContent = data.queries[state.query];
    el('pg-result-note').textContent = `${run.hits.length} ${run.hits.length === 1 ? 'match' : 'matches'}`;
    el('pg-results').replaceChildren(...run.hits.map((hit, index) => {
      const doc = documents.get(hit.id);
      const row = element('li', '');
      row.style.setProperty('--delay', `${index * 40}ms`);
      const meta = element('div', 'pg-doc-meta');
      meta.append(element('span', '', doc.category));
      const link = element('a', '', doc.title+' ↗');
      link.href = doc.url;
      link.title = `Cosine distance: ${hit.distance.toFixed(3)}`;
      row.append(meta, link, element('p', '', doc.text));
      return row;
    }));
    const source = state.tier === 'cold' ? 'Object storage' : state.tier === 'ssd' ? 'SSD cache' : 'RAM cache';
    el('pg-source').className = `pg-source ${state.tier}`;
    el('pg-source-name').textContent = source;
    el('pg-pass').textContent = state.tier === 'cold' ? 'FIRST SEARCH' : 'REPEAT SEARCH';
    el('pg-read-fill').style.width = `${cold.bytes ? Math.min(100, run.bytes / cold.bytes * 100) : 0}%`;
    el('pg-reads').textContent = run.requests;
    el('pg-bytes').textContent = fetchedBytes(run.bytes);
    el('pg-story').textContent = state.tier === 'cold'
      ? 'Glider fetches the selected document blocks and keeps them in the cache. Run this question again.'
      : run.requests === 0 ? 'The document blocks are cached. This search needs no download from object storage.' : 'Cached blocks are reused. The remaining blocks are fetched from object storage.';
    el('pg-run').textContent = state.tier === 'cold' ? 'Run again →' : 'Run again ✓';
    el('pg-reset').hidden = state.tier === 'cold';
    el('pg-probes').textContent = `${run.probed.length} / ${data.centroids}`;
    el('pg-clusters').replaceChildren(...Array.from({length:data.centroids}, (_, id) => {
      const node = element('span', `pg-cluster${run.probed.includes(id) ? ' active' : ''}`, String(id).padStart(2, '0'));
      node.title = `Cluster ${id}: ${run.probed.includes(id) ? 'probed' : 'not probed'}`;
      return node;
    }));
    el('pg-block-count').textContent = `${run.selected.length} / ${data.blocks.length}`;
    el('pg-blocks').replaceChildren(...blockSources(data, run).map(block => {
      const node = element('span', `pg-block ${block.source}`);
      const label = `Block ${block.id}: ${block.rows} documents, ${block.source}`;
      node.title = label; node.setAttribute('role','img'); node.setAttribute('aria-label',label);
      return node;
    }));
    el('pg-quality').textContent = `Recall@3: ${Math.round(run.recall * 100)}% against exact search. Cache hits: ${run.ram_hits + run.ssd_hits}. Scores are cosine distances, not confidence percentages.`;
    document.querySelectorAll('[data-query]').forEach(button => button.setAttribute('aria-pressed', String(Number(button.dataset.query) === state.query)));
    document.querySelectorAll('[data-tier]').forEach(button => button.setAttribute('aria-pressed', String(button.dataset.tier === state.tier)));
  }
  document.querySelectorAll('[data-query]').forEach(button => button.addEventListener('click', () => { state.query = Number(button.dataset.query); state.tier = 'cold'; render(); }));
  el('pg-run').addEventListener('click', () => { state.tier = 'ram'; render(); });
  el('pg-reset').addEventListener('click', () => { state.tier = 'cold'; render(); });
  el('pg-effort').addEventListener('change', event => { state.effort = event.target.value; state.tier = 'cold'; render(); });
  document.querySelectorAll('[data-tier]').forEach(button => button.addEventListener('click', () => { state.tier = button.dataset.tier; render(); }));
  render(); el('pg-app').hidden = false;
}
start().catch(() => { el('pg-error').hidden = false; });
