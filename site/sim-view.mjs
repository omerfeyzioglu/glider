import {
  createSimulation, sampleDataset, query, QUERY_PRESETS, QUERY_ANIMATION_MS, queryAnimationProgress, writeBatch,
  seal, crash, beginRestart, restartStep, RESTART_PHASES, warmStep, sizes, MiB,
} from './sim-model.mjs';

const el = id => document.getElementById(`sim-${id}`);
const app = el('app'), canvas = el('map'), ctx = canvas.getContext('2d');
const layer = document.createElement('canvas'), ink = layer.getContext('2d');
const motion = matchMedia('(prefers-reduced-motion: reduce)');
const palette = ['#F4A261', '#81B9C5', '#E9C46A', '#91B994', '#D2A3BE', '#B7C6E7', '#F7F4EE', '#76BFC0'];
const presets = QUERY_PRESETS;
let state, sample, writeDots = [], queryPoint = null, activeClusters = [], results = [], plan = null;
let busy = false, streaming = false, epoch = 0, frame = 0, width = 1, height = 1;
let recentBlocks = [], objectCounts = new Map(), queryAnimation = null;
const number = n => n.toLocaleString('en-US');
function bytes(n) { return n >= MiB ? `${(n / MiB).toFixed(1)} MiB` : n >= 1024 ? `${(n / 1024).toFixed(1)} KiB` : `${n} B`; }
function node(tag, className, text) {
  const element = document.createElement(tag); element.className = className;
  if (text !== undefined) element.textContent = text;
  return element;
}
const say = text => { el('caption').textContent = text; };
const wait = ms => motion.matches ? Promise.resolve() : new Promise(resolve => setTimeout(resolve, ms));
function phase(index) {
  document.querySelectorAll('[data-phase]').forEach(n => n.classList.toggle('active', +n.dataset.phase === index));
}
function glow(tier) {
  for (const name of ['ram', 'ssd', 's3']) el(name).classList.toggle('lit', tier === name);
}
function drawBase() {
  ink.clearRect(0, 0, width, height);
  ink.fillStyle = '#16304F'; ink.fillRect(0, 0, width, height);
  for (let colour = 0; colour < palette.length; colour++) {
    ink.fillStyle = palette[colour]; ink.globalAlpha = .75;
    for (const p of sample) if (p.cluster % palette.length === colour) ink.fillRect(p.x * width, p.y * height, 1.4, 1.4);
  }
  ink.globalAlpha = .65;
  for (const c of state.dataset.centroids) {
    ink.strokeStyle = palette[c.id % palette.length]; ink.lineWidth = 1;
    ink.beginPath(); ink.arc(c.x * width, c.y * height, state.dataset.centroids.length > 500 ? 2.2 : 3.5, 0, 2 * Math.PI); ink.stroke();
  }
  ink.globalAlpha = 1;
}
function draw() {
  frame = 0;
  ctx.clearRect(0, 0, width, height);
  ctx.drawImage(layer, 0, 0, layer.width, layer.height, 0, 0, width, height);
  for (const id of activeClusters) {
    const c = state.dataset.centroids[id];
    ctx.fillStyle = '#F07A1A20'; ctx.strokeStyle = '#F4A261'; ctx.lineWidth = 1.4;
    ctx.beginPath(); ctx.ellipse(c.x * width, c.y * height, c.radius * width + 5, c.radius * height + 5, 0, 0, 2 * Math.PI); ctx.fill(); ctx.stroke();
    ctx.strokeRect(c.x * width - 4, c.y * height - 4, 8, 8);
  }
  ctx.fillStyle = '#F07A1A';
  // At most 6,400 visible recent writes; all acknowledged IDs remain in S3.
  for (const p of writeDots) { ctx.beginPath(); ctx.arc(p.x * width, p.y * height, 1.8, 0, 2 * Math.PI); ctx.fill(); }
  if (queryPoint) {
    ctx.strokeStyle = '#F7F4EE'; ctx.lineWidth = 1.4;
    for (const p of results) {
      ctx.beginPath(); ctx.moveTo(queryPoint.x * width, queryPoint.y * height); ctx.lineTo(p.x * width, p.y * height); ctx.stroke();
      ctx.fillStyle = '#F07A1A'; ctx.beginPath(); ctx.arc(p.x * width, p.y * height, 4, 0, Math.PI * 2); ctx.fill(); ctx.stroke();
    }
    const x = queryPoint.x * width, y = queryPoint.y * height;
    ctx.strokeStyle = '#F7F4EE'; ctx.fillStyle = '#F07A1A'; ctx.lineWidth = 2;
    ctx.beginPath(); ctx.arc(x, y, 7, 0, Math.PI * 2); ctx.fill(); ctx.stroke();
    ctx.beginPath(); ctx.moveTo(x - 13, y); ctx.lineTo(x + 13, y); ctx.moveTo(x, y - 13); ctx.lineTo(x, y + 13); ctx.stroke();
  }
}
function invalidate() { if (!frame) frame = requestAnimationFrame(draw); }
function resize() {
  const box = canvas.getBoundingClientRect(), ratio = Math.min(devicePixelRatio || 1, 2);
  width = box.width; height = box.height;
  for (const surface of [canvas, layer]) { surface.width = Math.round(width * ratio); surface.height = Math.round(height * ratio); }
  ctx.setTransform(ratio, 0, 0, ratio, 0, 0); ink.setTransform(ratio, 0, 0, ratio, 0, 0);
  drawBase(); invalidate();
}
function object(kind, title, count) {
  const previous = objectCounts.get(kind) ?? count;
  const card = node('div', `sim-object${count === 0 ? ' empty' : ''}${count > previous ? ' arrived' : ''}`);
  card.dataset.kind = kind;
  const icon = node('i', 'sim-stack'); icon.setAttribute('aria-hidden', 'true');
  card.append(icon, node('strong', '', title), node('span', 'sim-count-badge', number(count)));
  objectCounts.set(kind, count); return card;
}
function renderInventory() {
  const s = sizes(state), root = state.s3.root, tail = state.ram?.tail.size ?? 0;
  el('count').textContent = `${number(state.dataset.count + state.acknowledged)} vectors`;
  el('status').textContent = state.status === 'running' ? 'PROCESS ONLINE' : state.status === 'crashed' ? 'PROCESS KILLED' : 'RECOVERING';
  el('offline').hidden = state.status === 'running';
  el('offline').firstElementChild.textContent = state.status === 'crashed' ? 'Process killed.' : 'Rebuilding memory…';
  el('s3-size').textContent = bytes(s.s3);
  const objects = [
    ['logs', 'Writes', 'sglog · v1 batches', state.s3.logs.size, s.logs],
    ['packs', 'Data packs', 'sgpack · blocks ≤120 KiB', state.s3.packs.length, s.packs],
    ['runs', 'Indexes', 'sgindex / sgmanifest', state.s3.runs * 2, s.indexes + s.manifests],
    ['root', 'Root', 'sgroot · v5', 1, s.roots],
    ['routing', 'Routing', 'sgcentroid / sgcluster', 2, s.centroids + s.catalog],
    ['lease', 'Ownership', 'sglease / permanent sglog + sgroot fences', 1 + state.s3.fences.length * 2, s.fences + 256],
  ];
  el('objects').replaceChildren(...objects.map(([kind, title, , count]) => object(kind, title, count)));
  el('object-details').replaceChildren(...objects.map(([, title, format, count, size]) =>
    node('p', '', `${title}: ${format} · ${number(count)} objects · ${bytes(size)}`)),
    node('p', '', `${number(state.s3.blocks.length)} blocks · selected root generation ${root.generation}`));
  el('s3-note').textContent = `${number(state.acknowledged)} new upserts acknowledged · ${state.s3.logs.size} / 32 log batches before seal.`;
  el('ssd-size').textContent = bytes(state.ssd.bytes);
  el('ssd-count').textContent = `${number(state.ssd.entries.size)} cached blocks`;
  // 96 visual slots aggregate the real block entries (not 96 physical blocks).
  const fraction = state.ssd.bytes / state.ssd.capacity, filled = Math.ceil(fraction * 96);
  const cached = [...state.ssd.entries.keys()];
  el('ssd-slots').replaceChildren(...Array.from({length: 96}, (_, i) => {
    const slot = node('i', `sim-slot${i < filled ? ' full' : ''}${i < filled && recentBlocks.includes(cached[Math.floor(i / Math.max(1, filled) * cached.length)]) ? ' hit' : ''}`);
    slot.setAttribute('aria-hidden', 'true'); return slot;
  }));
  el('ssd-slots').setAttribute('aria-label', `SSD ${Math.round(fraction * 100)} percent full, ${state.ssd.entries.size} cached blocks`);
  const warmStatus = el('warm').checked ? (state.warmCursor >= state.s3.blocks.length ? 'Warm-up complete; ' : state.ssd.capacity - state.ssd.bytes < 120 * 1024 ? 'Warm-up capacity reached; ' : 'Warming in idle units; ') : '';
  el('ssd-note').textContent = `${warmStatus}${number(state.ssd.entries.size)} blocks · ${number(state.ssd.evictions)} LRU evictions · ${number(state.warmReads)} warm-up reads; each slot represents part of capacity.`;
  el('ram-size').textContent = bytes(s.ramTotal);
  const memory = [
    ['ID directory', s.ram.directory],
    ['Routing', s.ram.centroids + s.ram.sketches],
    ['Recent writes', s.ram.tail],
    ['Hot blocks', s.ram.hot],
  ];
  el('memory').replaceChildren(...memory.map(([title, size]) => {
    const row = node('div', 'sim-memory-row');
    row.append(node('strong', '', title));
    const bar = node('div', 'sim-memory-bar'), fill = node('i', '');
    fill.style.width = `${size / Math.max(s.ramTotal, 1) * 100}%`;
    bar.append(fill); row.append(bar); return row;
  }));
  el('memory-details').replaceChildren(...memory.map(([title, size]) => node('p', '', `${title}: ${bytes(size)}`)),
    node('p', '', `${number(state.ram?.sketchRows ?? 0)} sketch rows · ${number(state.ram?.centroids.length ?? 0)} centroids · ${number(tail)} tail rows · ${state.ram?.hot.entries.size ?? 0} hot blocks`),
    node('p', '', '128D sizing: directory 24 B/ID; five-bit sketches 92 B/row; centroids 512 B each; tail 640 B/row.'));
  el('seal').disabled = state.status !== 'running' || !tail || (busy && !queryAnimation);
  el('again').disabled = !queryPoint || state.status !== 'running' || (busy && !queryAnimation);
  el('stream').disabled = state.status !== 'running';
  el('stream').setAttribute('aria-pressed', String(streaming));
  el('stream').textContent = streaming ? 'Pause writes' : 'Stream writes';
  el('crash').disabled = state.status !== 'running';
  el('restart').disabled = state.status !== 'crashed';
  document.querySelectorAll('[data-query]').forEach(button => { button.disabled = (busy && !queryAnimation) || state.status !== 'running'; });
}
function clearQuery() {
  plan = null; results = []; activeClusters = []; recentBlocks = []; queryPoint = null;
  for (const name of ['probes', 'reads', 'sources', 'remote', 'cache', 'latency']) el(name).textContent = '—';
  el('results').textContent = 'Top-10 neighbours appear as connected points after a query.';
  el('blocks').replaceChildren(node('span', '', 'Up to 12 candidate blocks · 8 range GETs · 1 MiB remote'));
  phase(-1); glow(null); invalidate();
}
function renderQuery() {
  el('probes').textContent = number(plan.clusters.length);
  el('reads').textContent = `${plan.reads.length} / ${plan.candidates.length}`;
  el('sources').textContent = `${plan.sources.ram} / ${plan.sources.ssd} / ${plan.sources.s3}`;
  el('remote').textContent = `${plan.sources.s3} blocks (${+(plan.bytes / 1024).toFixed(1)} KiB)`;
  el('cache').textContent = `${plan.sources.ram + plan.sources.ssd} blocks`;
  el('latency').textContent = `${plan.latency} ms`;
  el('results').textContent = `Top ${plan.results.length} · exact rerank of fetched blocks + tail · IDs ${plan.results.map(p => number(p.id)).join(', ')}`;
}
const blockCards = new Map();
function renderBlocks() {
  blockCards.clear();
  el('blocks').replaceChildren(...plan.candidates.map(block => {
    const read = plan.reads.find(r => r.block.id === block.id);
    const card = node('div', 'sim-read pending');
    card.append(node('strong', '', `B${block.id}`), node('span', '', read ? read.source.toUpperCase() : 'budget'));
    card.title = `${block.pack}, block ${block.id}, ${bytes(block.bytes)}${read ? ` from ${read.source.toUpperCase()}` : ' skipped: cold budget exhausted'}`;
    blockCards.set(block.id, card); return card;
  }));
}
async function flight(from, to, label, duration = 150) {
  if (motion.matches) return;
  // Read geometry once per hop; the browser animates transforms without layout.
  const origin = app.getBoundingClientRect(), a = el(from).getBoundingClientRect(), b = el(to).getBoundingClientRect();
  const token = el('flight'); token.textContent = label; token.hidden = false;
  const transform = box => `translate(${box.left - origin.left + box.width / 2}px, ${box.top - origin.top + box.height / 2}px) translate(-50%, -50%)`;
  const animation = token.animate([{transform: transform(a)}, {transform: transform(b)}], {duration, easing: 'ease-in-out', fill: 'forwards'});
  try { await animation.finished; } catch { /* A reset/crash cancels an in-flight illustration. */ }
  animation.cancel(); token.hidden = true;
}
function cancelWork() {
  stopQueryAnimation(); epoch++; busy = false;
  for (const animation of el('flight').getAnimations()) animation.cancel();
  el('flight').hidden = true; el('recovery').hidden = true; glow(null);
}
function stopQueryAnimation() {
  if (!queryAnimation) return;
  cancelAnimationFrame(queryAnimation.frame); clearTimeout(queryAnimation.deadline);
  queryAnimation = null;
  el('query-flights').replaceChildren(); el('query-flights').hidden = true;
}
function finishQueryAnimation() {
  if (!queryAnimation) return;
  stopQueryAnimation();
  for (const read of plan.reads) blockCards.get(read.block.id).className = `sim-read ${read.source}`;
  results = plan.results; phase(3); busy = false; glow(null); renderInventory(); invalidate();
  say(plan.sources.s3 ? 'Query complete. Those blocks are now cached — run again to see the difference.' : 'Cache hit. Every selected block was served locally.');
}
function runQuery(point) {
  if (state.status !== 'running') return;
  // A new click settles the old illustration. Execution/cache admission does
  // not depend on any frame, timer, visibility state or animation promise.
  finishQueryAnimation();
  if (busy) return;
  queryPoint = {...point}; results = []; recentBlocks = [];
  plan = query(state, point); activeClusters = plan.clusters;
  recentBlocks = plan.reads.map(read => read.block.id);
  busy = true;
  const started = performance.now();
  queryAnimation = {started, frame: 0, deadline: 0};
  renderQuery(); renderBlocks(); renderInventory(); invalidate();
  const remote = plan.reads.filter(read => read.source === 's3');
  const local = plan.reads.filter(read => read.source !== 's3');
  const origin = app.getBoundingClientRect(), source = el('s3').getBoundingClientRect(), target = el('ram').getBoundingClientRect();
  const tokens = remote.map(read => {
    const token = node('span', 'sim-flight', `B${read.block.id}`);
    el('query-flights').append(token); return token;
  });
  let shownStage = -1;
  const update = () => {
    if (!queryAnimation) return;
    const elapsed = performance.now() - started;
    const progress = queryAnimationProgress(elapsed, remote.length);
    if (motion.matches || progress.done) { finishQueryAnimation(); return; }
    if (progress.stage !== shownStage) {
      shownStage = progress.stage; phase(progress.stage);
      glow(progress.stage === 2 && remote.length ? 's3' : plan.sources.ssd && !remote.length ? 'ssd' : 'ram');
      say(['Find nearby clusters.', 'Choose the most promising blocks.',
        remote.length ? 'Read missing blocks in parallel and keep them in cache.' : 'Serve the selected blocks directly from cache.',
        'Rank the nearest neighbours.'][progress.stage]);
      for (const read of local) blockCards.get(read.block.id).className = progress.stage >= 2 ? `sim-read ${read.source}` : 'sim-read pending';
    }
    for (let i = 0; i < remote.length; i++) {
      const read = remote[i], batchMs = 720 / Math.max(1, Math.ceil(remote.length / 4));
      const t = Math.max(0, Math.min(1, (elapsed - 280 - Math.floor(i / 4) * batchMs) / batchMs));
      blockCards.get(read.block.id).className = i < progress.remoteDone ? 'sim-read s3' : 'sim-read pending';
      const token = tokens[i]; token.hidden = t <= 0 || t >= 1;
      const x = source.left + source.width / 2 + (target.left + target.width / 2 - source.left - source.width / 2) * t - origin.left;
      const y = source.top + source.height / 2 + (target.top + target.height / 2 - source.top - source.height / 2) * t - origin.top;
      token.style.transform = `translate(${x + (i % 4 - 1.5) * 36}px, ${y}px) translate(-50%, -50%)`;
    }
    el('query-flights').hidden = !remote.length || progress.stage !== 2;
    if (progress.stage >= 3) { results = plan.results; invalidate(); }
    queryAnimation.frame = requestAnimationFrame(update);
  };
  // RAF provides motion; the deadline also settles the UI when frames pause.
  queryAnimation.deadline = setTimeout(finishQueryAnimation, QUERY_ANIMATION_MS);
  update();
}
async function sealTail() {
  finishQueryAnimation();
  if (busy || state.status !== 'running' || !state.ram.tail.size) return;
  busy = true; const ticket = ++epoch;
  phase(-1); glow('ram'); say('Seal recent writes into durable data packs.');
  renderInventory(); await flight('ram', 's3', 'tail → sgpack', 420);
  if (ticket !== epoch) return;
  const publication = seal(state); renderInventory(); glow('s3');
  say(`${number(publication.rows)} recent writes are now sealed in S3.`);
  await wait(600); if (ticket !== epoch) return;
  busy = false; glow(null); renderInventory();
}
async function recover() {
  if (state.status !== 'crashed') return;
  const ticket = ++epoch; busy = true; beginRestart(state); el('recovery').hidden = false; el('progress').value = 0;
  for (let i = 0; i < RESTART_PHASES.length; i++) {
    if (ticket !== epoch) return;
    say(RESTART_PHASES[i]); glow(i < 3 ? 's3' : 'ram'); renderInventory();
    await wait(520); if (ticket !== epoch) return;
    const step = restartStep(state); el('progress').value = step.progress; renderInventory();
  }
  busy = false; glow(null); renderInventory();
  say(`Restart complete: ${number(state.acknowledged)} acknowledged upserts survived; ${state.ram.tail.size} unsealed rows were replayed from S3.`);
}
function reset() {
  cancelWork(); streaming = false; objectCounts = new Map();
  state = createSimulation({count: +el('size').value, ssdCapacity: +el('capacity').value * MiB});
  sample = sampleDataset(state.dataset); writeDots = []; clearQuery();
  el('warm').checked = false;
  say('Click the map to route a query through centroids and cached blocks.'); renderInventory(); resize();
}
el('stream').addEventListener('click', () => { streaming = !streaming; renderInventory(); });
el('crash').addEventListener('click', event => {
  cancelWork(); streaming = false; crash(state, el('lose-disk').checked); clearQuery(); renderInventory();
  say(`RAM is empty${el('lose-disk').checked ? ' and the SSD was lost' : '; the SSD cache survives'}; S3 still holds every acknowledged write.`);
  if (event.detail === 0) el('restart').focus();
});
el('restart').addEventListener('click', recover);
el('seal').addEventListener('click', sealTail);
el('again').addEventListener('click', () => { if (queryPoint) runQuery(queryPoint); });
motion.addEventListener('change', () => { if (motion.matches) finishQueryAnimation(); });
document.addEventListener('visibilitychange', () => {
  if (queryAnimation && performance.now() - queryAnimation.started >= QUERY_ANIMATION_MS) finishQueryAnimation();
});
el('reset').addEventListener('click', reset);
el('size').addEventListener('change', reset);
el('warm').addEventListener('change', renderInventory);
el('capacity').addEventListener('change', () => {
  state.ssd.capacity = +el('capacity').value * MiB;
  // Capacity edits are configuration changes, with the same LRU order as reads.
  while (state.ssd.bytes > state.ssd.capacity) {
    const key = state.ssd.entries.keys().next().value;
    state.ssd.bytes -= state.ssd.entries.get(key); state.ssd.entries.delete(key); state.ssd.evictions++;
  }
  state.warmCursor = 0; renderInventory();
});
canvas.addEventListener('click', event => {
  const box = canvas.getBoundingClientRect(); runQuery({x: (event.clientX - box.left) / box.width, y: (event.clientY - box.top) / box.height});
});
document.querySelectorAll('[data-query]').forEach(button => button.addEventListener('click', () => runQuery(presets[+button.dataset.query])));
// One timer drives idle work, not canvas frames; writes/warm-up never compete
// with the visual query/recovery transaction. RAF only redraws a dirty canvas.
setInterval(() => {
  if (busy || state.status !== 'running' || document.hidden) return;
  if (state.s3.logs.size >= 32) { void sealTail(); return; }
  if (streaming) {
    const batch = writeBatch(state, queryPoint ?? presets[1]);
    writeDots.push(...batch.rows); writeDots = writeDots.slice(-6400);
    say('100 new writes are durable in S3 and visible in RAM.');
    renderInventory(); invalidate();
    if (batch.shouldSeal) void sealTail();
  } else if (el('warm').checked) {
    let added = [];
    for (let i = 0; i < 8; i++) {
      const blocks = warmStep(state); added.push(...blocks); if (!blocks.length) break;
    }
    if (added.length) { recentBlocks = added.map(b => b.id); renderInventory(); }
  }
}, 160);
reset();
new ResizeObserver(resize).observe(canvas);
