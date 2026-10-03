import {
  createSimulation, sampleDataset, query, QUERY_PRESETS, QUERY_ANIMATION_MS, queryAnimationProgress, writeBatch,
  seal, crash, beginRestart, restartStep, RESTART_PHASES, warmStep, sizes, MiB,
} from './sim-model.mjs';

const el = id => document.getElementById(`sim-${id}`);
const canvas = el('map'), ctx = canvas.getContext('2d');
const layer = document.createElement('canvas'), ink = layer.getContext('2d');
const motion = matchMedia('(prefers-reduced-motion: reduce)');
const palette = ['#F4A261', '#81B9C5', '#E9C46A', '#91B994', '#D2A3BE', '#B7C6E7', '#F7F4EE', '#76BFC0'];
const presets = QUERY_PRESETS;
let state, sample, writeDots = [], queryPoint = null, activeClusters = [], results = [], plan = null;
let busy = false, streaming = false, epoch = 0, frame = 0, width = 1, height = 1;
let recentBlocks = [], queryAnimation = null;
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
  const card = node('div', `sim-object${count === 0 ? ' empty' : ''}`);
  card.dataset.kind = kind;
  const icon = node('i', 'sim-stack'); icon.setAttribute('aria-hidden', 'true');
  card.append(icon, node('strong', '', title), node('span', 'sim-count-badge', number(count)));
  return card;
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
// One transparent, DPR-scaled overlay owns all transfer tracks and tags.
// Geometry is measured only when layout or the query marker changes, never
// per animation frame. Inventory/model updates do not wait for this illustration.
const grid = el('grid'), tracks = el('tracks'), rail = tracks.getContext('2d');
let geometry = {}, trackWidth = 1, trackHeight = 1, transfer = null;
const flashes = new Map();
function clearArrivals() {
  for (const animations of flashes.values()) for (const animation of animations) animation.cancel();
  flashes.clear();
}
function arrival(tier) {
  const panel = tier === 'map' ? canvas : el(tier);
  const counter = tier === 'ssd' ? el('ssd-count') : tier === 'map' ? null : el(`${tier}-size`);
  // Restart only this receiver's flash; a source never lights up on departure.
  flashes.get(tier)?.forEach(animation => animation.cancel());
  const animations = [panel.animate([
    {outline: '3px solid #F07A1A', outlineOffset: '-3px'},
    {outline: '3px solid transparent', outlineOffset: '-3px'},
  ], {duration: 260})];
  if (counter) animations.push(counter.animate([
    {backgroundColor: '#F07A1A', color: '#0E1B2C'},
    {backgroundColor: 'transparent'},
  ], {duration: 260}));
  flashes.set(tier, animations);
}
function measureTracks() {
  const origin = grid.getBoundingClientRect(), ratio = devicePixelRatio || 1;
  trackWidth = origin.width; trackHeight = origin.height;
  tracks.width = Math.round(trackWidth * ratio); tracks.height = Math.round(trackHeight * ratio);
  rail.setTransform(ratio, 0, 0, ratio, 0, 0);
  const box = element => {
    const r = element.getBoundingClientRect();
    return {left: r.left - origin.left, right: r.left - origin.left + r.width,
      top: r.top - origin.top, bottom: r.top - origin.top + r.height, width: r.width};
  };
  const s3 = box(el('s3')), ssd = box(el('ssd')), ram = box(el('ram')), map = box(canvas);
  const stacked = s3.top >= map.bottom;
  const right = Math.min(trackWidth - 7, s3.right + 10);
  const port = r => ({x: r.right, y: r.top + 30});
  const a = port(s3), b = port(ssd), c = port(ram);
  const point = transfer?.point ?? queryPoint ?? presets[1];
  const q = {x: map.left + point.x * map.width, y: map.top + point.y * (map.bottom - map.top)};
  const left = s3.left - 10, bottom = ram.bottom + 10;
  geometry = {
    's3-ssd': [a, {x: right, y: a.y}, {x: right, y: b.y}, b],
    'ssd-ram': [b, {x: right, y: b.y}, {x: right, y: c.y}, c],
    'ram-map': stacked
      ? [c, {x: right, y: c.y}, {x: right, y: q.y}, q]
      : [c, {x: right, y: c.y}, {x: right, y: bottom}, {x: left, y: bottom}, {x: left, y: q.y}, q],
    // A log PUT bypasses the disposable SSD. It uses the same outer rail,
    // passing the SSD port without an arrival or cache admission there.
    'ram-s3': [c, {x: right, y: c.y}, {x: right, y: a.y}, a],
  };
  const cards = el('objects').children;
  if (cards.length >= 2) geometry.seal = {logs: box(cards[0]), packs: box(cards[1])};
  if (transfer) buildRoutes();
  paintTransfers(performance.now());
}
function routeFor(legs) {
  const segments = [], stops = []; let length = 0;
  for (const leg of legs) {
    const points = leg.reverse ? [...geometry[leg.track]].reverse() : geometry[leg.track];
    for (let i = 1; i < points.length; i++) {
      const a = points[i - 1], b = points[i], distance = Math.abs(b.x - a.x) + Math.abs(b.y - a.y);
      if (distance) { segments.push({a, b, start: length, length: distance, track: leg.track, label: leg.label}); length += distance; }
    }
    stops.push({tier: leg.to, distance: length});
  }
  return {segments, stops, length};
}
function buildRoutes() {
  for (const tag of transfer.tags) tag.route = routeFor(tag.legs);
}
function trackLine(points, active = false) {
  rail.beginPath(); rail.moveTo(points[0].x, points[0].y);
  for (const p of points.slice(1)) rail.lineTo(p.x, p.y);
  rail.lineJoin = 'miter'; rail.lineCap = 'butt'; rail.setLineDash([]);
  rail.globalAlpha = active ? 1 : .22;
  rail.strokeStyle = '#0E1B2C'; rail.lineWidth = 6; rail.stroke();
  if (active) { rail.strokeStyle = '#F07A1A'; rail.lineWidth = 4; rail.stroke(); }
  rail.globalAlpha = active ? .6 : .4;
  rail.strokeStyle = active ? '#0E1B2C' : '#F7F4EE'; rail.lineWidth = 1;
  rail.setLineDash([3, 5]); rail.stroke(); rail.setLineDash([]); rail.globalAlpha = 1;
}
// A short acceleration/deceleration at the endpoints, with constant distance
// per millisecond through the middle and through every right-angle bend.
function slide(t) {
  const edge = .08, speed = 1 / (1 - edge);
  if (t < edge) return speed * t * t / (2 * edge);
  if (t > 1 - edge) return 1 - speed * (1 - t) ** 2 / (2 * edge);
  return speed * (t - edge / 2);
}
function paperTag(point, label) {
  const w = Math.max(38, label.length * 6 + 12), h = 22;
  // Hang the tag to the left of its rail anchor, including on the outer
  // vertical rail. A fixed offset avoids a jump at a viewport-edge clamp.
  const x = point.x - w + 3;
  const y = point.y - h / 2;
  rail.fillStyle = '#0E1B2C'; rail.fillRect(x + 2, y + 2, w, h);
  rail.fillStyle = '#F7F4EE'; rail.fillRect(x, y, w, h);
  rail.strokeStyle = '#0E1B2C'; rail.lineWidth = 2; rail.strokeRect(x, y, w, h);
  rail.fillStyle = '#0E1B2C'; rail.font = '600 10px monospace'; rail.textAlign = 'center'; rail.textBaseline = 'middle';
  rail.fillText(label, x + w / 2, y + h / 2);
}
function paintSeal(t) {
  // Logs and the pack occupy the first inventory row. Their paper tiles
  // compress into the pack slot inside S3, with no inter-panel flight.
  const {logs, packs} = geometry.seal;
  const x = logs.left + 18, target = packs.left + 18, y = logs.top + 26;
  rail.save();
  rail.beginPath(); rail.rect(logs.left, logs.top,
    packs.right - logs.left, Math.max(logs.bottom - logs.top, packs.bottom - packs.top)); rail.clip();
  if (t < .8) {
    for (let i = 0; i < 3; i++) {
      const amount = slide(Math.min(1, t / .8));
      paperTag({x: x + i * 14 + (target - x - i * 14) * amount, y}, 'log');
    }
  } else paperTag({x: target, y}, 'pack');
  rail.restore();
}
function paintTransfers(now) {
  rail.clearRect(0, 0, trackWidth, trackHeight);
  // The direct log rail overlaps the read rail: draw each shared track once.
  for (const key of ['s3-ssd', 'ssd-ram', 'ram-map']) if (geometry[key]) trackLine(geometry[key]);
  if (!transfer) return;
  const elapsed = now - transfer.started;
  if (transfer.sealing) { if (!motion.matches) paintSeal(Math.min(1, elapsed / transfer.duration)); return; }
  const moving = [], active = new Set();
  for (const tag of transfer.tags) {
    const t = motion.matches ? 1 : Math.max(0, Math.min(1, (elapsed - tag.delay) / tag.duration));
    const distance = slide(t) * tag.route.length;
    for (let i = 0; i < tag.route.stops.length; i++) {
      const stop = tag.route.stops[i];
      if (distance >= stop.distance && !tag.arrived.has(i)) { tag.arrived.add(i); arrival(stop.tier); }
    }
    if (motion.matches || t <= 0 || t >= 1) continue;
    const segment = tag.route.segments.find(s => distance < s.start + s.length);
    if (!segment) continue;
    active.add(segment.track);
    const fraction = (distance - segment.start) / segment.length;
    moving.push({point: {x: segment.a.x + (segment.b.x - segment.a.x) * fraction,
      y: segment.a.y + (segment.b.y - segment.a.y) * fraction}, label: segment.label ?? tag.label});
  }
  for (const key of active) trackLine(geometry[key], true);
  for (const tag of moving) paperTag(tag.point, tag.label);
}
function stopTransfers(settle = false) {
  if (!transfer) return;
  if (settle && !transfer.sealing) {
    for (const tag of transfer.tags) for (let i = 0; i < tag.route.stops.length; i++) {
      if (!tag.arrived.has(i)) arrival(tag.route.stops[i].tier);
    }
  }
  cancelAnimationFrame(transfer.frame); clearTimeout(transfer.deadline);
  const done = transfer.done; transfer = null; paintTransfers(performance.now()); done?.();
}
function startTransfers(tags, {duration = 900, point = queryPoint ?? presets[1], sealing = false} = {}) {
  stopTransfers(); clearArrivals();
  transfer = {started: performance.now(), tags: tags.map(tag => ({...tag, arrived: new Set()})),
    point: {...point}, duration, sealing, frame: 0, deadline: 0};
  measureTracks();
  if (motion.matches) { stopTransfers(true); return Promise.resolve(); }
  return new Promise(resolve => {
    transfer.done = resolve;
    const update = () => {
      if (!transfer) return;
      paintTransfers(performance.now());
      if (performance.now() - transfer.started >= duration) { stopTransfers(true); return; }
      transfer.frame = requestAnimationFrame(update);
    };
    transfer.deadline = setTimeout(() => stopTransfers(true), duration);
    update();
  });
}
function cancelWork() {
  stopQueryAnimation(); epoch++; busy = false;
  stopTransfers();
  clearArrivals(); el('recovery').hidden = true; glow(null);
}
function stopQueryAnimation() {
  if (!queryAnimation) return;
  cancelAnimationFrame(queryAnimation.frame); clearTimeout(queryAnimation.deadline);
  queryAnimation = null;
  stopTransfers();
}
function finishQueryAnimation() {
  if (!queryAnimation) return;
  stopTransfers(true); stopQueryAnimation();
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
  // Eight representative tags keep parallel reads legible, including cache
  // hits. All model reads/stats remain intact and settle at the same deadline.
  const tags = plan.reads.slice(0, 8).map((read, i, visible) => ({
    label: `B${read.block.id}`, delay: 280 + i * 24, duration: 720 - (visible.length - 1) * 24,
    legs: [
      ...(read.source === 's3' ? [{track: 's3-ssd', to: 'ssd'}] : []),
      ...(read.source !== 'ram' ? [{track: 'ssd-ram', to: 'ram'}] : []),
      {track: 'ram-map', to: 'map'},
    ],
  }));
  void startTransfers(tags, {duration: QUERY_ANIMATION_MS});
  let shownStage = -1;
  const update = () => {
    if (!queryAnimation) return;
    const elapsed = performance.now() - started;
    const progress = queryAnimationProgress(elapsed, remote.length);
    if (motion.matches || progress.done) { finishQueryAnimation(); return; }
    if (progress.stage !== shownStage) {
      shownStage = progress.stage; phase(progress.stage);
      say(['Find nearby clusters.', 'Choose the most promising blocks.',
        remote.length ? 'Read missing blocks in parallel and keep them in cache.' : 'Serve the selected blocks directly from cache.',
        'Rank the nearest neighbours.'][progress.stage]);
      for (const read of local) blockCards.get(read.block.id).className = progress.stage >= 2 ? `sim-read ${read.source}` : 'sim-read pending';
    }
    for (let i = 0; i < remote.length; i++) {
      blockCards.get(remote[i].block.id).className = i < progress.remoteDone ? 'sim-read s3' : 'sim-read pending';
    }
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
  phase(-1); glow(null); say('Seal recent writes into durable data packs.');
  renderInventory(); await startTransfers([], {duration: 420, sealing: true});
  if (ticket !== epoch) return;
  const publication = seal(state); renderInventory(); arrival('s3');
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
  cancelWork(); streaming = false;
  state = createSimulation({count: +el('size').value, ssdCapacity: +el('capacity').value * MiB});
  sample = sampleDataset(state.dataset); writeDots = []; clearQuery();
  el('warm').checked = false;
  say('Click the map to route a query through centroids and cached blocks.'); renderInventory(); resize(); measureTracks();
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
motion.addEventListener('change', () => {
  if (motion.matches) { stopTransfers(true); finishQueryAnimation(); }
});
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
    if (!transfer) void startTransfers([{label: `W${batch.sequence}`, delay: 0, duration: 780,
      legs: [{track: 'ram-map', reverse: true, to: 'ram'}, {track: 'ram-s3', to: 's3', label: `L${batch.sequence}`}],
    }], {duration: 800, point: queryPoint ?? presets[1]});
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
const trackObserver = new ResizeObserver(measureTracks);
for (const element of [grid, canvas, el('s3'), el('ssd'), el('ram')]) trackObserver.observe(element);
measureTracks();
