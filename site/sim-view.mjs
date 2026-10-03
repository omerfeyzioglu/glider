import {
  createSimulation, sampleDataset, query, QUERY_PRESETS, QUERY_ANIMATION_MS, queryAnimationProgress, queryPhaseFractions, writeBatch,
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
  traceSegments.forEach((segment, i) => segment.classList.toggle('active', i === index));
}
let traceSegments = [];
function traceProgress(progress) {
  traceSegments.forEach((segment, i) => { segment.style.setProperty('--progress', progress[i]); });
}
function renderTrace() {
  const shares = queryPhaseFractions(plan);
  traceSegments = plan.phases.map((phase, i) => {
    const title = phase.name[0].toUpperCase() + phase.name.slice(1);
    const segment = node('li', 'sim-trace-phase');
    segment.style.flex = `${shares[i]} 1 0%`;
    segment.style.setProperty('--progress', 0);
    const fill = node('div', 'sim-trace-fill');
    const tiers = phase.sources ? ['s3', 'ssd', 'ram'].filter(tier => phase.sources[tier]) : [phase.tier];
    const detail = phase.sources ? tiers.map(tier => `${tier.toUpperCase()} ×${phase.sources[tier]}`).join(', ')
      : `RAM${phase.name === 'rerank' ? ' CPU over fetched blocks + unsealed tail' : phase.name === 'route' ? ' centroid comparison' : ' candidate selection'}`;
    segment.title = `${title}: ${phase.ms} ms · ${detail}`;
    segment.setAttribute('aria-label', segment.title);
    fill.setAttribute('aria-hidden', 'true');
    for (const tier of tiers) {
      const source = node('div', `sim-trace-source ${tier}`);
      source.style.flex = `${phase.sources?.[tier] ?? 1} 1 0%`;
      source.append(node('strong', 'sim-trace-label', title),
        node('span', 'sim-trace-label', phase.sources ? `${tier.toUpperCase()} ×${phase.sources[tier]}` : phase.name === 'rerank' ? 'RAM · CPU' : 'RAM'));
      fill.append(source);
    }
    segment.append(fill);
    return segment;
  });
  el('trace-bar').replaceChildren(...traceSegments); el('trace-bar').hidden = false;
  const {s3, ssd, ram} = plan.sources, count = plan.reads.length;
  const blocks = n => `${n} block${n === 1 ? '' : 's'}`;
  const origin = s3 === count ? `Fetched ${blocks(count)} from S3 (${+(plan.bytes / 1024).toFixed(1)} KiB)`
    : ram === count ? `All ${blocks(count)} came from RAM — no S3 read`
    : ssd === count ? `${blocks(count)} from SSD — no S3 read`
    : `Fetched ${[['S3', s3], ['SSD', ssd], ['RAM', ram]].filter(([, n]) => n).map(([tier, n]) => `${blocks(n)} from ${tier}`).join(', ')}${s3 ? ` (${+(plan.bytes / 1024).toFixed(1)} KiB from S3)` : ' — no S3 read'}`;
  el('trace-summary').textContent = `${origin}; reranked in RAM on the CPU.`;
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
  holdTransferCounters();
}
function clearQuery() {
  plan = null; results = []; activeClusters = []; recentBlocks = []; queryPoint = null;
  for (const name of ['probes', 'reads', 'sources', 'remote', 'cache', 'latency']) el(name).textContent = '—';
  el('results').textContent = 'Top-10 neighbours appear as connected points after a query.';
  el('blocks').replaceChildren(node('span', '', 'Up to 12 candidate blocks · 8 range GETs · 1 MiB remote'));
  traceSegments = []; el('trace-bar').replaceChildren(); el('trace-bar').hidden = true;
  el('trace-summary').textContent = 'Route and select in RAM → fetch from RAM, SSD or S3 → rerank in RAM on the CPU.';
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
// One transparent, DPR-scaled overlay owns the tracks and fill.
// Geometry is measured only when layout or the query marker changes, never
// per animation frame. Inventory/model updates do not wait for this illustration.
const grid = el('grid'), tracks = el('tracks'), rail = tracks.getContext('2d');
let geometry = {}, trackWidth = 1, trackHeight = 1, transfer = null;
const flashes = new Map();
const TRANSFER_SPEED = 2; // Non-query flows: CSS pixels/ms, including bends.
const TRANSFER_FADE_MS = 200;
const tierCounters = {ssd: ['ssd-size', 'ssd-count'], ram: ['ram-size'], s3: ['s3-size']};
function snapshotCounters() {
  return Object.fromEntries(Object.values(tierCounters).flat().map(id => [id, el(id).textContent]));
}
function holdTransferCounters() {
  if (!transfer?.counters) return;
  for (const stop of transfer.route.stops) {
    for (const id of tierCounters[stop.tier] ?? []) {
      transfer.finalCounters[id] = el(id).textContent;
      if (!transfer.arrived.has(stop.tier)) el(id).textContent = transfer.counters[id];
    }
  }
}
function transferArrival(tier) {
  if (transfer.arrived.has(tier)) return;
  transfer.arrived.add(tier);
  for (const id of tierCounters[tier] ?? []) el(id).textContent = transfer.finalCounters[id];
  arrival(tier);
}
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
  const mapPanel = box(document.querySelectorAll('.sim-map-panel')[0]);
  const stacked = s3.top >= mapPanel.bottom;
  const top = r => ({x: (r.left + r.right) / 2, y: r.top});
  const bottom = r => ({x: (r.left + r.right) / 2, y: r.bottom});
  const verticalGap = (a, b) => a.x === b.x ? [a, b]
    : [a, {x: a.x, y: (a.y + b.y) / 2}, {x: b.x, y: (a.y + b.y) / 2}, b];
  const s3SSD = verticalGap(bottom(s3), top(ssd));
  const ssdRAM = verticalGap(bottom(ssd), top(ram));
  const point = transfer?.point ?? queryPoint ?? presets[1];
  const q = {x: map.left + point.x * map.width, y: map.top + point.y * (map.bottom - map.top)};
  const ramLeft = {x: ram.left, y: (ram.top + ram.bottom) / 2};
  const mapEdge = {x: mapPanel.right, y: ramLeft.y};
  const ramMap = [ramLeft, mapEdge];
  // Only extend to the marker when the entire elbow fits inside the map.
  if (mapEdge.y >= map.top && mapEdge.y <= map.bottom &&
      q.x >= map.left && q.x <= map.right && q.y >= map.top && q.y <= map.bottom) {
    ramMap.push({x: q.x, y: mapEdge.y}, q);
  }
  const s3Map = verticalGap(top(s3), bottom(mapPanel));
  // Paths are separate strokes across gaps; crossing a tier never paints over
  // its contents. In the stacked layout RAM reaches the map via adjacent gaps.
  geometry = {
    's3-ssd': [s3SSD],
    'ssd-ram': [ssdRAM],
    'ram-map': stacked
      ? [[...ssdRAM].reverse(), [...s3SSD].reverse(), s3Map]
      : [ramMap],
    'map-gap': stacked ? [s3Map] : [ramMap],
    // A log PUT follows the read connectors backwards, without SSD admission.
    'ram-s3': [[...ssdRAM].reverse(), [...s3SSD].reverse()],
  };
  const cards = el('objects').children;
  if (cards.length >= 2) geometry.seal = {logs: box(cards[0]), packs: box(cards[1])};
  if (transfer) buildRoutes();
  paintTransfers(performance.now());
}
function routeFor(legs) {
  const segments = [], stops = []; let length = 0;
  for (const leg of legs) {
    const paths = leg.reverse ? [...geometry[leg.track]].reverse().map(points => [...points].reverse()) : geometry[leg.track];
    for (const points of paths) for (let i = 1; i < points.length; i++) {
      const a = points[i - 1], b = points[i], distance = Math.abs(b.x - a.x) + Math.abs(b.y - a.y);
      if (distance) { segments.push({a, b, start: length, length: distance}); length += distance; }
    }
    stops.push({tier: leg.to, distance: length});
  }
  return {segments, stops, length};
}
function buildRoutes() {
  transfer.route = routeFor(transfer.legs);
  transfer.duration = transfer.sealing || transfer.queryFlow ? transfer.duration : transfer.route.length / TRANSFER_SPEED;
  if (transfer.done && transfer.fadeStarted === null) {
    clearTimeout(transfer.deadline);
    transfer.deadline = setTimeout(() => stopTransfers(true, !transfer.sealing),
      Math.max(0, transfer.started + transfer.duration - performance.now()));
  }
}
function trackLine(points) {
  rail.beginPath(); rail.moveTo(points[0].x, points[0].y);
  for (const p of points.slice(1)) rail.lineTo(p.x, p.y);
  rail.lineJoin = 'miter'; rail.lineCap = 'butt'; rail.setLineDash([]);
  rail.globalAlpha = .22;
  rail.strokeStyle = '#0E1B2C'; rail.lineWidth = 6; rail.stroke();
  rail.globalAlpha = .4;
  rail.strokeStyle = '#F7F4EE'; rail.lineWidth = 1;
  rail.setLineDash([3, 5]); rail.stroke(); rail.setLineDash([]); rail.globalAlpha = 1;
}
function paintFill(distance, opacity) {
  rail.beginPath();
  let previous = null;
  for (const segment of transfer.route.segments) {
    if (distance <= segment.start) break;
    if (!previous || previous.x !== segment.a.x || previous.y !== segment.a.y) rail.moveTo(segment.a.x, segment.a.y);
    const fraction = Math.min(1, (distance - segment.start) / segment.length);
    previous = {x: segment.a.x + (segment.b.x - segment.a.x) * fraction,
      y: segment.a.y + (segment.b.y - segment.a.y) * fraction};
    rail.lineTo(previous.x, previous.y);
  }
  rail.globalAlpha = opacity; rail.lineJoin = 'miter'; rail.lineCap = 'butt'; rail.setLineDash([]);
  rail.strokeStyle = transfer.colour; rail.lineWidth = 4; rail.stroke(); rail.globalAlpha = 1;
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
  // Draw shared gap connectors once, including in the stacked layout.
  for (const key of ['s3-ssd', 'ssd-ram', 'map-gap']) for (const points of geometry[key] ?? []) trackLine(points);
  if (!transfer) return;
  const elapsed = now - transfer.started;
  if (transfer.sealing) { if (!motion.matches) paintSeal(Math.min(1, elapsed / transfer.duration)); return; }
  let distance = transfer.fadeStarted !== null || motion.matches ? transfer.route.length
    : Math.min(transfer.route.length, Math.max(0, elapsed) * TRANSFER_SPEED);
  if (transfer.queryFlow && transfer.fadeStarted === null && !motion.matches) {
    const progress = queryAnimationProgress(elapsed, plan);
    const ramDistance = transfer.route.stops.find(stop => stop.tier === 'ram')?.distance ?? 0;
    distance = ramDistance * progress.phases[2] + (transfer.route.length - ramDistance) * progress.phases[3];
  }
  for (const stop of transfer.route.stops) if (distance >= stop.distance) transferArrival(stop.tier);
  if (motion.matches) return;
  const opacity = transfer.fadeStarted === null ? 1 : Math.max(0, 1 - (now - transfer.fadeStarted) / TRANSFER_FADE_MS);
  paintFill(distance, opacity);
}
function stopTransfers(settle = false, fade = false) {
  if (!transfer) return;
  if (settle && !transfer.sealing) {
    for (const stop of transfer.route.stops) transferArrival(stop.tier);
    if (fade && !motion.matches) {
      if (transfer.fadeStarted === null) {
        transfer.fadeStarted = Math.min(performance.now(), transfer.started + transfer.duration);
        clearTimeout(transfer.deadline);
        const remaining = TRANSFER_FADE_MS - (performance.now() - transfer.fadeStarted);
        if (remaining <= 0) { stopTransfers(); return; }
        transfer.deadline = setTimeout(() => stopTransfers(), remaining);
      }
      paintTransfers(performance.now());
      return;
    }
  }
  if (transfer.counters) for (const [id, value] of Object.entries(transfer.finalCounters)) el(id).textContent = value;
  cancelAnimationFrame(transfer.frame); clearTimeout(transfer.deadline);
  const done = transfer.done; transfer = null; paintTransfers(performance.now()); done?.();
}
function startTransfers(flow, {duration = 900, point = queryPoint ?? presets[1], sealing = false, counters = null, queryFlow = false} = {}) {
  stopTransfers(); clearArrivals();
  transfer = {started: queryFlow ? queryAnimation.started : performance.now(), legs: flow?.legs ?? [],
    colour: flow?.colour ?? '#F07A1A', arrived: new Set(), fadeStarted: null,
    counters, queryFlow, finalCounters: snapshotCounters(), point: {...point}, duration, sealing, frame: 0, deadline: 0};
  measureTracks();
  if (counters) for (const stop of transfer.route.stops) {
    if (!transfer.arrived.has(stop.tier)) for (const id of tierCounters[stop.tier] ?? []) el(id).textContent = counters[id];
  }
  if (motion.matches) { stopTransfers(true); return Promise.resolve(); }
  return new Promise(resolve => {
    transfer.done = resolve;
    const update = () => {
      if (!transfer) return;
      const now = performance.now();
      paintTransfers(now);
      if (transfer.fadeStarted !== null) {
        if (now - transfer.fadeStarted >= TRANSFER_FADE_MS) { stopTransfers(); return; }
      } else if (now - transfer.started >= transfer.duration) {
        stopTransfers(true, !transfer.sealing);
        if (!transfer) return;
      }
      transfer.frame = requestAnimationFrame(update);
    };
    transfer.deadline = setTimeout(() => stopTransfers(true, !sealing), transfer.duration);
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
}
function finishQueryAnimation() {
  if (!queryAnimation) return;
  stopQueryAnimation();
  // Execution and cache admission never wait for frames. The query track and
  // trace share a deadline; settling also completes the reduced-motion path.
  if (transfer && (motion.matches || performance.now() - transfer.started >= transfer.duration)) stopTransfers(true, true);
  for (const read of plan.reads) blockCards.get(read.block.id).className = `sim-read ${read.source}`;
  traceProgress([1, 1, 1, 1]);
  results = plan.results; phase(3); busy = false; glow(null); renderInventory(); invalidate();
  say(plan.sources.s3 ? 'Query complete. Those blocks are now cached — run again to see the difference.' : 'Cache hit. Every selected block was served locally.');
}
function runQuery(point) {
  if (state.status !== 'running') return;
  // A new click settles the old illustration. Execution/cache admission does
  // not depend on any frame, timer, visibility state or animation promise.
  finishQueryAnimation();
  if (busy) return;
  stopTransfers(true);
  queryPoint = {...point}; results = []; recentBlocks = [];
  const counters = snapshotCounters();
  plan = query(state, point); activeClusters = plan.clusters;
  recentBlocks = plan.reads.map(read => read.block.id);
  busy = true;
  const started = performance.now();
  queryAnimation = {started, frame: 0, deadline: 0};
  renderQuery(); renderTrace(); renderBlocks(); renderInventory(); invalidate();
  const remote = plan.reads.filter(read => read.source === 's3');
  const local = plan.reads.filter(read => read.source !== 's3');
  // One aggregate flow starts at the deepest source needed by this plan.
  const source = remote.length ? 's3' : plan.sources.ssd ? 'ssd' : 'ram';
  void startTransfers({legs: [
    ...(source === 's3' ? [{track: 's3-ssd', to: 'ssd'}] : []),
    ...(source !== 'ram' ? [{track: 'ssd-ram', to: 'ram'}] : []),
    {track: 'ram-map', to: 'map'},
  ]}, {duration: QUERY_ANIMATION_MS, counters, queryFlow: true});
  let shownStage = -1;
  const update = () => {
    if (!queryAnimation) return;
    const elapsed = performance.now() - started;
    const progress = queryAnimationProgress(elapsed, plan);
    if (motion.matches || progress.done) { finishQueryAnimation(); return; }
    traceProgress(progress.phases);
    if (progress.stage !== shownStage) {
      shownStage = progress.stage; phase(progress.stage);
      say(['Compare centroids in RAM.', 'Select candidate blocks using RAM routing sketches.',
        remote.length ? 'Fetch blocks from RAM, then SSD, otherwise S3 range reads.' : 'Fetch selected blocks from the RAM / SSD caches.',
        'Rerank fetched blocks + the unsealed tail in RAM on the CPU.'][progress.stage]);
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
  renderInventory(); await startTransfers(null, {duration: 420, sealing: true});
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
    const counters = snapshotCounters();
    const batch = writeBatch(state, queryPoint ?? presets[1]);
    writeDots.push(...batch.rows); writeDots = writeDots.slice(-6400);
    say('100 new writes are durable in S3 and visible in RAM.');
    renderInventory(); invalidate();
    if (!transfer) void startTransfers({colour: '#16304F',
      legs: [{track: 'ram-map', reverse: true, to: 'ram'}, {track: 'ram-s3', to: 's3'}],
    }, {point: queryPoint ?? presets[1], counters});
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
