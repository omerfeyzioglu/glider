import test from 'node:test';
import assert from 'node:assert/strict';
import {
  SEED, MiB, BLOCK_BYTES, BUDGET, SEAL_LOGS, QUERY_PRESETS, QUERY_ANIMATION_MS, queryAnimationProgress, LRU, generateDataset, sampleDataset,
  basePoint, assignCluster, createSimulation, query, writeBatch, seal, crash,
  restart, beginRestart, restartStep, scanRows, sizes, warmStep, blockRows, prng,
} from './sim-model.mjs';

const midpoint = {x: .5, y: .5};
test('fixed-seed generation, sample IDs and cluster assignment are reproducible', () => {
  const a = generateDataset(100000, SEED), b = generateDataset(100000, SEED);
  assert.deepEqual(a, b, `seed=${SEED}`);
  assert.deepEqual(sampleDataset(a, 500), sampleDataset(b, 500), `seed=${SEED}`);
  assert.notDeepEqual(sampleDataset(a, 10), sampleDataset(generateDataset(100000, SEED + 1), 10));
  for (const point of sampleDataset(a, 500)) {
    const cluster = a.centroids[point.cluster];
    assert.ok(point.id >= cluster.firstId && point.id < cluster.firstId + cluster.count, `seed=${SEED}, ID=${point.id}`);
    const nearest = [...a.centroids].sort((u, v) =>
      ((u.x - point.x) ** 2 + (u.y - point.y) ** 2) - ((v.x - point.x) ** 2 + (v.y - point.y) ** 2) || u.id - v.id)[0];
    assert.equal(assignCluster(a, point), nearest.id, `seed=${SEED}, ID=${point.id}`);
    assert.deepEqual(point, basePoint(a, point.id));
  }
});

test('every query respects candidates, requests and remote bytes at all dataset sizes', () => {
  for (const count of [100000, 1000000, 10000000]) {
    const state = createSimulation({count, ssdCapacity: 2 * BLOCK_BYTES, ramCapacity: 0});
    const random = prng(SEED);
    for (let i = 0; i < 20; i++) {
      const result = query(state, {x: random(), y: random()});
      const context = `seed=${SEED}, count=${count}, query=${i}`;
      assert.ok(result.candidates.length <= BUDGET.candidates, context);
      assert.ok(result.reads.length <= BUDGET.candidates, context);
      assert.ok(result.sources.s3 <= BUDGET.requests, context);
      assert.ok(result.bytes <= BUDGET.bytes, context);
      assert.equal(result.reads.length, Object.values(result.sources).reduce((a, b) => a + b), context);
      assert.equal(result.bytes, result.reads.filter(r => r.source === 's3').reduce((n, r) => n + r.block.bytes, 0));
    }
  }
});

test('identical queries use RAM, then preserved SSD after restart with no remote reads', () => {
  const state = createSimulation({count: 100000});
  const cold = query(state, midpoint), warm = query(state, midpoint);
  assert.ok(cold.sources.s3 > 0);
  assert.equal(warm.sources.s3, 0); assert.equal(warm.bytes, 0);
  assert.deepEqual(warm.results, cold.results);
  crash(state); restart(state);
  const disk = query(state, midpoint);
  assert.equal(disk.sources.s3, 0); assert.equal(disk.sources.ram, 0); assert.ok(disk.sources.ssd > 0);
  assert.deepEqual(disk.results, cold.results);
  crash(state, true); restart(state);
  assert.ok(query(state, midpoint).sources.s3 > 0);
});

test('crash empties every RAM structure, optionally SSD, and leaves S3 byte-for-byte unchanged', () => {
  for (const loseDisk of [false, true]) {
    const state = createSimulation({count: 100000});
    writeBatch(state); query(state, midpoint);
    const durable = structuredClone(state.s3), diskBytes = state.ssd.bytes;
    crash(state, loseDisk);
    assert.equal(state.ram, null);
    assert.equal(sizes(state).ramTotal, 0);
    assert.equal(state.ssd.bytes, loseDisk ? 0 : diskBytes);
    assert.deepEqual(state.s3, durable);
    assert.throws(() => query(state, midpoint), /Restart/);
    assert.throws(() => writeBatch(state), /Restart/);
  }
});

test('every acknowledged ID and value is found by a recovered scan across seal and repeated crashes', () => {
  const state = createSimulation({count: 100000});
  const acknowledged = new Map();
  for (let i = 0; i < SEAL_LOGS + 7; i++) {
    const batch = writeBatch(state);
    for (const row of batch.rows) acknowledged.set(row.id, {...row});
    if (batch.shouldSeal) seal(state);
  }
  for (const loseDisk of [false, true, true]) {
    crash(state, loseDisk); restart(state);
    const recovered = new Map();
    let rows = 0;
    for (const row of scanRows(state)) { rows++; if (acknowledged.has(row.id)) recovered.set(row.id, row); }
    assert.equal(rows, 100000 + acknowledged.size);
    assert.deepEqual(recovered, acknowledged);
    assert.equal(state.ram.tail.size, 700);
  }
});

test('seal publishes packs and a newer root before pruning covered logs; empty seal does nothing', () => {
  const state = createSimulation({count: 100000});
  const before = {root: state.s3.root.generation, packs: state.s3.packs.length};
  for (let i = 0; i < SEAL_LOGS; i++) assert.equal(writeBatch(state).shouldSeal, i === SEAL_LOGS - 1);
  assert.equal(state.s3.logs.size, SEAL_LOGS);
  const published = seal(state);
  assert.equal(published.rows, 3200);
  assert.equal(state.ram.tail.size, 0); assert.equal(state.s3.logs.size, 0);
  assert.ok(state.s3.packs.length > before.packs);
  assert.equal(state.s3.root.generation, before.root + 1);
  assert.equal(state.s3.root.sequence, SEAL_LOGS);
  assert.equal(state.ram.directory.sealed.size, 3200);
  assert.equal(seal(state), null);
  for (const pack of state.s3.packs) {
    assert.ok(pack.blocks.length <= 12);
    assert.ok(pack.blocks.reduce((n, id) => n + state.s3.blocks[id].bytes, 0) <= MiB);
  }
});

test('restart publishes permanent log/root fences, skips the marker and replays the unsealed tail', () => {
  const state = createSimulation({count: 100000});
  const batch = writeBatch(state); crash(state);
  const selected = {...state.s3.root};
  beginRestart(state);
  assert.throws(() => query(state, midpoint), /Restart/);
  assert.equal(restartStep(state).progress, .2);
  restartStep(state);
  assert.deepEqual(state.s3.fences, [{log: batch.sequence + 1, root: selected.generation + 1}]);
  assert.deepEqual(state.s3.root, selected);
  while (state.status === 'recovering') restartStep(state);
  assert.equal(state.ram.tail.size, 100);
  const fences = structuredClone(state.s3.fences);
  seal(state); assert.deepEqual(state.s3.fences, fences);
  assert.equal(state.s3.root.generation, selected.generation + 2);
  assert.equal(writeBatch(state).sequence, batch.sequence + 2);
});

test('LRU touches protect recent blocks; eviction and oversize admissions respect capacity', () => {
  const cache = new LRU(2 * BLOCK_BYTES);
  cache.admit('a', BLOCK_BYTES); cache.admit('b', BLOCK_BYTES); cache.touch('a'); cache.admit('c', BLOCK_BYTES);
  assert.deepEqual([...cache.entries.keys()], ['a', 'c']);
  assert.equal(cache.evictions, 1); assert.equal(cache.bytes, cache.capacity);
  assert.equal(cache.admit('oversize', 3 * BLOCK_BYTES), false);
  const state = createSimulation({count: 100000, ssdCapacity: 3 * BLOCK_BYTES, ramCapacity: 0});
  for (const point of [{x: .2, y: .2}, midpoint, {x: .8, y: .8}]) {
    query(state, point); assert.ok(state.ssd.bytes <= state.ssd.capacity);
  }
  assert.ok(state.ssd.evictions > 0);
});

test('idle warm-up admits at most 256KiB to SSD only and stops without evicting query contents', () => {
  const state = createSimulation({count: 100000, ssdCapacity: 20 * BLOCK_BYTES});
  query(state, midpoint);
  const entries = [...state.ssd.entries.keys()], hot = state.ram.hot.bytes, evictions = state.ssd.evictions;
  for (let i = 0; i < 10; i++) {
    const added = warmStep(state);
    assert.ok(added.reduce((n, b) => n + b.bytes, 0) <= 256 * 1024);
  }
  assert.ok(state.warmReads > 0);
  assert.ok(state.ssd.entries.size > entries.length);
  assert.equal(state.ram.hot.bytes, hot);
  assert.equal(state.ssd.evictions, evictions);
  assert.deepEqual([...state.ssd.entries.keys()].slice(0, entries.length), entries);
  assert.ok(state.ssd.bytes <= state.ssd.capacity);
});

test('exact rerank is top-10 over every fetched row and the live unsealed tail', () => {
  const state = createSimulation({count: 100000});
  const written = writeBatch(state, midpoint);
  const point = written.rows[0];
  const result = query(state, point);
  const oracle = [...result.reads.flatMap(r => [...blockRows(state, r.block)]), ...state.ram.tail.values()]
    .map(row => ({...row, distance: (row.x - point.x) ** 2 + (row.y - point.y) ** 2}))
    .sort((a, b) => a.distance - b.distance || a.id - b.id).slice(0, 10);
  assert.deepEqual(result.results, oracle);
  assert.equal(result.results[0].id, point.id);
});

test('128D size accounting grows with represented rows, not the drawn sample', () => {
  const measured = [100000, 1000000, 10000000].map(count => sizes(createSimulation({count})));
  for (let i = 1; i < measured.length; i++) {
    assert.equal(measured[i].directory, measured[i - 1].directory * 10);
    assert.equal(measured[i].sketches, measured[i - 1].sketches * 10);
    assert.ok(measured[i].s3 > measured[i - 1].s3 * 9);
    assert.ok(measured[i].ramTotal > measured[i - 1].ramTotal * 9);
  }
  assert.equal(measured[0].directory, 100000 * 24);
  assert.equal(measured[0].sketches, 100000 * 92);
});


test('a completed warm-up serves a query from SSD while the RAM block cache stays cold', () => {
  const state = createSimulation({count: 100000});
  while (state.warmCursor < state.s3.blocks.length) assert.ok(warmStep(state).length > 0);
  assert.equal(state.ram.hot.bytes, 0);
  const result = query(state, midpoint);
  assert.equal(result.sources.ram, 0);
  assert.equal(result.sources.s3, 0);
  assert.ok(result.sources.ssd > 0);
});


test('preset positions stay fixed across repeated queries, writes and dataset changes without a view', () => {
  const expected = [{x: .24, y: .24}, {x: .5, y: .5}, {x: .76, y: .76}];
  for (const count of [100000, 1000000, 10000000]) {
    const state = createSimulation({count});
    for (const point of QUERY_PRESETS) { query(state, point); writeBatch(state, point); }
    assert.deepEqual(QUERY_PRESETS, expected);
  }
  assert.throws(() => { QUERY_PRESETS[1].x += .01; }, TypeError);
  assert.throws(() => { QUERY_PRESETS.push(midpoint); }, TypeError);
});

test('the same preset twice reads zero S3 blocks on the second plan with lower simulated latency', () => {
  for (const count of [100000, 1000000, 10000000]) {
    for (const point of QUERY_PRESETS) {
      const state = createSimulation({count});
      const first = query(state, point), second = query(state, point);
      const context = `seed=${SEED}, count=${count}, preset=${JSON.stringify(point)}`;
      assert.ok(first.sources.s3 > 0, context);
      assert.equal(second.sources.s3, 0, context);
      assert.equal(second.bytes, 0, context);
      assert.equal(second.sources.ram + second.sources.ssd, second.reads.length, context);
      assert.ok(second.latency < first.latency / 4, context);
      assert.deepEqual(second.results, first.results, context);
    }
  }
});

test('organic layouts cover the panel, vary populations and density, and preserve every ID', () => {
  for (const count of [100000, 1000000, 10000000]) {
    const dataset = generateDataset(count), points = sampleDataset(dataset);
    const context = `seed=${SEED}, count=${count}`;
    assert.equal(dataset.centroids.reduce((n, c) => n + c.count, 0), count, context);
    let next = 1;
    for (const c of dataset.centroids) {
      assert.equal(c.firstId, next, context); next += c.count;
      assert.equal(basePoint(dataset, c.firstId).cluster, c.id, context);
      assert.equal(basePoint(dataset, next - 1).cluster, c.id, context);
    }
    assert.ok(new Set(dataset.centroids.map(c => c.count)).size > 10, context);
    assert.ok(new Set(dataset.centroids.map(c => c.radius)).size > 10, context);
    assert.ok(points.every(p => p.x > 0 && p.x < 1 && p.y > 0 && p.y < 1), context);
    for (const axis of ['x', 'y']) {
      assert.ok(Math.min(...points.map(p => p[axis])) < .08, context);
      assert.ok(Math.max(...points.map(p => p[axis])) > .92, context);
    }
    // Soft overlaps in a projected embedding are intentional, while posting
    // membership stays stable (writes still route to their nearest centre).
    assert.ok(points.some(p => p.cluster !== assignCluster(dataset, p)), context);
  }
});

test('query animation settles parallel batches by elapsed time, including skipped frames', () => {
  assert.ok(QUERY_ANIMATION_MS <= 1500);
  assert.equal(queryAnimationProgress(0, 8).stage, 0);
  assert.equal(queryAnimationProgress(140, 8).stage, 1);
  assert.equal(queryAnimationProgress(280, 8).stage, 2);
  assert.equal(queryAnimationProgress(639, 8).remoteDone, 0);
  assert.equal(queryAnimationProgress(640, 8).remoteDone, 4);
  assert.equal(queryAnimationProgress(1000, 8).remoteDone, 8);
  for (const remote of [0, 1, 4, 5, 8]) {
    assert.equal(queryAnimationProgress(QUERY_ANIMATION_MS, remote).done, true);
    assert.equal(queryAnimationProgress(60000, remote).remoteDone, remote);
  }
});

// Exercise the real view with a DOM adapter and controllable clock. No browser
// frames are delivered unless a test requests them: stats/admission must work
// even before the first frame or while the tab is throttled.
async function viewHarness(reducedMotion = false) {
  const {readFile} = await import('node:fs/promises');
  const {runInNewContext} = await import('node:vm');
  const model = await import('./sim-model.mjs');
  const html = await readFile(new URL('./index.html', import.meta.url), 'utf8');
  const source = await readFile(new URL('./sim-view.mjs', import.meta.url), 'utf8');
  const drawing = new Proxy({}, {get: () => () => {}, set: () => true});
  class Element {
    constructor() {
      this.children = []; this.dataset = {}; this.style = {}; this.listeners = new Map();
      this.classList = {toggle() {}}; this.checked = false; this.disabled = false;
    }
    append(...children) { this.children.push(...children); }
    replaceChildren(...children) { this.children = children; }
    addEventListener(event, handler) { this.listeners.set(event, handler); }
    dispatch(event = 'click', properties = {}) {
      if (event === 'click') assert.equal(this.disabled, false, 'control must remain usable');
      return this.listeners.get(event)?.({detail: 1, ...properties});
    }
    setAttribute() {}
    getContext() { return drawing; }
    getBoundingClientRect() { return {left: 0, top: 0, width: 375, height: 390}; }
    getAnimations() { return []; }
    focus() {}
    animate() { return {finished: Promise.resolve(), cancel() {}}; }
  }
  const elements = new Map([...html.matchAll(/id="(sim-[^"]+)"/g)].map(match => [match[1], new Element()]));
  const get = id => { assert.ok(elements.has(`sim-${id}`), `missing DOM ID: sim-${id}`); return elements.get(`sim-${id}`); };
  get('size').value = '1000000'; get('capacity').value = '256'; get('offline').firstElementChild = new Element();
  const presets = [0, 1, 2].map(id => { const button = new Element(); button.dataset.query = String(id); return button; });
  const phases = [0, 1, 2, 3].map(id => { const row = new Element(); row.dataset.phase = String(id); return row; });
  const document = new Element(); document.hidden = false;
  document.getElementById = id => elements.get(id) ?? null;
  document.createElement = () => new Element();
  document.querySelectorAll = selector => selector === '[data-query]' ? presets : phases;
  const motion = new Element(); motion.matches = reducedMotion;
  let now = 0, serial = 0, idle;
  const frames = new Map(), timers = new Map();
  runInNewContext(source.replace(/^import\s*\{[^}]+\}\s*from\s*'\.\/sim-model.mjs';/, ''), {
    ...model, document, matchMedia: () => motion, devicePixelRatio: 1,
    performance: {now: () => now}, ResizeObserver: class {observe() {}},
    requestAnimationFrame: callback => { frames.set(++serial, callback); return serial; },
    cancelAnimationFrame: id => frames.delete(id),
    setTimeout: (callback, ms) => { timers.set(++serial, {callback, at: now + ms}); return serial; },
    clearTimeout: id => timers.delete(id), setInterval: callback => { idle = callback; },
  }, {filename: 'sim-view.mjs'});
  const advance = (ms, deliverFrames = false) => {
    now += ms;
    for (const [id, timer] of [...timers]) if (timer.at <= now) { timers.delete(id); timer.callback(); }
    if (deliverFrames) for (const [id, callback] of [...frames]) { frames.delete(id); callback(now); }
  };
  return {get, presets, advance, idle: () => idle(), motion, document, timers};
}

test('view immediately shows the current plan; repeated and interrupted clicks hit cache before any animation frame', async () => {
  const view = await viewHarness();
  view.presets[1].dispatch();
  assert.match(view.get('remote').textContent, /^8 blocks \(960 KiB\)$/);
  assert.equal(view.get('cache').textContent, '0 blocks');
  const coldLatency = parseFloat(view.get('latency').textContent);
  assert.equal(view.get('again').disabled, false);
  view.presets[1].dispatch(); // Interrupt before either RAF or deadline fires.
  assert.equal(view.get('remote').textContent, '0 blocks (0 KiB)');
  assert.equal(view.get('cache').textContent, '8 blocks');
  assert.equal(view.get('sources').textContent, '8 / 0 / 0');
  assert.ok(parseFloat(view.get('latency').textContent) < coldLatency / 4);
  view.get('again').dispatch();
  assert.equal(view.get('remote').textContent, '0 blocks (0 KiB)');
  // No frames at all: the deadline must still finish and unblock idle writes.
  view.advance(1200);
  assert.equal(view.timers.size, 0);
  assert.match(view.get('caption').textContent, /Cache hit/);
  view.get('stream').dispatch(); view.idle();
  assert.equal(view.get('count').textContent, '1,000,100 vectors');
  view.presets[0].dispatch(); view.get('reset').dispatch(); view.advance(60000, true);
  assert.equal(view.get('remote').textContent, '—');
  assert.equal(view.get('again').disabled, true);
  assert.equal(view.get('count').textContent, '1,000,000 vectors');
});

test('view keeps seal, crash, lost-disk recovery and dataset controls usable with reduced motion', async () => {
  const view = await viewHarness(true);
  view.get('stream').dispatch(); view.idle(); view.get('stream').dispatch();
  assert.equal(view.get('seal').disabled, false);
  await view.get('seal').dispatch();
  assert.equal(view.get('seal').disabled, true);
  view.presets[1].dispatch();
  assert.match(view.get('caption').textContent, /Query complete/);
  view.get('lose-disk').checked = true;
  view.get('crash').dispatch();
  assert.equal(view.get('ram-size').textContent, '0 B');
  assert.equal(view.get('ssd-size').textContent, '0 B');
  assert.equal(view.presets[1].disabled, true);
  await view.get('restart').dispatch();
  assert.equal(view.presets[1].disabled, false);
  assert.equal(view.get('count').textContent, '1,000,100 vectors');
  assert.match(view.get('caption').textContent, /100 acknowledged upserts survived/);
  view.get('size').value = '100000'; view.get('size').dispatch('change');
  assert.equal(view.get('count').textContent, '100,000 vectors');
  view.presets[1].dispatch(); view.get('again').dispatch();
  assert.equal(view.get('remote').textContent, '0 blocks (0 KiB)');
});
