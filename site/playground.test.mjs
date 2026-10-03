import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {STEPS, MEASURED, initial, advance, view} from './playground-model.mjs';
import recording from './search-recording.mjs';

function walk() {
  const states = [initial()];
  for (let i = 0; i < STEPS.length; i++) states.push(advance(states.at(-1)));
  return states.map(view);
}

test('steps run in a fixed order and the next one is highlighted', () => {
  assert.deepEqual(STEPS.map(s => s.id), ['write', 'query', 'again', 'crash', 'restart']);
  assert.deepEqual(walk().map(v => v.next), ['write', 'query', 'again', 'crash', 'restart', null]);
  let state = initial();
  for (let i = 0; i < STEPS.length; i++) state = advance(state);
  assert.throws(() => advance(state), /done/);
});

test('reset returns to the empty initial state', () => {
  const fresh = view(initial());
  assert.deepEqual(fresh.tiers, {ram: false, ssd: false, s3: false});
  assert.equal(fresh.next, 'write');
  assert.equal(fresh.results, false);
  assert.deepEqual(view(initial()), fresh);
});

test('the write fills S3 only, and the first query fills both caches', () => {
  const [, written, cold, warm] = walk();
  assert.deepEqual(written.tiers, {ram: false, ssd: false, s3: true});
  assert.deepEqual(written.hops.at(-1), {from: 'server', to: 's3', label: '24 docs'});
  assert.deepEqual(cold.tiers, {ram: true, ssd: true, s3: true});
  assert.ok(cold.hops.some(h => h.from === 's3'));
  assert.ok(!warm.hops.some(h => h.from === 's3' || h.to === 's3'));
});

test('counters show measured latencies and where each answer came from', () => {
  const [start, written, cold, warm, crashed, restarted] = walk();
  assert.deepEqual([start, written, cold, warm, crashed, restarted].map(v => v.source.value), ['—', '—', 'S3', 'RAM', '—', 'S3']);
  assert.deepEqual([written, cold, warm, restarted].map(v => v.latency.value), [MEASURED.write, MEASURED.cold, MEASURED.warm, MEASURED.cold]);
  assert.equal(crashed.latency.value, '—');
  assert.deepEqual([cold, warm, crashed, restarted].map(v => v.results), [true, true, false, true]);
});

test('a crash empties RAM and SSD only, and acknowledged writes lost stays 0', () => {
  const views = walk();
  const crashed = views[4];
  assert.deepEqual(crashed.tiers, {ram: false, ssd: false, s3: true});
  assert.equal(crashed.server, 'Killed');
  assert.deepEqual(crashed.hops, []);
  for (const v of views) assert.equal(v.lost, 0);
  for (const v of views.slice(1)) assert.equal(v.tiers.s3, true);
});

test('restart reads from S3 again with the same path as the first query', () => {
  const views = walk();
  assert.deepEqual(views[5].hops, views[2].hops);
  assert.deepEqual(views[5].tiers, {ram: true, ssd: true, s3: true});
});

test('results are the engine recording for the question shown', () => {
  assert.equal(recording.question, 'Will I lose my data if the SSD cache disappears?');
  assert.equal(recording.documents, 24);
  assert.deepEqual(recording.results.map(r => r.title), ['Cache loss does not lose data', 'Authenticate cached blocks', 'Recover after a restart']);
  for (const r of recording.results) assert.match(r.url, /^https:\/\/github\.com\/omerfeyzioglu\/glider\/blob\/main\//);
});

test('the page cites the measured numbers it shows', () => {
  const benchmarks = readFileSync(new URL('../benchmarks/M39.md', import.meta.url), 'utf8');
  const current = benchmarks.slice(benchmarks.indexOf('## Current main on S3'));
  assert.match(current, /\| Warm unfiltered p95 \| [^|]+ \| 29\.8 ms \|/);
  assert.match(current, /\| Cold unfiltered p95 \| [^|]+ \| 57\.0 ms \|/);
  assert.match(current, /\| Write p95 \(p50\) \| [^|]+ \| 86\.9 ms/);
});
