import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {recording, blockSources, fetchedBytes, efforts} from './playground-model.mjs';
const data = JSON.parse(readFileSync(new URL('./explorer-data.json', import.meta.url)));
test('every UI state resolves to an engine recording and respects its read budget', () => {
  for (const query of [0, 1]) for (const fresh of [false, true]) for (const effort of Object.keys(efforts)) for (const tier of ['cold', 'ssd', 'ram']) {
    const run = recording(data, {query, fresh, effort, tier});
    assert.ok(run.requests <= efforts[effort].requests);
    assert.ok(run.bytes <= efforts[effort].bytes);
    assert.equal(run.probed.length, efforts[effort].probes);
    assert.equal(run.hits.length, 5);
    assert.equal(run.recall, run.hits.filter(h => run.oracle.includes(h.id)).length / 5);
    if (tier !== 'cold') assert.equal(run.requests, 0);
    if (fresh) { assert.equal(run.hits[0].id, 9000 + query); assert.equal(run.hits[0].distance, 0); }
  }
  assert.equal(data.records.length, 36);
});
test('block colors reflect recorded read sources and leave untouched blocks unread', () => {
  for (const run of data.records) {
    const blocks = blockSources(data, run);
    assert.equal(blocks.filter(b => b.source !== 'unread').length, run.selected.length);
    assert.equal(blocks.filter(b => b.source === 'ssd').length, run.ssd_hits);
    assert.equal(blocks.filter(b => b.source === 'ram').length, run.ram_hits);
    assert.ok(run.selected.every(b => data.blocks.some(block => block.id === b.id)));
  }
});
test('unknown recordings fail explicitly and byte units are honest', () => {
  assert.throws(() => recording(data, {tier: 'missing'}), /Unknown/);
  assert.equal(fetchedBytes(0), '0 B');
  assert.equal(fetchedBytes(1024), '1.0 KiB');
});
