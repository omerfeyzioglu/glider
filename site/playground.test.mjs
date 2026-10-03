import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {recording, blockSources, fetchedBytes, efforts} from './playground-model.mjs';
const data = JSON.parse(readFileSync(new URL('./explorer-data.json', import.meta.url)));
test('every UI state resolves to an engine recording and respects its read budget', () => {
  for (const query of [0, 1, 2]) for (const effort of Object.keys(efforts)) for (const tier of ['cold', 'ssd', 'ram']) {
    const run = recording(data, {query, effort, tier});
    assert.ok(run.requests <= efforts[effort].requests);
    assert.ok(run.bytes <= efforts[effort].bytes);
    assert.equal(run.probed.length, efforts[effort].probes);
    assert.ok(run.hits.length <= 3);
    if (effort === 'wide') assert.equal(run.hits.length, 3);
    assert.equal(run.recall, run.hits.filter(h => run.oracle.includes(h.id)).length / 3);
    if (tier !== 'cold') assert.equal(run.requests, 0);
  }
  assert.equal(data.records.length, 27);
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

test('semantic searches return actual documents from the indexed collection', () => {
  const expected = ['Cache loss does not lose data', 'Filter by metadata', 'Queries and writes run together'];
  for (const [query,title] of expected.entries()) {
    const run = recording(data, {query});
    assert.ok(run.hits.some(h => data.documents.find(d => d.id === h.id).title === title));
    assert.ok(run.hits.every(h => data.documents.some(d => d.id === h.id && d.text && d.url)));
  }
});
