import test from 'node:test';
import assert from 'node:assert/strict';
import {points, search, storedVector, queryBody, commands} from './playground-model.mjs';

const defaults = {vector: [6.5, 4.5], metric: 'squared_euclidean', k: 5, kind: 'all', maxPrice: null};
test('nearest neighbors use squared L2 and deterministic distance/ID order', () => {
  const data = [
    {id: 3, vector: [1, 0], metadata: {}},
    {id: 2, vector: [-1, 0], metadata: {}},
    {id: 1, vector: [3, 4], metadata: {}},
  ];
  assert.deepEqual(search({...defaults, vector: [0, 0]}, data).results.map(p => [p.id, p.distance]), [[2, 1], [3, 1], [1, 25]]);
  assert.equal(search({...defaults, vector: [0, 0], metric: 'manhattan'}, data).results[2].distance, 7);
});
test('equality and numeric filters form a conjunction; fewer than K and empty results are honest', () => {
  const result = search({...defaults, kind: 'docs', maxPrice: 3});
  assert.deepEqual(result.results.map(p => p.id), [20, 1]);
  assert.equal(result.eligible.length, 2);
  assert.equal(search({...defaults, maxPrice: 0}).results.length, 0);
  const data = [{id: 1, vector: [1, 1], metadata: {kind: 'docs'}},
    {id: 2, vector: [1, 1], metadata: {kind: 'docs', price: 'invalid'}}];
  assert.equal(search({...defaults, kind: 'docs', maxPrice: 3}, data).results.length, 0);
});
test('cosine uses f32 unit vectors, clamps rounding, and rejects zero vectors', () => {
  assert.deepEqual(storedVector([3, 4], 'cosine'), [Math.fround(.6), Math.fround(.8)]);
  const data = [{id: 1, vector: [2, 2], metadata: {}}, {id: 2, vector: [2, -2], metadata: {}}];
  const result = search({...defaults, vector: [1, 1], metric: 'cosine'}, data).results;
  assert.ok(result[0].distance >= 0 && result[0].distance < 1e-7);
  assert.equal(result[1].distance, 1);
  assert.throws(() => search({...defaults, vector: [0, 0], metric: 'cosine'}), /nonzero/);
});
test('invalid inputs cannot generate misleading results', () => {
  for (const vector of [[NaN, 1], [Infinity, 1], [1e40, 1], [1], ['1', 1]]) {
    assert.throws(() => search({...defaults, vector}), /finite/);
  }
  for (const k of [0, -1, 1.5, 1001]) assert.throws(() => search({...defaults, k}), /integer/);
});
test('every metric has its own collection and exports the exact demo data and filter', () => {
  for (const metric of ['squared_euclidean', 'manhattan', 'cosine']) {
    const options = {...defaults, metric, kind: 'memory', maxPrice: 5};
    const code = commands(options);
    const bodies = [...code.setup.matchAll(/-d '([^']+)'/g)].map(match => JSON.parse(match[1]));
    assert.equal(bodies[0].dimensions, 2);
    assert.equal(bodies[0].metric, metric);
    assert.deepEqual(bodies[1].upsert, points);
    assert.deepEqual(JSON.parse(code.query.match(/-d '([^']+)'/)[1]), queryBody(options));
    assert.deepEqual(queryBody(options).filter, {kind: 'memory', price: {$lte: 5}});
    assert.equal(queryBody(options).exact, true);
    assert.ok(code.query.includes(`/collections/${bodies[0].name}/query`));
    assert.ok(!code.setup.includes('\n+'));
  }
});
