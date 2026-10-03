// Synthetic 2D data, deliberately small enough to inspect and reproduce.
export const points = [
  [1, 1.2, 2.1, 'docs', 2], [2, 2, 3.4, 'docs', 4],
  [3, 2.8, 1.5, 'docs', 8], [4, 3.2, 4.8, 'docs', 6],
  [5, 4.1, 3, 'memory', 3], [6, 4.8, 5.5, 'memory', 9],
  [7, 5.5, 3.8, 'docs', 5], [8, 6.2, 5, 'memory', 2],
  [9, 7.1, 4.2, 'ops', 7], [10, 7.8, 6.1, 'ops', 4],
  [11, 8.5, 3, 'ops', 6], [12, 9, 5.2, 'memory', 8],
  [13, 1.5, 7.5, 'memory', 3], [14, 2.7, 8.3, 'memory', 5],
  [15, 3.8, 6.8, 'docs', 7], [16, 5, 8.8, 'ops', 1],
  [17, 6.5, 7.2, 'memory', 4], [18, 7.5, 8.5, 'ops', 9],
  [19, 8.8, 7.5, 'ops', 2], [20, 4.5, 1.2, 'docs', 1],
  [21, 6.8, 1.8, 'memory', 6], [22, 8.2, 1, 'ops', 3],
  [23, 1, 5.5, 'docs', 9], [24, 9.2, 9.2, 'memory', 10],
].map(([id, x, y, kind, price]) => ({
  id, vector: [x, y], metadata: {kind, price: String(price)},
}));

export const metrics = {
  squared_euclidean: {name: 'Squared Euclidean', slug: 'l2', note: 'Ranks by squared Euclidean (straight-line) distance. Lower is closer.'},
  manhattan: {name: 'Manhattan', slug: 'l1', note: 'Ranks by the sum of absolute coordinate differences. Lower is closer.'},
  cosine: {name: 'Cosine', slug: 'cosine', note: 'Ranks by direction, not magnitude. Vectors are normalized before scoring; the plot shows the submitted coordinates.'},
};

export function storedVector(vector, metric) {
  if (!Array.isArray(vector) || vector.length !== 2 ||
      vector.some(x => typeof x !== 'number' || !Number.isFinite(Math.fround(x)))) {
    throw new Error('Enter two finite vector components.');
  }
  const values = vector.map(Math.fround);
  if (metric !== 'cosine') return values;
  const norm = Math.sqrt(values.reduce((sum, x) => sum + x * x, 0));
  if (norm === 0) throw new Error('Cosine needs a nonzero query vector. Change X or Y.');
  return values.map(x => Math.fround(x / norm));
}

export function queryBody({vector, k, kind, maxPrice}) {
  const filter = {};
  if (kind !== 'all') filter.kind = kind;
  if (maxPrice !== null) filter.price = {$lte: maxPrice};
  return {vector, k, filter, exact: true, include_metadata: true};
}

export function search(options, data = points) {
  const {vector, metric, k, kind, maxPrice} = options;
  if (!Object.hasOwn(metrics, metric)) throw new Error('Choose a supported distance metric.');
  if (!Number.isInteger(k) || k < 1 || k > 1000) throw new Error('K must be an integer from 1 to 1000.');
  if (!['all', 'docs', 'memory', 'ops'].includes(kind)) throw new Error('Choose a supported kind.');
  if (maxPrice !== null && (typeof maxPrice !== 'number' || !Number.isFinite(maxPrice))) {
    throw new Error('Enter a finite price limit.');
  }
  const query = storedVector(vector, metric);
  const eligible = data.filter(point =>
    (kind === 'all' || point.metadata.kind === kind) &&
    (maxPrice === null || (Object.hasOwn(point.metadata, 'price') &&
      point.metadata.price.trim() !== '' && Number.isFinite(Number(point.metadata.price)) &&
      Number(point.metadata.price) <= maxPrice)));
  const results = eligible.map(point => {
    const value = storedVector(point.vector, metric);
    const distance = metric === 'cosine'
      ? Math.max(0, 1 - query.reduce((dot, x, i) => dot + x * value[i], 0))
      : query.reduce((sum, x, i) => {
          const d = x - value[i];
          return sum + (metric === 'manhattan' ? Math.abs(d) : d * d);
        }, 0);
    return {id: point.id, distance, metadata: {...point.metadata}};
  }).sort((a, b) => a.distance - b.distance || a.id - b.id).slice(0, k);
  return {eligible, results};
}

export function commands(options) {
  const metric = metrics[options.metric];
  const name = `playground-${metric.slug}`;
  const base = `http://localhost:8080/v1/collections/${name}`;
  const curl = (url, body) => `curl --fail-with-body -sS '${url}' \\\n  -H 'content-type: application/json' \\\n  -d '${body}'`;
  return {
    query: curl(`${base}/query`, JSON.stringify(queryBody(options), null, 2)),
    setup: '# With a multi-collection Glider server running:\n' +
      curl('http://localhost:8080/v1/collections', JSON.stringify({name, dimensions: 2, metric: options.metric})) +
      '\n\n' + curl(`${base}/write`, '{"upsert":[\n' + points.map(p => '  ' + JSON.stringify(p)).join(',\n') + '\n]}'),
  };
}
