// "The life of a query": the playground's state machine. Latencies are p95
// values measured on AWS S3 (benchmarks/M39.md#current-main-on-s3).
export const MEASURED = Object.freeze({write: '86.9 ms', cold: '57 ms', warm: '29.8 ms'});

export const STEPS = Object.freeze([
  {id: 'write', title: 'Write', detail: 'Upsert 24 docs pages'},
  {id: 'query', title: 'First query', detail: 'Nothing cached yet'},
  {id: 'again', title: 'Same query again', detail: 'Caches are warm'},
  {id: 'crash', title: 'Crash the server', detail: 'kill -9'},
  {id: 'restart', title: 'Restart & query', detail: 'A new server takes over'},
]);

const coldRead = [
  {from: 'client', to: 'server', label: 'query'},
  {from: 'server', to: 's3', label: 'read'},
  {from: 's3', to: 'ssd', label: 'blocks'},
  {from: 'ssd', to: 'ram', label: 'blocks'},
  {from: 'ram', to: 'server', label: 'blocks'},
  {from: 'server', to: 'client', label: 'top 3'},
];

// AFTER[n] is the state once n steps are done; its `hops` animate step n.
const AFTER = [
  {
    tiers: {ram: false, ssd: false, s3: false}, server: 'Ready', results: false,
    source: {value: '—', note: 'no query yet'}, latency: {value: '—', note: 'no query yet'},
    lostNote: 'nothing written yet',
    caption: 'Glider keeps your data in S3. RAM and SSD only make it fast. Press Write to start.',
    hops: [],
  },
  {
    tiers: {ram: false, ssd: false, s3: true}, server: 'Running', results: false,
    source: {value: '—', note: 'no query yet'}, latency: {value: MEASURED.write, note: 'p95 durable write'},
    lostNote: 'the write is in S3',
    caption: 'The write is acknowledged only after it is stored in S3.',
    hops: [{from: 'client', to: 'server', label: '24 docs'}, {from: 'server', to: 's3', label: '24 docs'}],
  },
  {
    tiers: {ram: true, ssd: true, s3: true}, server: 'Running', results: true,
    source: {value: 'S3', note: 'a cold read'}, latency: {value: MEASURED.cold, note: 'p95 cold query'},
    lostNote: 'S3 holds every write',
    caption: 'A cold query reads a few ranges from S3 (at most 8 per query) and keeps them in SSD and RAM.',
    hops: coldRead,
  },
  {
    tiers: {ram: true, ssd: true, s3: true}, server: 'Running', results: true,
    source: {value: 'RAM', note: 'no S3 read'}, latency: {value: MEASURED.warm, note: 'p95 under concurrent load'},
    lostNote: 'S3 holds every write',
    caption: 'The same query is answered from RAM, with no trip to S3.',
    hops: [
      {from: 'client', to: 'server', label: 'query'},
      {from: 'server', to: 'ram', label: 'read'},
      {from: 'ram', to: 'server', label: 'blocks'},
      {from: 'server', to: 'client', label: 'top 3'},
    ],
  },
  {
    tiers: {ram: false, ssd: false, s3: true}, server: 'Killed', results: false,
    source: {value: '—', note: 'server is down'}, latency: {value: '—', note: 'server is down'},
    lostNote: 'after kill -9',
    caption: 'The server is killed and its disk is lost too: RAM and SSD are empty, but S3 still holds every acknowledged write.',
    hops: [],
  },
  {
    tiers: {ram: true, ssd: true, s3: true}, server: 'Restarted', results: true,
    source: {value: 'S3', note: 'caches were empty'}, latency: {value: MEASURED.cold, note: 'p95 cold query'},
    lostNote: 'every write survived',
    caption: 'A new server takes over (lease + fencing), reads from S3 again, refills its caches and returns the same results.',
    hops: coldRead,
  },
];

export function initial() {
  return {done: 0};
}

export function advance(state) {
  if (state.done >= STEPS.length) throw new Error('All steps are done');
  return {done: state.done + 1};
}

export function view(state) {
  const after = AFTER[state.done];
  if (!after) throw new Error(`Unknown step count ${state.done}`);
  return {...after, lost: 0, next: STEPS[state.done]?.id ?? null};
}
