export const efforts = Object.freeze({
  lean: {probes: 2, requests: 1, bytes: 16384},
  balanced: {probes: 8, requests: 2, bytes: 65536},
  wide: {probes: 16, requests: 8, bytes: 262144},
});
export function recording(data, {query = 0, effort = 'balanced', tier = 'cold', fresh = false} = {}) {
  const result = data.records.find(r => r.query === query && r.effort === effort && r.tier === tier && r.fresh === fresh);
  if (!result) throw new Error('Unknown query state');
  return result;
}
export function blockSources(data, run) {
  const selected = new Map(run.selected.map(b => [b.id, b.source]));
  return data.blocks.map(block => ({...block, source: selected.get(block.id) ?? 'unread'}));
}
export function fetchedBytes(bytes) {
  return bytes === 0 ? '0 B' : `${(bytes / 1024).toFixed(1)} KiB`;
}
