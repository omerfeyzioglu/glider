#!/usr/bin/env python3
"""Encode the curated website documentation collection; runtime needs no model."""
import json
from pathlib import Path

MODEL = 'sentence-transformers/all-MiniLM-L6-v2'
MODEL_REVISION = '1110a243fdf4706b3f48f1d95db1a4f5529b4d41'
ROOT = Path(__file__).resolve().parents[1]
# Summaries of current public documentation; links give each result its context.
CARDS = [
 ('Cache loss does not lose data', 'The RAM and SSD caches are disposable acceleration. Acknowledged writes remain in object storage; losing the local cache makes queries cold again without losing committed data.', 'DESIGN.md#persisted-pack-sketches-and-selective-reads', 'Storage'),
 ('Warm searches avoid remote reads', 'The server warms its local SSD block cache in the background. Queries can read selected vector blocks from RAM or SSD instead of downloading them from object storage.', 'DESIGN.md#persisted-pack-sketches-and-selective-reads', 'Storage'),
 ('The bucket is authoritative', 'Glider acknowledges a write after publishing a complete immutable log object. Roots and logs define the committed state; local caches are derived and never replace durable storage.', 'DESIGN.md#acknowledgement-and-recovery', 'Storage'),
 ('Recover after a restart', 'A restarted server takes over the collection, fences its previous writer and replays the durable log. It serves every acknowledged write after recovery.', 'docs/API.md#consistency', 'Storage'),
 ('Immutable vector packs', 'Full vectors live in immutable object-store packs. In memory, the segmented engine retains the latest-ID directory, routing sketches and the unsealed log tail.', 'DESIGN.md#engines-and-namespace-compatibility', 'Storage'),
 ('Authenticate cached blocks', 'Block bytes from RAM, SSD and object storage are authenticated against the committed block digest before decoding and scoring. Corrupt cached entries cannot become authoritative data.', 'DESIGN.md#persisted-pack-sketches-and-selective-reads', 'Storage'),
 ('Filter by metadata', 'Search supports equality, set membership, existence, numeric comparisons and logical combinations over string metadata. The full predicate is checked against the records read for the query.', 'docs/API.md#filters', 'Search'),
 ('Get complete filtered results', 'Add exact: true to search all live points and evaluate the full metadata filter exhaustively. The query returns up to k matches; bounded approximate search can miss eligible points.', 'docs/API.md#filters', 'Search'),
 ('Combine numeric and category filters', 'A filter can combine a color equality with a price range and tag membership. Conditions on the same key and different members of one filter object are ANDed.', 'docs/API.md#filters', 'Search'),
 ('Route declared filter keys', 'Required equality predicates on declared routed metadata keys restrict candidate routing. Glider still applies the complete filter to authenticated records during reranking.', 'DESIGN.md#persisted-pack-sketches-and-selective-reads', 'Search'),
 ('Choose your distance metric', 'A collection has a fixed dimension and a fixed metric: squared Euclidean, Manhattan or cosine. Cosine vectors and queries must have nonzero norm and are normalized by the engine.', 'DESIGN.md#engines-and-namespace-compatibility', 'Search'),
 ('Exact search is the recall oracle', 'Exact search scores every live matching vector and sorts by distance then ID. Approximate query results are evaluated against exact neighbors using recall at k.', 'docs/API.md#post-v1query', 'Search'),
 ('Queries and writes run together', 'Reader threads search immutable published views while the committer accepts writes. Each query observes a consistent snapshot; readers do not wait behind a long object-store write.', 'DESIGN.md#concurrent-queries-on-published-views-m34', 'Concurrency'),
 ('Read your acknowledged writes', 'A query sent after a successful write acknowledgement observes that write. A snapshot never exposes a partially applied write group or a partially published root.', 'docs/API.md#consistency', 'Concurrency'),
 ('New writes are searchable immediately', 'Recent acknowledged writes stay in the in-memory log tail until sealing. Selective queries scan that tail exactly and merge it with neighbors reranked from selected immutable blocks.', 'docs/API.md#post-v1query', 'Concurrency'),
 ('Retry without applying a write twice', 'Each write carries a request ID. Resending a retained request returns its original outcome instead of applying its mutations again. The retry window is bounded.', 'docs/API.md#request-ids-and-retries', 'Concurrency'),
 ('Bound concurrent work', 'Admission limits count queued and active operations and their encoded payload bytes. Requests that exceed available capacity return overload instead of creating an unbounded queue.', 'DESIGN.md#bounded-concurrent-admission-m16', 'Concurrency'),
 ('One committer owns each collection', 'Glider establishes single-writer ownership of each collection. A takeover fences earlier writers, while queries can execute concurrently on published snapshots.', 'docs/API.md#consistency', 'Concurrency'),
 ('Start Glider with Docker', 'Run the Glider server on port 8080 with a mounted Docker volume to preserve local data. Then write points with curl and issue your first vector query.', 'docs/INSTALL.md', 'Getting started'),
 ('Use the Python client', 'The dependency-free Python client creates collections, upserts vectors and metadata, and returns nearest neighbors. Each collection selects its dimensions and distance metric.', 'README.md#quickstart', 'Getting started'),
 ('Browse collections in the console', 'Open localhost:8080/console to browse your collections and points, run queries, and inspect the Glider server from your browser.', 'docs/API.md#get-console', 'Getting started'),
 ('Run on S3 or MinIO', 'Glider supports S3-compatible object storage. Configure a bucket and a nonoverlapping namespace prefix; the service must provide strongly consistent reads and conditional object creation.', 'docs/INSTALL.md', 'Getting started'),
 ('Give agents durable memory', 'The glider-mcp client exposes remember, recall and forget tools to MCP clients. Memories survive restarts and request IDs support safe retries within the retained window.', 'README.md#agent-memory-mcp', 'Getting started'),
 ('Inspect query I/O', 'Set profile: true on a query to include its execution mode, elapsed time, queue wait and remote I/O counters. These counters describe the reads caused by that query.', 'docs/API.md#post-v1query', 'Getting started'),
]
QUESTIONS = [
 'Will I lose my data if the SSD cache disappears?',
]

def main():
    import torch
    from sentence_transformers import SentenceTransformer
    torch.set_num_threads(1)
    revision = MODEL_REVISION
    model = SentenceTransformer(MODEL, revision=revision, device='cpu', cache_folder='/tmp/glider-site-model-cache')
    documents = [dict(id=i, title=title, text=text, url='https://github.com/omerfeyzioglu/glider/blob/main/'+link, category=category) for i,(title,text,link,category) in enumerate(CARDS)]
    texts = [d['title']+'. '+d['text'] for d in documents]+QUESTIONS
    vectors = model.encode(texts, normalize_embeddings=True, show_progress_bar=False).tolist()
    for doc, vector in zip(documents, vectors): doc['vector'] = vector
    queries = [dict(text=text, vector=vector) for text,vector in zip(QUESTIONS,vectors[len(documents):])]
    data = dict(version=1, model=MODEL, model_revision=revision, dimensions=384, documents=documents, queries=queries)
    output = ROOT/'tests/fixtures/site-search-input.json'
    output.parent.mkdir(exist_ok=True)
    output.write_text(json.dumps(data,indent=2)+'\n')
    print(f'Encoded {len(documents)} documents and {len(queries)} queries with {MODEL}@{revision}')

if __name__ == '__main__': main()
