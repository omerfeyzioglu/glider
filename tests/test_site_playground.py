"""Check browser demo scoring and its exported curl against the real engine."""

import json
from pathlib import Path
import subprocess
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'clients/python/tests'))
from support import RealServer, requires_server  # noqa: E402
from glider_client import Client  # noqa: E402


@requires_server
class PlaygroundOracleTests(unittest.TestCase):
    def test_exported_dataset_and_queries_match_glider(self):
        # The same module drives the page, the curl exports and this oracle check.
        script = """
          import {search, queryBody, commands} from './site/playground-model.mjs';
          const fixtures = [];
          for (const metric of ['squared_euclidean', 'manhattan', 'cosine']) {
            const cases = [];
            const vectors = [[6.5, 4.5], [2, 2], [9.9, .1], [1.2, 2.1]];
            if (metric !== 'cosine') vectors.push([0, 0]);
            for (const vector of vectors) {
              for (const kind of ['all', 'docs', 'memory', 'ops']) {
                for (const maxPrice of [null, 3, 5, 0]) {
                  for (const k of [1, 5, 10]) {
                    const options = {vector, metric, kind, maxPrice, k};
                    cases.push({body: queryBody(options), expected: search(options).results});
                  }
                }
              }
            }
            fixtures.push({commands: commands({vector:[6.5,4.5], metric, kind:'docs', maxPrice:3, k:5}), cases});
          }
          process.stdout.write(JSON.stringify(fixtures));
        """
        fixtures = json.loads(subprocess.check_output(
            ['node', '--input-type=module', '-e', script], cwd=ROOT, text=True))
        server = RealServer()
        self.addCleanup(server.stop)
        client = Client(server.url)
        for fixture in fixtures:
            setup = fixture['commands']['setup'].replace('http://localhost:8080', server.url)
            loaded = subprocess.run(['sh', '-c', 'set -e\n' + setup],
                                    capture_output=True, text=True)
            self.assertEqual(loaded.returncode, 0, loaded.stderr)
            query = fixture['commands']['query'].replace('http://localhost:8080', server.url)
            exported = subprocess.run(['sh', '-c', query], capture_output=True, text=True)
            self.assertEqual(exported.returncode, 0, exported.stderr)
            self.assertEqual([hit['id'] for hit in json.loads(exported.stdout)['results']], [20, 1])
            # Extract the collection from the exported URL, so route drift fails.
            route = query.split("'")[1].removeprefix(server.url)
            for case in fixture['cases']:
                with self.subTest(route=route, body=case['body']):
                    actual = client._request('POST', route, case['body'])['results']
                    self.assertEqual([p['id'] for p in actual], [p['id'] for p in case['expected']])
                    for found, expected in zip(actual, case['expected']):
                        self.assertEqual(found['metadata'], expected['metadata'])
                        self.assertAlmostEqual(found['distance'], expected['distance'], delta=1e-12)


if __name__ == '__main__':
    unittest.main()
