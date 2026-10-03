"""Tests for Markdown heading slugs and fenced link extraction."""

from pathlib import Path
import sys
import unittest


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))
import check_links


class CheckLinksTests(unittest.TestCase):
    def test_slug(self):
        self.assertEqual(check_links.slug("Hello, World! `API`_v2"), "hello-world-api_v2")
        self.assertEqual(check_links.slug("A [link](guide.md) & More"), "a-link-more")
        self.assertEqual(check_links.heading_slugs("# Same\n## Same\n# `API` v2\n"),
                         {"same", "same-1", "api-v2"})

    def test_extract_links_skips_fenced_blocks(self):
        content = ("[local](docs/API.md#write) [web](https://example.com)\n"
                   "```sh\n[ignored](missing.md)\n```\n"
                   "~~~markdown\n[also ignored](missing.md)\n~~~\n"
                   "[next](../README.md)\n")
        self.assertEqual(list(check_links.extract_links(content)), [
            ("[local](docs/API.md#write)", "docs/API.md#write"),
            ("[web](https://example.com)", "https://example.com"),
            ("[next](../README.md)", "../README.md"),
        ])


if __name__ == "__main__":
    unittest.main()
