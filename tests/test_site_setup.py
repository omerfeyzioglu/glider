"""Validate independently copied setup commands, without UI labels or comments."""
import ast
from html.parser import HTMLParser
import json
from pathlib import Path
import shlex
import unittest


class Commands(HTMLParser):
    def __init__(self):
        super().__init__()
        self.section = None
        self.panel = None
        self.depth = 0
        self.panel_depth = None
        self.code = None
        self.in_pre = False
        self.commands = {}

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == 'section':
            self.section = attrs.get('id')
        if tag == 'div':
            self.depth += 1
            if attrs.get('role') == 'tabpanel':
                self.panel = attrs.get('id')
                self.panel_depth = self.depth
        if tag == 'pre':
            self.in_pre = True
        if tag == 'code' and self.in_pre and self.section in ('agents', 'quickstart'):
            self.code = []

    def handle_endtag(self, tag):
        if tag == 'pre':
            self.in_pre = False
        if tag == 'code' and self.code is not None:
            self.commands.setdefault((self.section, self.panel), []).append(''.join(self.code))
            self.code = None
        if tag == 'div':
            if self.depth == self.panel_depth:
                self.panel = self.panel_depth = None
            self.depth -= 1
        if tag == 'section':
            self.section = None

    def handle_data(self, data):
        if self.code is not None:
            self.code.append(data)


def shell(command):
    return shlex.split(command.replace('\\\n', ' '))


class SetupCommandsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        parser = Commands()
        parser.feed((Path(__file__).resolve().parents[1] / 'site/index.html').read_text())
        cls.commands = parser.commands

    def test_labels_and_comments_are_not_copied(self):
        for commands in self.commands.values():
            for command in commands:
                self.assertFalse(any(line.lstrip().startswith('#') for line in command.splitlines()))
                self.assertNotIn('Copy', command)
                self.assertNotIn('Start the server', command)

    def test_curl_steps_are_complete_independent_commands(self):
        start, write, query = self.commands[('quickstart', 'p-qc')]
        self.assertEqual(shell(start)[0], 'docker')
        self.assertIn('glider-quickstart-data:/var/lib/glider', shell(start))
        self.assertIn('GLIDER_DIMENSIONS=3', shell(start))
        self.assertIn('localhost:8080/v1/write', shell(write))
        payload = json.loads(shell(write)[shell(write).index('-d') + 1])
        self.assertEqual([row['id'] for row in payload['upsert']], [1, 2])
        self.assertIn('localhost:8080/v1/query', shell(query))
        payload = json.loads(shell(query)[shell(query).index('-d') + 1])
        self.assertEqual(payload, {'vector': [1, 1, .9], 'k': 2, 'include_metadata': True})

    def test_mcp_steps_preserve_install_fragment_and_collection(self):
        start, install, connect = self.commands[('agents', None)]
        self.assertIn('glider-data:/var/lib/glider', shell(start))
        self.assertNotIn('GLIDER_DIMENSIONS=3', shell(start))
        self.assertEqual(shell(install)[:2], ['pip', 'install'])
        self.assertTrue(shell(install)[2].endswith('#subdirectory=clients/python'))
        self.assertIn('GLIDER_COLLECTION=memory', shell(connect))
        self.assertEqual(shell(connect)[-2:], ['--', 'glider-mcp'])

    def test_python_snippets_keep_valid_syntax_and_their_own_client(self):
        start, install, write, query = self.commands[('quickstart', 'p-qp')]
        self.assertEqual(shell(start)[0], 'docker')
        self.assertEqual(shell(install)[:2], ['pip', 'install'])
        for code in (write, query):
            ast.parse(code)
            self.assertIn('client = Client(', code)


if __name__ == '__main__':
    unittest.main()
