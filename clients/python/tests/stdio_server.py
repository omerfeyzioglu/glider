"""Runs the MCP server over stdio with the fake embedder (used by test_mcp)."""

import os

import support
from glider_client import Client
from glider_client.mcp_server import Memory, build_server

build_server(Memory(Client(os.environ["GLIDER_URL"]), support.fake_embed)).run(transport="stdio")
