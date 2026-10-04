"""Python client for the Glider vector database HTTP API."""

from .client import Client, GliderError, Hit, Point

__all__ = ["Client", "GliderError", "Hit", "Point"]
__version__ = "1.0.0"
