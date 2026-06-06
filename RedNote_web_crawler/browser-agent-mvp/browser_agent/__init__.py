"""Browser Agent MVP.

This package implements the deterministic runtime core:
Recipe + Network Extractor + Feedback Memory.
"""

from .recipe import Recipe
from .extractor import NetworkExtractor
from .feedback import FeedbackMemory

__all__ = ["Recipe", "NetworkExtractor", "FeedbackMemory"]
