"""Ontology Loader package."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("ontology-loader")
except PackageNotFoundError:
    __version__ = "unknown"
