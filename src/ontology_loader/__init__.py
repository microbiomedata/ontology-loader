from importlib.metadata import version, PackageNotFoundError

try:
    __version__ = version("ontology-loader")
except PackageNotFoundError:
    __version__ = "unknown"