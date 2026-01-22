from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("linepipe")
except PackageNotFoundError:
    __version__ = "None"
