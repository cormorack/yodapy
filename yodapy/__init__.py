from __future__ import (
    absolute_import,
    division,
    print_function,
    unicode_literals,
)

from yodapy import datasources

from ._version import get_versions


__author__ = 'Landung Setiawan'

__all__ = ['utils', 'datasources']

__version__ = get_versions()['version']
del get_versions
