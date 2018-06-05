from __future__ import (absolute_import,
                        division,
                        print_function,
                        unicode_literals)

from yodapy import datasources

__author__ = 'Landung Setiawan'

__all__ = ['utils', 'datasources']

from ._version import get_versions
__version__ = get_versions()['version']
del get_versions
