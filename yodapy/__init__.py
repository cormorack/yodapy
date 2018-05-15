from __future__ import (absolute_import,
                        division,
                        print_function,
                        unicode_literals)

from yodapy.core import OOIASSET

__author__ = 'Landung Setiawan'

__all__ = ['core', 'utils']

from ._version import get_versions
__version__ = get_versions()['version']
del get_versions
