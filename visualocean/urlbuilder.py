from __future__ import (absolute_import,
                        division,
                        print_function,
                        unicode_literals)

import os
from urllib.parse import urljoin

# Global Variables
ROOT_URL = 'https://ooinet.oceanobservatories.org/api/m2m/'
ASSET_META = urljoin(ROOT_URL, os.path.join('12587', 'events', 'deployment', 'inv'))
ASSET_DATA = urljoin(ROOT_URL, os.path.join('12576', 'sensor', 'inv'))


def create_data_url(*args):
    return os.path.join(ASSET_DATA, *args)


def create_meta_url(*args):
    return os.path.join(ASSET_META, *args)
