# -*- coding: utf-8 -*-

from __future__ import (division,
                        absolute_import,
                        print_function,
                        unicode_literals)

import os
import warnings
import json

from yodapy.utils.files import (CREDENTIALS_FILE,
                                FILE_CONTENT,
                                check_file_permissions)


def set_credentials_file(data_source=None, username=None, token=None):
    if data_source:
        data_source = data_source.lower()
        if data_source in ['ooi']:
            if username and token:
                if check_file_permissions():
                    with open(CREDENTIALS_FILE, 'w') as f:
                        FILE_CONTENT[CREDENTIALS_FILE][data_source] = {'username': username,  # noqa
                                                                       'api_key': token}  # noqa
                        f.write(json.dumps(FILE_CONTENT[CREDENTIALS_FILE]))
                else:
                    warnings.warn('You don\'t have a read-write permission '
                                  'to your home (\'~\') directory!')
            else:
                warnings.warn('Please enter your username and token!')
        else:
            warnings.warn(f'Datasource: {data_source} is not valid. Available: ooi')
    else:
        warnings.warn('Please specify a data_source. Available: ooi')
