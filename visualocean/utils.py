from __future__ import (absolute_import,
                        division,
                        print_function,
                        unicode_literals)

import datetime

def datetime_to_string(dt):
    return dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')