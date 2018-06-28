# -*- coding: utf-8 -*-

from __future__ import (division,
                        absolute_import,
                        print_function,
                        unicode_literals)

import os
import warnings

from yodapy.utils.files import (YODAPY_DIR, check_file_permissions)


def create_folder(source_name):
    if check_file_permissions():
        fold_name = source_name.lower()
        fold_path = os.path.join(YODAPY_DIR, fold_name)

        if not os.path.exists(YODAPY_DIR):
            os.mkdir(YODAPY_DIR)

        if not os.path.exists(fold_path):
            os.mkdir(fold_path)

        return fold_path
    else:
        warnings.warn('You don\'t have a read-write permission '
                      'to your home (\'~\') directory!')
