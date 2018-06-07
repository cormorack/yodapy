# -*- coding: utf-8 -*-

from __future__ import (division,
                        absolute_import,
                        print_function,
                        unicode_literals)

import os

HOME_DIR = os.environ.get('HOME')


def create_folder(source_name):
    yodapy_pth = os.path.join(HOME_DIR, '.yodapy')

    fold_name = source_name.lower()
    fold_path = os.path.join(yodapy_pth, fold_name)

    if not os.path.exists(yodapy_pth):
        os.mkdir(yodapy_pth)

    if not os.path.exists(fold_path):
        os.mkdir(fold_path)

    return fold_path


def meta_cache(source_name):
    def meta_cache_decorator(func):
        def wrapper():
            fold_path = create_folder(source_name)
            func_res = func(fold_path)
            return func_res
        return wrapper
    return meta_cache_decorator
