# -*- coding: utf-8 -*-

from __future__ import (division,
                        absolute_import,
                        print_function,
                        unicode_literals)

import os

HOME_DIR = os.path.expanduser('~')
YODAPY_PATH = os.path.join(HOME_DIR, '.yodapy')


def create_folder(source_name):

    fold_name = source_name.lower()
    fold_path = os.path.join(YODAPY_PATH, fold_name)

    if not os.path.exists(YODAPY_PATH):
        os.mkdir(YODAPY_PATH)

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
