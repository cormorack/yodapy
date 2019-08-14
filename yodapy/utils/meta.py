# -*- coding: utf-8 -*-

from __future__ import (
    absolute_import,
    division,
    print_function,
    unicode_literals,
)

import os
import shutil
import warnings

from yodapy.utils.files import YODAPY_DIR, check_file_permissions


def create_folder(source_name):
    if check_file_permissions():
        fold_name = source_name.lower()
        fold_path = os.path.join(YODAPY_DIR, fold_name)

        if not os.path.exists(YODAPY_DIR):
            os.mkdir(YODAPY_DIR)

        if not os.path.exists(fold_path):
            os.mkdir(fold_path)

        return fold_path
    else:  # pragma: no cover
        warnings.warn(
            "You don't have a read-write permission "
            "to your home ('~') directory!"
        )


def delete_all_cache(source_name):
    if check_file_permissions():
        fold_name = source_name.lower()
        fold_path = os.path.join(YODAPY_DIR, fold_name)
        if os.path.exists(YODAPY_DIR):
            if len(os.listdir(fold_path)) > 0:
                for folder in os.listdir(fold_path):
                    cache_path = os.path.join(fold_path, folder)
                    file_count = len(os.listdir(cache_path))
                    print(f"deleting {file_count} files from {cache_path}")
                    try:
                        shutil.rmtree(cache_path)
                    except Exception as e:
                        warnings.warn(e)
    else:  # pragma: no cover
        warnings.warn(
            "You don't have a read-write permission "
            "to your home ('~') directory!"
        )
