# -*- coding: utf-8 -*-

import os

# file structure
HOME_DIR = os.path.expanduser('~')
YODAPY_DIR = os.path.join(HOME_DIR, ".yodapy")
CREDENTIALS_FILE = os.path.join(YODAPY_DIR, ".credentials")
CONFIG_FILE = os.path.join(YODAPY_DIR, ".config")
TEST_DIR = os.path.join(HOME_DIR, ".test")
TEST_FILE = os.path.join(YODAPY_DIR, ".permission_test")

# this sets both the DEFAULTS and the TYPES for these files
FILE_CONTENT = {CREDENTIALS_FILE: {'ooi': {'username': '',
                                   'api_key': ''}},
                CONFIG_FILE: {}}


def _permissions():
    try:
        os.mkdir(TEST_DIR)
        os.rmdir(TEST_DIR)
        if not os.path.exists(YODAPY_DIR):
            os.mkdir(YODAPY_DIR)
        with open(TEST_FILE, 'w') as f:
            f.write('testing\n')
        os.remove(TEST_FILE)
        return True
    except Exception as e:
        print(e)
        return False


_file_permissions = _permissions()


def check_file_permissions():
    return _file_permissions
