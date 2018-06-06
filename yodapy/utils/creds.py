# -*- coding: utf-8 -*-

from __future__ import (division,
                        absolute_import,
                        print_function,
                        unicode_literals)

import os

from yodapy.utils.meta import HOME_DIR


def set_ooi_credentials_file(username=None, token=None):
    netrc_template = """machine ooinet.oceanobservatories.org
                        login {username}
                        password {token}""".format

    if username and token:
        fpath = os.path.join(HOME_DIR, '.netrc')
        with open(fpath, 'w') as f:
            f.write(netrc_template(username=username,
                                   token=token))
        os.chmod(fpath, 0o700)
    else:
        raise EnvironmentError('Please enter your ooinet username and token!')