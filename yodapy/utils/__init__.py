# -*- coding: utf-8 -*-

from __future__ import (division,
                        absolute_import,
                        print_function,
                        unicode_literals)

import os


def set_credentials_file(username=None, token=None):
    netrc_template = """machine ooinet.oceanobservatories.org
                        login {username}
                        password {token}""".format
    home_dir = os.environ.get('HOME')

    if username and token:
        fpath = os.path.join(home_dir, '.netrc')
        with open(fpath, 'w') as f:
            f.write(netrc_template(username=username,
                                   token=token))
        os.chmod(fpath, 0o700)
    else:
        raise EnvironmentError('Please enter your ooinet username and token!')
