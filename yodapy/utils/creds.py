# -*- coding: utf-8 -*-

from __future__ import (
    absolute_import,
    division,
    print_function,
    unicode_literals,
)

import json
import os
import warnings

from yodapy.utils.files import (
    CREDENTIALS_FILE,
    FILE_CONTENT,
    check_file_permissions,
)


def set_credentials_file(data_source, username, token):
    """
    Sets and saves the credential file for a data source.

    Args:
        data_source (str): Data source string. Currently only supports 'ooi'.
        username (str): Username value for the specified data_source.
        token (str): Token or password value for the specified data_source.
    """
    if data_source:
        data_source = data_source.lower()
        if data_source in ["ooi"]:
            if username and token:
                if check_file_permissions():
                    with open(CREDENTIALS_FILE, "w") as f:
                        FILE_CONTENT[CREDENTIALS_FILE][data_source] = {
                            "username": username,  # noqa
                            "api_key": token,
                        }  # noqa
                        f.write(json.dumps(FILE_CONTENT[CREDENTIALS_FILE]))
                else:  # pragma: no cover
                    warnings.warn(
                        "You don't have a read-write permission "
                        "to your home ('~') directory!"
                    )
            else:  # pragma: no cover
                warnings.warn("Please enter your username and token!")
        else:  # pragma: no cover
            warnings.warn(
                f"Datasource: {data_source} is not valid. Available: ooi"
            )
    else:  # pragma: no cover
        warnings.warn("Please specify a data_source. Available: ooi")
