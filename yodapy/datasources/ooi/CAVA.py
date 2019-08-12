import logging
import os
import threading

import pandas as pd

from yodapy.datasources.datasource import DataSource


logging.basicConfig(level=logging.INFO,
                    format='(%(threadName)-10s) %(message)s',
                    )

logger = logging.getLogger(__name__)

META_PATH = os.path.join(os.path.dirname(__file__), 'infrastructure')


class CAVA(DataSource):

    def __init__(self):
        super().__init__()

        self._cava_arrays = None
        self._cava_sites = None
        self._cava_infrastructures = None
        self._cava_instruments = None
        self._cava_parameters = None
        self._source_name = 'Cabled Array Value Add Metadata'

        self._cava_setup()

    @property
    def cava_arrays(self):
        """ Cabled array team Arrays vocab table. """
        return self._cava_arrays

    @property
    def cava_sites(self):
        """ Cabled array team Sites vocab table. """
        return self._cava_sites

    @property
    def cava_infrastructures(self):
        """ Cabled array team Infrastructures vocab table. """
        return self._cava_infrastructures

    @property
    def cava_instruments(self):
        """ Cabled array team Instruments vocab table. """
        return self._cava_instruments

    @property
    def cava_parameters(self):
        """ Cabled array team Parameters vocab table. """
        return self._cava_parameters

    def _retrieve_meta(self):

        self._cava_arrays = self._get_df_from_metacsv(META_PATH, 'cava_arrays.csv')
        self._cava_sites = self._get_df_from_metacsv(META_PATH, 'cava_sites.csv')
        self._cava_infrastructures = self._get_df_from_metacsv(
            META_PATH, 'cava_infrastructures.csv')
        self._cava_instruments = self._get_df_from_metacsv(
            META_PATH, 'cava_instruments.csv')
        self._cava_parameters = self._get_df_from_metacsv(META_PATH, 'cava_parameters.csv')

    def _cava_setup(self):

        gthread = threading.Thread(name='retrieve-gsheet',
                                   target=self._retrieve_meta)
        gthread.setDaemon(True)

        gthread.start()

    def _get_df_from_metacsv(self, path, csv):
        return pd.read_csv(os.path.join(path, csv))
